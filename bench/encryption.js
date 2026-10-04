'use strict';

// What sealing a message costs, on the shapes @alexify/wrpc/encryption uses
// (src/encryption/aead.js and aead.browser.js), and the three decisions the
// numbers make:
//
//   1. node:crypto synchronously against crypto.subtle on the SAME machine —
//      why the Node half is a platform pair and not the one subtle file the
//      rest of the directory is. subtle pays a threadpool hand-off per call,
//      the argument bench/zlib-async.js makes for zlib.
//   2. A key prepared once against the raw key handed to every call — why
//      the contract is `cipher.key(raw)` and not `seal(rawKey, …)`. On Node
//      it is a wash (a KeyObject saves nothing measurable); over
//      crypto.subtle an importKey per message costs half as much again,
//      and the import is what makes the key non-extractable in a page.
//   3. The fan-out: under session encryption every recipient has its own
//      key, so a broadcast is N seals where plain wrpc serializes ONCE
//      (sendPrepared). The last rows are that price, per emit.
//
// Sizes are a small packet, a typical one and a large result.

const crypto = require('node:crypto');
const { performance } = require('node:perf_hooks');
const node = require('../src/encryption/aead.js');
const browser = require('../src/encryption/aead.browser.js');
const { counterNonce } = require('../src/encryption/bytes.js');

const SIZES = [64, 1024, 16 * 1024];
const RAW = crypto.randomBytes(32);
const AAD = Buffer.from('wrpc-sealed v1\0rooms\0k1\0room:lobby');

const payload = (size) => {
  const row = JSON.stringify({ type: 'event', name: 'chat/message', data: { from: 'u1', text: 'x'.repeat(24) } });
  return Buffer.from(row.repeat(Math.ceil(size / row.length)).slice(0, size));
};

const MEASURE_MS = 400;

const micros = (fn) => {
  for (let i = 0; i < 2000; i++) fn(i);
  let iterations = 0;
  const started = performance.now();
  while (performance.now() - started < MEASURE_MS) {
    for (let i = 0; i < 100; i++) fn(iterations + i);
    iterations += 100;
  }
  return ((performance.now() - started) * 1000) / iterations;
};

const microsAsync = async (fn) => {
  for (let i = 0; i < 500; i++) await fn(i);
  let iterations = 0;
  const started = performance.now();
  while (performance.now() - started < MEASURE_MS) {
    await fn(iterations);
    iterations++;
  }
  return ((performance.now() - started) * 1000) / iterations;
};

const cell = (value) => `${value.toFixed(2)} µs`.padStart(12);

async function main() {
  const gcm = node.aead().key(RAW);
  const chacha = node.aead({ algorithm: 'chacha20-poly1305' }).key(RAW);
  const subtle = await browser.aead().key(RAW);
  const nonce = new Uint8Array(12);

  console.log('seal, per message'.padEnd(34) + SIZES.map((size) => `${size} B`.padStart(12)).join(''));
  const rows = [
    ['JSON copy (no encryption)', (body) => () => Buffer.from(body)],
    ['aes-256-gcm  node:crypto sync', (body) => (i) => gcm.seal(counterNonce(i, nonce), body, AAD)],
    ['chacha20-poly1305  node:crypto sync', (body) => (i) => chacha.seal(counterNonce(i, nonce), body, AAD)],
    [
      'aes-256-gcm  raw key per call',
      (body) => (i) => {
        const cipher = crypto.createCipheriv('aes-256-gcm', RAW, counterNonce(i, nonce));
        cipher.setAAD(AAD);
        return Buffer.concat([cipher.update(body), cipher.final(), cipher.getAuthTag()]);
      },
    ],
  ];
  for (const [label, make] of rows) {
    console.log(label.padEnd(34) + SIZES.map((size) => cell(micros(make(payload(size))))).join(''));
  }
  const awaited = [];
  for (const size of SIZES) {
    const body = payload(size);
    awaited.push(await microsAsync((i) => subtle.seal(counterNonce(i, new Uint8Array(12)), body, AAD)));
  }
  console.log('aes-256-gcm  crypto.subtle (await)'.padEnd(34) + awaited.map(cell).join(''));
  const imported = [];
  for (const size of SIZES) {
    const body = payload(size);
    imported.push(
      await microsAsync(async (i) =>
        (await browser.aead().key(RAW)).seal(counterNonce(i, new Uint8Array(12)), body, AAD),
      ),
    );
  }
  console.log('aes-256-gcm  subtle + importKey'.padEnd(34) + imported.map(cell).join(''));

  console.log('\nopen, per message'.padEnd(35) + SIZES.map((size) => `${size} B`.padStart(12)).join(''));
  const opens = [];
  const opensCopied = [];
  const opensAsync = [];
  const key = crypto.createSecretKey(RAW);
  for (const size of SIZES) {
    const sealed = gcm.seal(nonce.fill(0), payload(size), AAD);
    const split = sealed.length - 16;
    opens.push(micros(() => gcm.open(nonce, sealed, AAD)));
    // The idiomatic spelling aead.js does not use on this path: the body
    // copied once more through Buffer.concat.
    opensCopied.push(
      micros(() => {
        const decipher = crypto.createDecipheriv('aes-256-gcm', key, nonce, { authTagLength: 16 });
        decipher.setAuthTag(sealed.subarray(split));
        decipher.setAAD(AAD);
        return Buffer.concat([decipher.update(sealed.subarray(0, split)), decipher.final()]);
      }),
    );
    opensAsync.push(await microsAsync(() => subtle.open(nonce, sealed, AAD)));
  }
  console.log('aes-256-gcm  node:crypto sync'.padEnd(34) + opens.map(cell).join(''));
  console.log('aes-256-gcm  … via Buffer.concat'.padEnd(34) + opensCopied.map(cell).join(''));
  console.log('aes-256-gcm  crypto.subtle (await)'.padEnd(34) + opensAsync.map(cell).join(''));

  // A subkey per sender salt is derived once and cached; this is the miss.
  const salt = crypto.randomBytes(16);
  const derive = micros(() => crypto.hkdfSync('sha256', RAW, salt, AAD, 32));
  const keyed = micros(() => node.aead().key(RAW));
  console.log(`\nhkdfSync subkey (a cache miss)     ${cell(derive)}`);
  console.log(`cipher.key(raw) (a KeyObject)      ${cell(keyed)}`);

  // The whole backplane path (src/rpc/envelope.js): frame, seal, base64 —
  // and back, through the sender cache. What a room emit pays per publish,
  // and every other instance per receive. The receiving side runs with
  // `replayWindow: false` — the loop decodes ONE message again and again,
  // which a window would refuse — so the decode row is without the window's
  // check (one array read and one write).
  const { createEnvelope } = require('../src/rpc/envelope.js');
  const quiet = { warn() {} };
  const envelopeOptions = { maxMessage: 1 << 24, name: 'bench', layer: 'rooms', event: 'backplane', log: quiet };
  const sending = createEnvelope({ ...envelopeOptions, encryption: { keys: RAW } });
  const receiving = createEnvelope({ ...envelopeOptions, encryption: { keys: RAW, replayWindow: false } });
  console.log('\nbackplane envelope, per message'.padEnd(35) + SIZES.map((size) => `${size} B`.padStart(12)).join(''));
  const encodes = [];
  const decodes = [];
  for (const size of SIZES) {
    const text = payload(size).toString();
    encodes.push(micros(() => sending.encode(text, 'room:lobby')));
    const wire = sending.encode(text, 'room:lobby');
    decodes.push(micros(() => receiving.decode(wire, 'room:lobby')));
  }
  console.log('seal + base64 (encode)'.padEnd(34) + encodes.map(cell).join(''));
  console.log('base64 + open (decode)'.padEnd(34) + decodes.map(cell).join(''));

  // A session frame (src/encryption/session.js) as the server sends one:
  // the inner kind byte, the seal, the 00 06 header — against the bare
  // AEAD above, which is the floor.
  const { SecureChannel } = require('../src/encryption/session.js');
  const { createNoise } = require('../src/encryption/noise.js');
  const { x25519 } = require('../src/encryption/dh.js');
  const { createKdf } = require('../src/encryption/hkdf.js');
  const noise = createNoise({ pattern: 'NN', dh: x25519(), cipher: node.aead(), kdf: createKdf() });
  // A fresh pair per row: a frame opens once, in order — the counter is the nonce.
  const channels = async () => {
    const [initiator, responder] = [await noise.initiator(), await noise.responder()];
    await responder.read(await initiator.write());
    await initiator.read(await responder.write());
    return [new SecureChannel(await initiator.finish()), new SecureChannel(await responder.finish())];
  };
  console.log('\nsession frame, per message'.padEnd(35) + SIZES.map((size) => `${size} B`.padStart(12)).join(''));
  const sessionSeals = [];
  const sessionOpens = [];
  for (const size of SIZES) {
    const text = payload(size).toString();
    const [sealing] = await channels();
    sessionSeals.push(micros(() => sealing.seal(text)));
    const [server, page] = await channels();
    // Each frame opens once (the counter moves), so a batch is sealed ahead.
    const batch = 20000;
    let frames = [];
    let next = 0;
    sessionOpens.push(
      micros(() => {
        if (next === frames.length) {
          frames = Array.from({ length: batch }, () => server.seal(text));
          next = 0;
        }
        return page.open(frames[next++]);
      }),
    );
  }
  // The inner frame of a text packet, the two ways session.js can build it.
  const textEncoder = new TextEncoder();
  const inners = { encodeInto: [], buffer: [] };
  for (const size of SIZES) {
    const text = payload(size).toString();
    inners.encodeInto.push(
      micros(() => {
        const scratch = new Uint8Array(1 + text.length * 3);
        return scratch.subarray(0, 1 + textEncoder.encodeInto(text, scratch.subarray(1)).written);
      }),
    );
    inners.buffer.push(
      micros(() => {
        const inner = Buffer.allocUnsafe(1 + Buffer.byteLength(text));
        inner.write(text, 1);
        return inner;
      }),
    );
  }
  console.log('inner frame  TextEncoder.encodeInto'.padEnd(34) + inners.encodeInto.map(cell).join(''));
  console.log('inner frame  Buffer.write'.padEnd(34) + inners.buffer.map(cell).join(''));
  console.log('seal a text packet'.padEnd(34) + sessionSeals.map(cell).join(''));
  console.log('open a text packet (incl. sealing)'.padEnd(34) + sessionOpens.map(cell).join(''));
  // A binary frame the size of a stream chunk: the inner frame is a copy of
  // the bytes, then the seal — the row a file upload under a session pays.
  {
    const chunk = crypto.randomBytes(64 * 1024);
    const [sealing] = await channels();
    console.log(`seal a 64 KB binary chunk           ${cell(micros(() => sealing.seal(chunk)))}`);
  }
  const started = performance.now();
  for (let i = 0; i < 200; i++) {
    const [a, b] = [await noise.initiator(), await noise.responder()];
    await b.read(await a.write());
    await a.read(await b.write());
    await Promise.all([a.finish(), b.finish()]);
  }
  console.log(`a whole NN handshake, both ends     ${cell(((performance.now() - started) * 1000) / 200)}`);
  // NK is the default pattern a client runs (it pins the server's key):
  // one more DH than NN on each side.
  {
    const nk = createNoise({ pattern: 'NK', dh: x25519(), cipher: node.aead(), kdf: createKdf() });
    const staticKey = await x25519().generateKeyPair();
    const nkStarted = performance.now();
    for (let i = 0; i < 200; i++) {
      const a = await nk.initiator({ remoteStatic: staticKey.publicKey });
      const b = await nk.responder({ staticKey });
      await b.read(await a.write());
      await a.read(await b.write());
      await Promise.all([a.finish(), b.finish()]);
    }
    console.log(`a whole NK handshake, both ends     ${cell(((performance.now() - nkStarted) * 1000) / 200)}`);
  }

  // One sealed HTTP request, both ends (src/encryption/http.js over hpke.js):
  // an X25519 each way, the key schedule, the request and its answer.
  const { createHpke, dhKem } = require('../src/encryption/hpke.js');
  const kdfForHpke = createKdf();
  const kem = dhKem(x25519(), kdfForHpke);
  const hpke = createHpke({ kem, kdf: kdfForHpke, cipher: node.aead() });
  const recipient = await kem.generateKeyPair();
  const requestBody = payload(1024);
  const requests = 300;
  const hpkeStarted = performance.now();
  for (let i = 0; i < requests; i++) {
    const sent = await hpke.setupSender(recipient.publicKey, { info: AAD });
    const sealedRequest = await sent.context.seal(null, requestBody);
    const received = await hpke.setupRecipient(sent.enc, recipient, { info: AAD });
    await received.open(null, sealedRequest);
    await received.export(AAD, 32);
    await sent.context.export(AAD, 32);
  }
  console.log(`\na sealed HTTP request, both ends    ${cell(((performance.now() - hpkeStarted) * 1000) / requests)}`);

  console.log(
    '\nfan-out of one 1 KB event, per emit'.padEnd(35) + [10, 1000, 10000].map((n) => `${n}`.padStart(12)).join(''),
  );
  const body = payload(1024);
  const recipients = Array.from({ length: 10000 }, () => node.aead().key(crypto.randomBytes(32)));
  const shared = [];
  const sealedEach = [];
  for (const count of [10, 1000, 10000]) {
    // Plain wrpc: the frame is built once and the same bytes go to everyone.
    shared.push(
      micros(() => {
        const frame = Buffer.from(body);
        let sent = 0;
        for (let r = 0; r < count; r++) sent += frame.length;
        return sent;
      }),
    );
    sealedEach.push(
      micros((i) => {
        counterNonce(i, nonce);
        let sent = 0;
        for (let r = 0; r < count; r++) sent += recipients[r].seal(nonce, body, null).length;
        return sent;
      }),
    );
  }
  console.log('one shared frame (today)'.padEnd(34) + shared.map(cell).join(''));
  console.log('one seal per recipient (bare AEAD)'.padEnd(34) + sealedEach.map(cell).join(''));
  // What a sealed fan-out ACTUALLY costs: SecureChannel.seal(text) per
  // recipient — the inner frame built from the text, the counter nonce,
  // the seal, the header — not the bare AEAD above. 2000 established
  // channels, reused round-robin for the larger counts (a seal's cost does
  // not depend on which channel it is).
  const text = payload(1024).toString();
  const pool = [];
  for (let i = 0; i < 2000; i++) pool.push((await channels())[0]);
  const perRecipient = [];
  for (const count of [10, 1000, 10000]) {
    perRecipient.push(
      micros(() => {
        let sent = 0;
        for (let r = 0; r < count; r++) sent += pool[r % pool.length].seal(text).length;
        return sent;
      }),
    );
  }
  console.log('one SecureChannel.seal(text) each'.padEnd(34) + perRecipient.map(cell).join(''));
  // What SealedSocket.sendPrepared does instead: the plaintext inner frame —
  // the kind byte and the text's UTF-8 — built ONCE per emit and sealed per
  // recipient. Still N seals; what goes is N byteLength + alloc + write of
  // the same string, which grows with the message where the seal's fixed
  // cost does not. Per recipient, over emits to 100 of them.
  const { innerOf } = require('../src/encryption/session.js');
  const perSeal = (message, shared) => {
    const emit = shared
      ? () => {
          const inner = innerOf(message);
          let sent = 0;
          for (let r = 0; r < 100; r++) sent += pool[r].sealInner(inner).length;
          return sent;
        }
      : () => {
          let sent = 0;
          for (let r = 0; r < 100; r++) sent += pool[r].seal(message).length;
          return sent;
        };
    return micros(emit) / 100;
  };
  console.log('\nsealed fan-out, per recipient'.padEnd(35) + SIZES.map((size) => `${size} B`.padStart(12)).join(''));
  const messages = SIZES.map((size) => payload(size).toString());
  console.log('seal(text): the inner frame each'.padEnd(34) + messages.map((m) => cell(perSeal(m, false))).join(''));
  console.log('sealInner(shared inner frame)'.padEnd(34) + messages.map((m) => cell(perSeal(m, true))).join(''));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
