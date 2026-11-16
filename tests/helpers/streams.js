'use strict';

// What the stream suites share: a raw protocol peer — a sender that does not
// keep its word is not something a WrpcClient can be made to be — and the
// few lines every one of them wrote around a readable. Not a *.test.js:
// node --test must not run helpers.

const timers = require('node:timers/promises');

const { chunkEncode } = require('../../src/chunks.js');
const { waitFor } = require('./wait.js');

const KIB = 1024;
const MIB = 1024 * KIB;
const bytes = (length, fill = 1) => new Uint8Array(length).fill(fill);
const tick = () => new Promise((resolve) => setImmediate(resolve));

// What a promise did within `ms`. A hang is an answer here — 'pending' —
// instead of a test that times out with nothing to read.
const outcome = async (promise, ms = 500) => {
  const controller = new AbortController();
  const result = await Promise.race([
    Promise.resolve(promise).then(
      (value) => ({ status: 'fulfilled', value }),
      (error) => ({ status: 'rejected', error }),
    ),
    timers.setTimeout(ms, { status: 'pending' }, { signal: controller.signal }).catch(() => null),
  ]);
  controller.abort();
  return result;
};

const drain = async (readable) => {
  let size = 0;
  for await (const chunk of readable) size += chunk.length;
  return size;
};

/**
 * A raw protocol peer on a WebSocket: what a sender that does not keep its
 * word looks like. `ping` is a procedure of the server's router that answers
 * anything — `settle()` calls it for a round trip.
 */
const rawPeer = async (t, url, { ping = 'files/ping' } = {}) => {
  const socket = new WebSocket(url, ['wrpc.v2']);
  socket.binaryType = 'arraybuffer';
  t.after(() => socket.close());
  const answers = [];
  socket.addEventListener('message', ({ data }) => {
    if (typeof data === 'string') answers.push(JSON.parse(data));
  });
  await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));
  const send = (packet) => socket.send(JSON.stringify(packet));
  return {
    socket,
    answers,
    send,
    announce: (id, size, name = 'blob') => send({ type: 'stream', id, name, size }),
    chunk: (id, payload) => socket.send(chunkEncode(id, payload)),
    end: (id, status = 'end') => send({ type: 'stream', id, status }),
    call: (id, method, args = {}) => send({ type: 'call', id, method, args }),
    answer: (id) => answers.find((packet) => packet.id === id),
    // A round trip: everything sent before it was dispatched by the server.
    async settle() {
      const id = `ping-${answers.length}-${Math.random()}`;
      send({ type: 'call', id, method: ping, args: {} });
      await waitFor(() => answers.some((packet) => packet.id === id), 'the round trip');
    },
  };
};

module.exports = { KIB, MIB, bytes, tick, outcome, drain, rawPeer };
