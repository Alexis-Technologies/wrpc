'use strict';

// The shared client-transport contract, mirroring tests/engine/engineContract:
// one structural spec, run against every registered transport — built-in and
// subpath alike. Structural rather than behavioral on purpose: each
// transport's WIRE behavior has its own suite; what this locks is the shape
// `connect()` constructs against, so a third-party registrant that would die
// at runtime dies here instead.

const assert = require('node:assert');

const { isClientTransport } = require('../../src/client.js');

// Awaited, and awaited by its caller: a subtest a synchronous parent never
// waits for is CANCELLED rather than run on Node 22 (Node 24's runner waits),
// which is how this suite reported `cancelledByParent` for every case there.
const runTransportContract = async (t, name, Transport) => {
  await t.test(`${name}: satisfies the ClientTransport contract`, () => {
    assert.strictEqual(isClientTransport(Transport), true);
    const proto = Transport.prototype;
    for (const method of ['open', 'close', 'write', 'terminate', 'send', 'on', 'off', 'online', 'offline']) {
      assert.strictEqual(typeof proto[method], 'function', `${name}#${method} must be a function`);
    }
  });

  await t.test(`${name}: instance flags and url`, () => {
    const instance = new Transport(`x://host/${name}`);
    assert.strictEqual(instance.url, `x://host/${name}`);
    assert.strictEqual(typeof instance.active, 'boolean');
    assert.strictEqual(typeof instance.persistent, 'boolean');
    assert.strictEqual(typeof instance.heartbeat, 'boolean');
    assert.strictEqual(instance.active, false, 'a fresh transport starts inactive');
  });

  // What `options.encryption` is checked against before anything opens:
  // true — a session transport, which sets `encryption` before it says
  // 'open'; 'request' — sealed per request, needs the option's `fetch`;
  // 'keys' — a keyring over a shared carrier; absent or false — refused.
  await t.test(`${name}: declares how it carries encryption, or that it does not`, () => {
    assert.ok([undefined, false, true, 'request', 'keys'].includes(Transport.encrypts), String(Transport.encrypts));
    if (Transport.encrypts !== true) return;
    const instance = new Transport(`x://host/${name}`);
    assert.strictEqual(instance.encryption, null, 'a session transport exposes `encryption`, null until established');
  });
};

module.exports = { runTransportContract };
