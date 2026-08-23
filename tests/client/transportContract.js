'use strict';

// The shared client-transport contract, mirroring tests/engine/engineContract:
// one structural spec, run against every registered transport — built-in and
// subpath alike. Structural rather than behavioral on purpose: each
// transport's WIRE behavior has its own suite; what this locks is the shape
// `connect()` constructs against, so a third-party registrant that would die
// at runtime dies here instead.

const assert = require('node:assert');

const { isClientTransport } = require('../../src/client.js');

const runTransportContract = (t, name, Transport) => {
  t.test(`${name}: satisfies the ClientTransport contract`, () => {
    assert.strictEqual(isClientTransport(Transport), true);
    const proto = Transport.prototype;
    for (const method of ['open', 'close', 'write', 'terminate', 'send', 'on', 'off', 'online', 'offline']) {
      assert.strictEqual(typeof proto[method], 'function', `${name}#${method} must be a function`);
    }
  });

  t.test(`${name}: instance flags and url`, () => {
    const instance = new Transport(`x://host/${name}`);
    assert.strictEqual(instance.url, `x://host/${name}`);
    assert.strictEqual(typeof instance.active, 'boolean');
    assert.strictEqual(typeof instance.persistent, 'boolean');
    assert.strictEqual(typeof instance.heartbeat, 'boolean');
    assert.strictEqual(instance.active, false, 'a fresh transport starts inactive');
  });
};

module.exports = { runTransportContract };
