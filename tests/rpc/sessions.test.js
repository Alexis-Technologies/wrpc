'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { setImmediate: settle, setTimeout: delay } = require('node:timers/promises');

const { Session, MemorySessionStore, SessionManager } = require('../../src/rpc/sessions.js');
const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure } = require('../../src/rpc/router.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const capturingStore = () => {
  const sets = [];
  return {
    sets,
    async get() {
      return null;
    },
    async set(token, data) {
      sets.push({ token, data: { ...data } });
    },
    async delete() {},
  };
};

test('MemorySessionStore', async (t) => {
  await t.test('get miss returns null', async () => {
    const store = new MemorySessionStore();
    assert.strictEqual(await store.get('missing'), null);
  });

  await t.test('set then get returns the stored data', async () => {
    const store = new MemorySessionStore();
    const data = { user: 'alex' };
    await store.set('tok', data);
    assert.strictEqual(await store.get('tok'), data);
  });

  await t.test('delete removes the entry', async () => {
    const store = new MemorySessionStore();
    await store.set('tok', { user: 'alex' });
    await store.delete('tok');
    assert.strictEqual(await store.get('tok'), null);
  });
});

test('SessionManager create', async (t) => {
  await t.test('generates a token when omitted', () => {
    const manager = new SessionManager({}, quiet);
    const session = manager.create();
    assert.ok(session instanceof Session);
    assert.match(session.token, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  });

  await t.test('uses the provided token', () => {
    const manager = new SessionManager({}, quiet);
    assert.strictEqual(manager.create('explicit').token, 'explicit');
  });

  await t.test('persists the initial data to the store', async () => {
    const store = capturingStore();
    const manager = new SessionManager({ store }, quiet);
    manager.create('tok', { user: 'alex' });
    await settle();
    assert.deepStrictEqual(store.sets, [{ token: 'tok', data: { user: 'alex' } }]);
  });

  await t.test('state proxy auto-saves on property set', async () => {
    const store = capturingStore();
    const manager = new SessionManager({ store }, quiet);
    const session = manager.create('tok', { count: 0 });
    session.state.count = 5;
    session.state.name = 'alex';
    await settle();
    assert.strictEqual(store.sets.length, 3);
    assert.deepStrictEqual(store.sets[1], { token: 'tok', data: { count: 5 } });
    assert.deepStrictEqual(store.sets[2], { token: 'tok', data: { count: 5, name: 'alex' } });
    assert.strictEqual(session.state.count, 5);
    assert.strictEqual(session.state.name, 'alex');
  });

  await t.test('custom generateToken is used for omitted tokens', () => {
    let n = 0;
    const manager = new SessionManager({ generateToken: () => `tok-${++n}` }, quiet);
    assert.strictEqual(manager.create().token, 'tok-1');
    assert.strictEqual(manager.create().token, 'tok-2');
    assert.strictEqual(manager.create('explicit').token, 'explicit');
  });
});

test('SessionManager restore and destroy', async (t) => {
  await t.test('restore returns a session with the stored state', async () => {
    const manager = new SessionManager({}, quiet);
    manager.create('tok', { user: 'alex' });
    await settle();
    const restored = await manager.restore('tok');
    assert.ok(restored instanceof Session);
    assert.strictEqual(restored.token, 'tok');
    assert.strictEqual(restored.state.user, 'alex');
  });

  await t.test('restored session auto-saves too', async () => {
    const store = new MemorySessionStore();
    const manager = new SessionManager({ store }, quiet);
    manager.create('tok', { count: 0 });
    await settle();
    const restored = await manager.restore('tok');
    restored.state.count = 7;
    await settle();
    const data = await store.get('tok');
    assert.strictEqual(data.count, 7);
  });

  await t.test('restore miss returns null', async () => {
    const manager = new SessionManager({}, quiet);
    assert.strictEqual(await manager.restore('missing'), null);
  });

  await t.test('destroy deletes the session from the store', async () => {
    const manager = new SessionManager({}, quiet);
    manager.create('tok', { user: 'alex' });
    await settle();
    await manager.destroy('tok');
    assert.strictEqual(await manager.restore('tok'), null);
  });
});

test('SessionManager cookies', async (t) => {
  await t.test('cookieHeader has the default attributes', () => {
    const manager = new SessionManager({}, quiet);
    const header = manager.cookieHeader('abc123');
    assert.ok(header.startsWith('token=abc123'));
    assert.ok(header.includes('Path=/'));
    assert.ok(header.includes('HttpOnly'));
    assert.ok(header.includes('Secure'));
    assert.ok(header.includes('SameSite=Lax'));
    assert.ok(!header.includes('Max-Age'));
  });

  await t.test('cookieDeleteHeader expires the cookie', () => {
    const manager = new SessionManager({}, quiet);
    const header = manager.cookieDeleteHeader();
    assert.ok(header.startsWith('token=deleted'));
    assert.ok(header.includes('Max-Age=0'));
    assert.ok(header.includes('Path=/'));
    assert.ok(header.includes('HttpOnly'));
  });

  await t.test('maxAge option adds Max-Age', () => {
    const manager = new SessionManager({ cookie: { maxAge: 3600 } }, quiet);
    assert.ok(manager.cookieHeader('t').includes('Max-Age=3600'));
  });

  await t.test('custom cookie name is used everywhere', () => {
    const manager = new SessionManager({ cookie: { name: 'sid' } }, quiet);
    assert.ok(manager.cookieHeader('t').startsWith('sid=t'));
    assert.ok(manager.cookieDeleteHeader().startsWith('sid=deleted'));
    assert.strictEqual(manager.readToken({ sid: 'x' }), 'x');
    assert.strictEqual(manager.readToken({ token: 'x' }), null);
  });

  await t.test('readToken reads the token cookie', () => {
    const manager = new SessionManager({}, quiet);
    assert.strictEqual(manager.readToken({ token: 'abc' }), 'abc');
    assert.strictEqual(manager.readToken({}), null);
  });
});

test('SessionManager with a custom async store', async () => {
  const map = new Map();
  const store = {
    async get(token) {
      await delay(1);
      return map.get(token) ?? null;
    },
    async set(token, data) {
      await delay(1);
      map.set(token, { ...data });
    },
    async delete(token) {
      await delay(1);
      map.delete(token);
    },
  };
  const manager = new SessionManager({ store }, quiet);
  const session = manager.create('slow', { hits: 0 });
  await delay(5);
  assert.deepStrictEqual(map.get('slow'), { hits: 0 });
  session.state.hits = 1;
  await delay(5);
  assert.deepStrictEqual(map.get('slow'), { hits: 1 });
  const restored = await manager.restore('slow');
  assert.strictEqual(restored.state.hits, 1);
  restored.state.hits = 2;
  await delay(5);
  assert.deepStrictEqual(map.get('slow'), { hits: 2 });
  await manager.destroy('slow');
  assert.strictEqual(await manager.restore('slow'), null);
});

test('RpcServer session isolation', async (t) => {
  const makeRouter = () =>
    defineRouter({
      unit: { hello: procedure({ access: 'public', handler: async () => 'hi' }) },
    });

  await t.test('default stores are per-server', async () => {
    const serverA = new RpcServer({ router: makeRouter(), logger: false });
    const serverB = new RpcServer({ router: makeRouter(), logger: false });
    serverA.sessions.create('tokA', { user: 'alex' });
    await settle();
    assert.ok(await serverA.sessions.restore('tokA'));
    assert.strictEqual(await serverB.sessions.restore('tokA'), null);
    await serverA.close();
    await serverB.close();
  });

  await t.test('a shared store is shared', async () => {
    const store = new MemorySessionStore();
    const serverA = new RpcServer({ router: makeRouter(), sessions: { store }, logger: false });
    const serverB = new RpcServer({ router: makeRouter(), sessions: { store }, logger: false });
    serverA.sessions.create('tokA', { user: 'alex' });
    await settle();
    const restored = await serverB.sessions.restore('tokA');
    assert.ok(restored instanceof Session);
    assert.strictEqual(restored.state.user, 'alex');
    await serverA.close();
    await serverB.close();
  });
});

test('initializeSession with the same token does not race delete against set', async () => {
  const { Client } = require('../../src/rpc/core.js');
  const deletes = [];
  const map = new Map();
  const store = {
    async get(token) {
      return map.get(token) ?? null;
    },
    async set(token, data) {
      map.set(token, data);
    },
    async delete(token) {
      deletes.push(token);
      map.delete(token);
    },
  };
  const noop = () => {};
  const quiet = { log: noop, info: noop, warn: noop, error: noop };
  const sessions = new SessionManager({ store }, quiet);
  const transport = { source: 'x', connection: {}, send: noop, error: noop, close: noop, once: noop };
  const client = new Client(transport, { sessions, logger: false });

  client.initializeSession('same-token', { round: 1 });
  client.initializeSession('same-token', { round: 2 }); // refresh, no finalize
  await new Promise((resolve) => setImmediate(resolve));

  assert.deepStrictEqual(deletes, []); // the old delete would race the new set
  assert.deepStrictEqual(map.get('same-token'), { round: 2 });

  client.initializeSession('other-token', { round: 3 }); // different token DOES finalize
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepStrictEqual(deletes, ['same-token']);
});

test('MemorySessionStore bounds', async (t) => {
  await t.test('evicts the least recently used entry past maxSessions', async () => {
    const store = new MemorySessionStore({ maxSessions: 2 });
    await store.set('a', { n: 1 });
    await store.set('b', { n: 2 });
    await store.get('a'); // 'a' becomes the most recently used
    await store.set('c', { n: 3 });
    assert.strictEqual(store.size, 2);
    assert.strictEqual(await store.get('b'), null);
    assert.deepStrictEqual(await store.get('a'), { n: 1 });
    assert.deepStrictEqual(await store.get('c'), { n: 3 });
  });

  await t.test('expires entries past the ttl', async () => {
    let clock = 1000;
    const store = new MemorySessionStore({ ttl: 100, now: () => clock });
    await store.set('a', { n: 1 });
    clock += 50;
    assert.deepStrictEqual(await store.get('a'), { n: 1 });
    clock += 100;
    assert.strictEqual(await store.get('a'), null);
    assert.strictEqual(store.size, 0);
  });

  await t.test('a write sweeps already-expired entries', async () => {
    let clock = 0;
    const store = new MemorySessionStore({ ttl: 10, now: () => clock });
    await store.set('old', { n: 1 });
    clock += 50;
    await store.set('fresh', { n: 2 });
    assert.strictEqual(store.size, 1);
    assert.deepStrictEqual(await store.get('fresh'), { n: 2 });
  });

  await t.test('ttl 0 and maxSessions 0 disable the bounds', async () => {
    const store = new MemorySessionStore({ ttl: 0, maxSessions: 0 });
    for (let i = 0; i < 50; i++) await store.set(`t${i}`, { i });
    assert.strictEqual(store.size, 50);
    assert.deepStrictEqual(await store.get('t0'), { i: 0 });
  });
});
