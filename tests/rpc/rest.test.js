'use strict';

// Declarative REST: procedure({ http, schema }) — the option validation, the
// router's per-verb route table, the shell's trie dispatch with plain-result
// bodies, and the client's REST leg over the http transport.

const test = require('node:test');
const assert = require('node:assert');

const { defineRouter, procedure, effectiveSchema } = require('../../index.js');
const { bootServer, connectClient } = require('../helpers/server.js');

const api = () =>
  defineRouter({
    projects: {
      create: procedure({
        access: 'public',
        http: { method: 'POST', path: '/projects/:orgId', status: 201 },
        handler: async (_ctx, { params, query, body }) => ({ orgId: params.orgId, name: body?.name, q: query }),
      }),
      findById: procedure({
        access: 'public',
        http: { method: 'GET', path: '/projects/:id' },
        handler: async (_ctx, { params, query }) => ({ id: params.id, q: query }),
      }),
      remove: procedure({
        access: 'public',
        http: { method: 'DELETE', path: '/projects/:id', status: 204 },
        handler: async () => ({ discarded: true }),
      }),
      boom: procedure({
        access: 'public',
        http: { method: 'GET', path: '/projects/:id/boom' },
        handler: async () => {
          const error = new Error('gone');
          error.code = 404;
          error.details = { why: 'archived' };
          throw error;
        },
      }),
      plain: procedure({ access: 'public', handler: async (_ctx, args) => ({ plain: true, args }) }),
    },
    misc: {
      // Deliberately outside every trie path: the conventional-mode probe.
      plain: procedure({ access: 'public', handler: async (_ctx, args) => ({ plain: true, args }) }),
    },
  });

test('procedure({ http }) option validation', async (t) => {
  const handler = async () => 1;

  await t.test('rejects unknown verbs, bad paths and bad statuses', () => {
    assert.throws(() => procedure({ handler, http: { method: 'FETCH', path: '/x' } }), /http\.method/);
    assert.throws(() => procedure({ handler, http: { method: 'GET', path: 'x' } }), /http\.path/);
    assert.throws(() => procedure({ handler, http: { method: 'GET', path: '/a/*' } }), /invalid segment/);
    assert.throws(() => procedure({ handler, http: { method: 'GET', path: '/x', status: 42 } }), /http\.status/);
  });

  await t.test('refused on subscriptions', () => {
    assert.throws(
      () => procedure({ handler: async function* () {}, http: { method: 'GET', path: '/x' } }),
      /does not support http/,
    );
  });

  await t.test('schema and input/output are mutually exclusive', () => {
    assert.throws(
      () => procedure({ handler, schema: { body: { type: 'object' } }, input: () => {} }),
      /mutually exclusive/,
    );
  });

  await t.test('query and querystring are interchangeable; both-and-different throws', () => {
    const q = { type: 'object' };
    assert.deepStrictEqual(procedure({ handler, schema: { query: q } }).schema, { querystring: q });
    assert.deepStrictEqual(procedure({ handler, schema: { querystring: q } }).schema, { querystring: q });
    assert.strictEqual(procedure({ handler, schema: { query: q, querystring: q } }).schema.querystring, q);
    assert.throws(() => procedure({ handler, schema: { query: q, querystring: { type: 'string' } } }), /use one/);
  });
});

test('router REST table', async (t) => {
  await t.test('per-verb trees: same position, different param names, different verbs', () => {
    const router = defineRouter({
      projects: {
        create: { access: 'public', http: { method: 'POST', path: '/projects/:orgId' }, handler: async () => 1 },
        findById: { access: 'public', http: { method: 'GET', path: '/projects/:id' }, handler: async () => 2 },
      },
    });
    assert.deepStrictEqual(router.matchRest('POST', ['projects', '7']).params, { orgId: '7' });
    assert.deepStrictEqual(router.matchRest('GET', ['projects', '7']).params, { id: '7' });
  });

  await t.test('static beats param; unknown path answers null; wrong verb answers allowed', () => {
    const router = api();
    assert.strictEqual(router.matchRest('GET', ['projects', 'x']).methodName, 'findById');
    assert.strictEqual(router.matchRest('GET', ['nope']), null);
    assert.deepStrictEqual(router.matchRest('PATCH', ['projects', 'x']).allowed.sort(), ['DELETE', 'GET', 'POST']);
  });

  await t.test('duplicate verb+path and mixed param names in one verb throw at build', () => {
    assert.throws(
      () =>
        defineRouter({
          a: { one: { http: { method: 'GET', path: '/x' }, handler: async () => 1 } },
          b: { two: { http: { method: 'GET', path: '/x' }, handler: async () => 2 } },
        }),
      /declared by both a\/one and b\/two/,
    );
    assert.throws(
      () =>
        defineRouter({
          a: {
            one: { http: { method: 'GET', path: '/x/:id' }, handler: async () => 1 },
            two: { http: { method: 'GET', path: '/x/:key/deep' }, handler: async () => 2 },
          },
        }),
      /same position/,
    );
  });

  await t.test('merge() carries the REST table', () => {
    const merged = api().merge(defineRouter({ extra: { ping: { access: 'public', handler: async () => 1 } } }));
    assert.strictEqual(merged.matchRest('POST', ['projects', '1']).methodName, 'create');
  });

  await t.test('introspection exposes the mapping', () => {
    const info = api().introspect().projects;
    assert.deepStrictEqual(info.create.http, { method: 'POST', path: '/projects/:orgId', status: 201 });
    assert.strictEqual('http' in info.plain, false);
  });
});

test('effectiveSchema: default wrpc error responses with user override', async (t) => {
  await t.test('derived from the procedure options', () => {
    const proc = procedure({
      handler: async () => 1,
      timeout: 50,
      schema: { body: { type: 'object' }, response: { 200: { type: 'object' } } },
    });
    const codes = Object.keys(effectiveSchema(proc).response).sort();
    assert.deepStrictEqual(codes, ['200', '400', '403', '408', '429', '500', '503']);
  });

  await t.test('a public, schemaless, untimed call documents only the always-set', () => {
    const proc = procedure({ access: 'public', handler: async () => 1 });
    assert.deepStrictEqual(Object.keys(effectiveSchema(proc).response).sort(), ['429', '500', '503']);
  });

  await t.test('response[code] overrides; false removes', () => {
    const mine = { type: 'object', properties: { custom: { type: 'string' } } };
    const proc = procedure({
      access: 'public',
      handler: async () => 1,
      schema: { response: { 500: mine, 503: false } },
    });
    const { response } = effectiveSchema(proc);
    assert.strictEqual(response[500], mine);
    assert.strictEqual('503' in response, false);
  });
});

test('invokeBare keeps queue/timeout semantics without hooks or validators', async (t) => {
  await t.test('timeout answers 408', async () => {
    const proc = procedure({
      access: 'public',
      timeout: 20,
      handler: () => new Promise(() => {}),
    });
    await assert.rejects(proc.invokeBare({ signal: null }, {}), (error) => error.code === 408);
  });

  await t.test('queue overflow answers 503; the slot is held until the handler settles', async () => {
    let release;
    const gate = new Promise((resolve) => {
      release = resolve;
    });
    const proc = procedure({
      access: 'public',
      queue: { concurrency: 1, size: 0 },
      handler: () => gate,
    });
    const first = proc.invokeBare({ signal: null }, {});
    await assert.rejects(proc.invokeBare({ signal: null }, {}), (error) => error.code === 503);
    release(42);
    assert.strictEqual(await first, 42);
  });

  await t.test('skips input validators and hooks by contract', async () => {
    const proc = procedure({
      access: 'public',
      input: () => {
        throw new Error('input validator must not run');
      },
      preHandler: () => {
        throw new Error('hook must not run');
      },
      handler: async (_ctx, args) => args,
    });
    assert.deepStrictEqual(await proc.invokeBare({ signal: null }, { ok: 1 }), { ok: 1 });
  });
});

test('shell REST dispatch: trie hits answer plain results', async (t) => {
  const { server, port } = await bootServer(t, { router: api() });
  const base = `http://127.0.0.1:${port}${server.rpc.basePath}`;

  await t.test('POST with params, query and body; status from http.status', async () => {
    const res = await fetch(`${base}/projects/42?x=1`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: 'Alpha' }),
    });
    assert.strictEqual(res.status, 201);
    assert.deepStrictEqual(await res.json(), { orgId: '42', name: 'Alpha', q: { x: '1' } });
  });

  await t.test('204 discards the result body', async () => {
    const res = await fetch(`${base}/projects/42`, { method: 'DELETE' });
    assert.strictEqual(res.status, 204);
    assert.strictEqual(await res.text(), '');
  });

  await t.test('a known path with the wrong verb answers 405 with Allow, in REST shape', async () => {
    const res = await fetch(`${base}/projects/42`, { method: 'PATCH' });
    assert.strictEqual(res.status, 405);
    assert.deepStrictEqual(res.headers.get('allow').split(', ').sort(), ['DELETE', 'GET', 'POST']);
    assert.deepStrictEqual(await res.json(), { message: 'Method Not Allowed', code: 405 });
  });

  await t.test('errors answer the wire error object, details included', async () => {
    const res = await fetch(`${base}/projects/1/boom`);
    assert.strictEqual(res.status, 404);
    assert.deepStrictEqual(await res.json(), { message: 'gone', code: 404, details: { why: 'archived' } });
  });

  await t.test('path params are percent-decoded; malformed escapes answer 400', async () => {
    const res = await fetch(`${base}/projects/a%2Fb`);
    assert.deepStrictEqual((await res.json()).id, 'a/b');
    const bad = await fetch(`${base}/projects/%ZZ`);
    assert.strictEqual(bad.status, 400);
  });

  await t.test('the conventional /:unit/:method mode still answers callback packets', async () => {
    const res = await fetch(`${base}/misc/plain?x=1`);
    assert.strictEqual(res.status, 200);
    const packet = await res.json();
    assert.strictEqual(packet.type, 'callback');
    assert.deepStrictEqual(packet.result, { plain: true, args: { x: '1' } });
  });
});

test('injected querystring codec parses REST queries', async (t) => {
  const seen = [];
  const querystring = {
    parse: (text) => {
      seen.push(text);
      return { parsed: text };
    },
  };
  const { server, port } = await bootServer(t, { router: api(), querystring });
  const base = `http://127.0.0.1:${port}${server.rpc.basePath}`;
  const res = await fetch(`${base}/projects/9?a[0]=1`);
  assert.deepStrictEqual((await res.json()).q, { parsed: 'a[0]=1' });
  assert.ok(seen.length > 0);
});

test('client REST leg: mapped procedures travel as real REST requests', async (t) => {
  const { server, port } = await bootServer(t, { router: api() });
  const base = `http://127.0.0.1:${port}${server.rpc.basePath}`;

  await t.test('same args shape, plain result, status honoured', async () => {
    const client = await connectClient(t, base, { transport: 'http' });
    await client.load('projects');
    const created = await client.api.projects.create({
      params: { orgId: '7' },
      query: { x: '1' },
      body: { name: 'B' },
    });
    assert.deepStrictEqual(created, { orgId: '7', name: 'B', q: { x: '1' } });
    assert.strictEqual(await client.api.projects.remove({ params: { id: '7' } }), undefined);
  });

  await t.test('a wire error rejects with WrpcError carrying code and details', async () => {
    const client = await connectClient(t, base, { transport: 'http' });
    await client.load('projects');
    await assert.rejects(client.api.projects.boom({ params: { id: '1' } }), (error) => {
      assert.strictEqual(error.code, 404);
      assert.strictEqual(error.message, 'gone');
      assert.deepStrictEqual(error.details, { why: 'archived' });
      return true;
    });
  });

  await t.test('a missing path param fails locally with 400', async () => {
    const client = await connectClient(t, base, { transport: 'http' });
    await client.load('projects');
    await assert.rejects(client.api.projects.findById({ query: {} }), (error) => error.code === 400);
  });

  await t.test('unmapped procedures keep packet mode', async () => {
    const client = await connectClient(t, base, { transport: 'http' });
    await client.load('projects');
    assert.deepStrictEqual(await client.api.projects.plain({ n: 1 }), { plain: true, args: { n: 1 } });
  });

  await t.test('the injected client querystring serializer builds the query', async () => {
    const client = await connectClient(t, base, {
      transport: 'http',
      querystring: { stringify: () => 'x=custom' },
    });
    await client.load('projects');
    const found = await client.api.projects.findById({ params: { id: '1' }, query: { anything: true } });
    assert.deepStrictEqual(found.q, { x: 'custom' });
  });

  await t.test('a ws client passes the same shape through packet mode', async () => {
    const client = await connectClient(t, `ws://127.0.0.1:${port}${server.rpc.basePath}`);
    await client.load('projects');
    const created = await client.api.projects.create({ params: { orgId: '9' }, query: {}, body: { name: 'C' } });
    assert.deepStrictEqual(created, { orgId: '9', name: 'C', q: {} });
  });
});

// ---------------------------------------------------------------------------
// Injected validation compilers (phase: ajv/fjs). Hand-rolled ajv-shaped
// fakes — no schema library enters devDependencies.

const fakeAjv = (log = []) => ({
  compile: (schema) => {
    log.push(schema);
    const validate = (value) => {
      validate.errors = null;
      if (schema.required) {
        for (const key of schema.required) {
          if (value?.[key] === undefined) {
            validate.errors = [{ message: `must have required property '${key}'`, instancePath: '' }];
            return false;
          }
        }
      }
      return true;
    };
    return validate;
  },
});

test('injected validation: compile-once, per-part issues, details', async (t) => {
  await t.test('compiles each declared part exactly once, at build', () => {
    const compiled = [];
    defineRouter(
      {
        p: {
          make: procedure({
            access: 'public',
            schema: { params: { id: 1 }, querystring: { id: 2 }, body: { id: 3 }, response: { 200: { id: 4 } } },
            handler: async () => 1,
          }),
        },
      },
      { validation: { ajv: fakeAjv(compiled) } },
    );
    assert.deepStrictEqual(compiled.map((s) => s.id).sort(), [1, 2, 3, 4]);
  });

  await t.test('a failing part rejects 400 with part-prefixed issue paths', async () => {
    const router = defineRouter(
      {
        p: {
          make: procedure({
            access: 'public',
            schema: { body: { required: ['name'] } },
            handler: async (_ctx, args) => args,
          }),
        },
      },
      { validation: { ajv: fakeAjv() } },
    );
    const proc = router.getProcedure('p', '*', 'make');
    await assert.rejects(proc.invoke({ signal: null }, { body: {} }, undefined, router.compiledFor(proc)), (error) => {
      assert.strictEqual(error.code, 400);
      assert.deepStrictEqual(error.details, {
        issues: [{ message: "must have required property 'name'", path: '/body' }],
      });
      return true;
    });
  });

  await t.test('a schema with parts and no injected ajv throws at build, naming the procedure', () => {
    assert.throws(
      () =>
        defineRouter({
          p: { make: procedure({ access: 'public', schema: { body: {} }, handler: async () => 1 }) },
        }),
      /p\/make declares schema validation/,
    );
  });

  await t.test('merge recompiles against the surviving compilers', () => {
    const compiled = [];
    const withAjv = defineRouter({}, { validation: { ajv: fakeAjv(compiled) } });
    const other = defineRouter({}, { validation: { ajv: fakeAjv() } });
    other.addUnit('p', { make: procedure({ access: 'public', schema: { body: {} }, handler: async () => 1 }) });
    const merged = withAjv.merge(other);
    const proc = merged.getProcedure('p', '*', 'make');
    assert.notStrictEqual(merged.compiledFor(proc), null);
    assert.ok(compiled.length > 0, 'the surviving ajv compiled the schema');
  });
});

test('injected serializer: the compiled fast path feeds the wire', async (t) => {
  const serializer = {
    compile: (schema) => (value) => JSON.stringify({ ...value, serialized: true }),
  };
  const router = defineRouter(
    {
      p: {
        make: procedure({
          access: 'public',
          schema: { response: { 200: { type: 'object' } } },
          handler: async () => ({ n: 1 }),
        }),
        hooked: procedure({
          access: 'public',
          schema: { response: { 200: { type: 'object' } } },
          handler: async () => ({ n: 2 }),
        }),
      },
      // onSend disables the fast path — the hook may reshape the packet.
      q: {
        hooks: { onSend: async () => {} },
        make: procedure({
          access: 'public',
          schema: { response: { 200: { type: 'object' } } },
          handler: async () => ({ n: 3 }),
        }),
      },
    },
    { validation: { ajv: fakeAjv(), serializer } },
  );
  assert.strictEqual(router.hasSerializers, true);

  const { bootServer: boot } = require('../helpers/server.js');
  const { server, port } = await boot(t, { router });
  const base = `http://127.0.0.1:${port}${server.rpc.basePath}`;

  await t.test('the serialized text is what arrives', async () => {
    const res = await fetch(`${base}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'call', id: '1', method: 'p/make', args: {} }),
    });
    const packet = await res.json();
    assert.deepStrictEqual(packet.result, { n: 1, serialized: true });
  });

  await t.test('an onSend hook takes the slow path — the mutated packet wins', async () => {
    const res = await fetch(`${base}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'call', id: '1', method: 'q/make', args: {} }),
    });
    const packet = await res.json();
    // Slow path: plain JSON.stringify of the packet — no `serialized` mark.
    assert.deepStrictEqual(packet.result, { n: 3 });
  });
});

test('introspection carries input schema parts; client prevalidates with injected ajv', async (t) => {
  const routerWithSchemas = () =>
    defineRouter(
      {
        p: {
          make: procedure({
            access: 'public',
            schema: { body: { required: ['name'] }, response: { 200: { type: 'object' } } },
            handler: async (_ctx, { body }) => ({ made: body.name }),
          }),
        },
      },
      { validation: { ajv: fakeAjv() } },
    );

  await t.test('info.schema holds the input parts only', () => {
    const info = routerWithSchemas().introspect().p.make;
    assert.deepStrictEqual(info.schema, { body: { required: ['name'] } });
  });

  await t.test('introspection option { schemas: false } strips them', () => {
    const info = routerWithSchemas().introspect(null, { schemas: false }).p.make;
    assert.strictEqual('schema' in info, false);
  });

  await t.test('the client rejects locally, no request made', async () => {
    const { server, port } = await bootServer(t, { router: routerWithSchemas() });
    const client = await connectClient(t, `ws://127.0.0.1:${port}${server.rpc.basePath}`, {
      validation: { ajv: fakeAjv() },
    });
    await client.load('p');
    let requests = 0;
    for (const peer of server.rpc.clients) peer.on('close', () => {});
    server.rpc.router.addHook?.('onRequest', async () => void requests++);
    await assert.rejects(client.api.p.make({ body: {} }), (error) => {
      assert.strictEqual(error.code, 400);
      assert.deepStrictEqual(error.details, {
        issues: [{ message: "must have required property 'name'", path: '/body' }],
      });
      return true;
    });
    assert.strictEqual(requests, 0, 'the doomed call never left the client');
    assert.deepStrictEqual(await client.api.p.make({ body: { name: 'x' } }), { made: 'x' });
  });

  await t.test('without injected ajv the client sends and the server answers 400', async () => {
    const { server, port } = await bootServer(t, { router: routerWithSchemas() });
    const client = await connectClient(t, `ws://127.0.0.1:${port}${server.rpc.basePath}`);
    await client.load('p');
    await assert.rejects(client.api.p.make({ body: {} }), (error) => error.code === 400);
  });
});
