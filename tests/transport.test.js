'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { ServerTransport, buildHeaders, parseCookies } = require('../src/transport.js');
const { Emitter } = require('../src/utils.js');

const { http: ServerHttpTransport, ws: ServerWsTransport, event: ServerEventTransport } = ServerTransport.transport;

class FakeConnection extends Emitter {
  constructor() {
    super();
    this.remoteAddress = '10.0.0.1';
    this.sent = [];
    this.sendResult = true;
    this.terminated = false;
  }

  send(data) {
    this.sent.push(data);
    return this.sendResult;
  }

  terminate() {
    this.terminated = true;
  }
}

const fakeCall = (overrides = {}) => {
  const responses = [];
  return {
    responses,
    method: 'POST',
    url: '/api',
    headers: {},
    body: null,
    remoteAddress: '127.0.0.1',
    respond: (response) => responses.push(response),
    ...overrides,
  };
};

test('buildHeaders', async (t) => {
  await t.test('defaults to a wildcard origin without a cors option', () => {
    const defaults = buildHeaders();
    assert.strictEqual(defaults['Access-Control-Allow-Origin'], '*');
    assert.strictEqual(defaults['Access-Control-Allow-Methods'], 'POST, GET, OPTIONS');
    // The SSE transport's own headers are in the default: neither is
    // CORS-safelisted, so leaving them out fails the preflight.
    assert.strictEqual(
      defaults['Access-Control-Allow-Headers'],
      'Content-Type, x-wrpc-channel, last-event-id, x-wrpc-meta',
    );
    assert.strictEqual(defaults['Vary'], undefined);
    assert.strictEqual(defaults['Content-Type'], 'application/json');

    const withoutOrigins = buildHeaders({}, 'https://example.com');
    assert.strictEqual(withoutOrigins['Access-Control-Allow-Origin'], '*');
  });

  await t.test('origins list: allowed origin is echoed with Vary: Origin', () => {
    const cors = { origins: ['https://a.com', 'https://b.com'] };
    const allowed = buildHeaders(cors, 'https://a.com');
    assert.strictEqual(allowed['Access-Control-Allow-Origin'], 'https://a.com');
    assert.strictEqual(allowed['Vary'], 'Origin');
    assert.strictEqual(allowed['Access-Control-Allow-Credentials'], undefined);
  });

  await t.test('origins list: disallowed origin gets no ACAO header', () => {
    const cors = { origins: ['https://a.com'] };
    const denied = buildHeaders(cors, 'https://evil.com');
    assert.strictEqual(denied['Access-Control-Allow-Origin'], undefined);
    assert.strictEqual(denied['Vary'], 'Origin');
    const missing = buildHeaders(cors, undefined);
    assert.strictEqual(missing['Access-Control-Allow-Origin'], undefined);
  });

  await t.test('origins as a predicate function', () => {
    const cors = { origins: (origin) => origin.endsWith('.good.com') };
    assert.strictEqual(
      buildHeaders(cors, 'https://app.good.com')['Access-Control-Allow-Origin'],
      'https://app.good.com',
    );
    assert.strictEqual(buildHeaders(cors, 'https://app.bad.com')['Access-Control-Allow-Origin'], undefined);
    assert.strictEqual(buildHeaders(cors, undefined)['Access-Control-Allow-Origin'], undefined);
  });

  await t.test('credentials are echoed only for allowed origins', () => {
    const cors = { origins: ['https://a.com'], credentials: true };
    const allowed = buildHeaders(cors, 'https://a.com');
    assert.strictEqual(allowed['Access-Control-Allow-Credentials'], 'true');
    const denied = buildHeaders(cors, 'https://evil.com');
    assert.strictEqual(denied['Access-Control-Allow-Credentials'], undefined);
  });

  await t.test('methods and headers overrides', () => {
    const headers = buildHeaders({ methods: 'PUT, PATCH', headers: 'Content-Type, X-Token' });
    assert.strictEqual(headers['Access-Control-Allow-Methods'], 'PUT, PATCH');
    assert.strictEqual(headers['Access-Control-Allow-Headers'], 'Content-Type, X-Token');
  });
});

test('ServerHttpTransport', async (t) => {
  await t.test('write responds exactly once and emits close after the write', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    const events = [];
    transport.on('close', () => events.push('close'));

    assert.strictEqual(transport.responded, false);
    transport.send({ ok: true });
    assert.strictEqual(transport.responded, true);
    assert.deepStrictEqual(events, ['close']);

    assert.strictEqual(call.responses.length, 1);
    const [{ status, headers, body }] = call.responses;
    assert.strictEqual(status, 200);
    assert.strictEqual(headers['Content-Type'], 'application/json');
    assert.strictEqual(headers['Content-Length'], body.length);
    assert.deepStrictEqual(JSON.parse(body.toString()), { ok: true });

    transport.send({ tooLate: true });
    assert.strictEqual(call.responses.length, 1);
  });

  await t.test('write passes an existing Buffer through unchanged', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    const buf = Buffer.from('already a buffer');
    transport.write(buf);
    assert.strictEqual(call.responses[0].body, buf);
  });

  await t.test('custom headers from options are used for the response', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call, { headers: { 'X-Custom': 'yes' } });
    transport.write('data');
    const [{ headers }] = call.responses;
    assert.strictEqual(headers['X-Custom'], 'yes');
  });

  await t.test('error() sends a callback packet with the HTTP status text', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    transport.error(500);
    const [{ status, body }] = call.responses;
    assert.strictEqual(status, 500);
    const packet = JSON.parse(body.toString());
    assert.deepStrictEqual(packet, {
      type: 'callback',
      id: '',
      error: { message: 'Internal Server Error', code: 500 },
    });
  });

  await t.test('error() falls back to "Unknown error" for an unknown status code', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    transport.error(999);
    const packet = JSON.parse(call.responses[0].body.toString());
    assert.strictEqual(packet.error.message, 'Unknown error');
  });

  await t.test('error() prefers the given error message and keeps the packet id', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    transport.error(422, { id: '42', error: new Error('domain failure') });
    const packet = JSON.parse(call.responses[0].body.toString());
    assert.deepStrictEqual(packet, { type: 'callback', id: '42', error: { message: 'domain failure', code: 422 } });
  });

  await t.test('error() carries details on a 4xx', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    const error = new Error('invalid');
    error.details = { issues: [{ message: 'name required', path: ['name'] }] };
    transport.error(400, { id: '1', error });
    const packet = JSON.parse(call.responses[0].body.toString());
    assert.deepStrictEqual(packet.error, {
      message: 'invalid',
      code: 400,
      details: { issues: [{ message: 'name required', path: ['name'] }] },
    });
  });

  await t.test('error() strips details from a 5xx', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    const error = new Error('secret internals');
    error.details = { query: 'SELECT *' };
    transport.error(500, { id: '1', error });
    const packet = JSON.parse(call.responses[0].body.toString());
    assert.strictEqual(packet.error.message, 'Internal Server Error');
    assert.strictEqual('details' in packet.error, false);
  });

  await t.test('error() keeps 5xx details when the error opts in with expose', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    const error = new Error('queue full');
    error.expose = true;
    error.details = { waiting: 100 };
    transport.error(503, { id: '1', error });
    const packet = JSON.parse(call.responses[0].body.toString());
    assert.deepStrictEqual(packet.error, { message: 'queue full', code: 503, details: { waiting: 100 } });
  });

  await t.test('error() omits the details key entirely when there are none', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    transport.error(404, { id: '1', error: new Error('nope') });
    const packet = JSON.parse(call.responses[0].body.toString());
    assert.strictEqual('details' in packet.error, false);
  });

  await t.test('getCookies returns {} when there is no cookie header', () => {
    const transport = new ServerHttpTransport(fakeCall());
    assert.deepStrictEqual(transport.getCookies(), {});
  });

  await t.test('getCookies parses the cookie header', () => {
    const call = fakeCall({ headers: { cookie: 'token=abc123; other=value' } });
    const transport = new ServerHttpTransport(call);
    assert.deepStrictEqual(transport.getCookies(), { token: 'abc123', other: 'value' });
  });

  await t.test('getCookies treats a valueless cookie item as an empty string', () => {
    const call = fakeCall({ headers: { cookie: 'flag' } });
    const transport = new ServerHttpTransport(call);
    assert.deepStrictEqual(transport.getCookies(), { flag: '' });
  });

  await t.test('sendSessionCookie accumulates Set-Cookie headers for the response', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    transport.sendSessionCookie('token=abc; Path=/; HttpOnly');
    transport.sendSessionCookie('theme=dark; Path=/');
    transport.send({ ok: true });
    const [{ headers }] = call.responses;
    assert.deepStrictEqual(headers['Set-Cookie'], ['token=abc; Path=/; HttpOnly', 'theme=dark; Path=/']);
  });

  await t.test('no Set-Cookie header when no cookie was sent', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    transport.send({ ok: true });
    assert.strictEqual('Set-Cookie' in call.responses[0].headers, false);
  });

  await t.test('close() responds with a 503 error packet', () => {
    const call = fakeCall();
    const transport = new ServerHttpTransport(call);
    transport.close();
    const [{ status, body }] = call.responses;
    assert.strictEqual(status, 503);
    const packet = JSON.parse(body.toString());
    assert.strictEqual(packet.error.code, 503);
    assert.strictEqual(packet.error.message, 'Service Unavailable');
  });

  await t.test('source comes from the call remoteAddress', () => {
    assert.strictEqual(new ServerHttpTransport(fakeCall()).source, '127.0.0.1');
    assert.strictEqual(new ServerHttpTransport(fakeCall({ remoteAddress: undefined })).source, '');
  });
});

test('ServerWsTransport', async (t) => {
  await t.test('source prefers meta.remoteAddress, falls back to the connection', () => {
    const connection = new FakeConnection();
    assert.strictEqual(new ServerWsTransport(connection, { remoteAddress: '192.168.0.7' }).source, '192.168.0.7');
    assert.strictEqual(new ServerWsTransport(new FakeConnection()).source, '10.0.0.1');
    const anonymous = new FakeConnection();
    anonymous.remoteAddress = undefined;
    assert.strictEqual(new ServerWsTransport(anonymous).source, '');
  });

  await t.test('forwards close and drain from the connection', async () => {
    const connection = new FakeConnection();
    const transport = new ServerWsTransport(connection, {});
    const events = [];
    transport.on('close', () => events.push('close'));
    transport.on('drain', () => events.push('drain'));
    await connection.emit('drain');
    await connection.emit('close');
    assert.deepStrictEqual(events, ['drain', 'close']);
  });

  await t.test('write() passes the connection backpressure boolean through', () => {
    const connection = new FakeConnection();
    const transport = new ServerWsTransport(connection, {});
    assert.strictEqual(transport.write('fits'), true);
    connection.sendResult = false;
    assert.strictEqual(transport.write('stalls'), false);
    assert.deepStrictEqual(connection.sent, ['fits', 'stalls']);
  });

  await t.test('write() converts typed arrays to Buffers, keeps strings and Buffers', () => {
    const connection = new FakeConnection();
    const transport = new ServerWsTransport(connection, {});
    const buf = Buffer.from('as-is');
    transport.write(buf);
    assert.strictEqual(connection.sent[0], buf);
    transport.write(new Uint8Array([1, 2, 3]));
    assert.ok(Buffer.isBuffer(connection.sent[1]));
    assert.deepStrictEqual(connection.sent[1], Buffer.from([1, 2, 3]));
  });

  await t.test('close() terminates the connection', () => {
    const connection = new FakeConnection();
    const transport = new ServerWsTransport(connection, {});
    transport.close();
    assert.strictEqual(connection.terminated, true);
  });
});

test('ServerEventTransport', async (t) => {
  await t.test('writes post messages to the port and closes it', () => {
    const listeners = {};
    const sent = [];
    let portClosed = false;
    const port = {
      on: (event, fn) => (listeners[event] = fn),
      postMessage: (data) => sent.push(data),
      close: () => (portClosed = true),
    };
    const transport = new ServerEventTransport(port);
    transport.write({ hello: true });
    assert.deepStrictEqual(sent, [{ hello: true }]);
    transport.close();
    assert.strictEqual(portClosed, true);
  });

  await t.test('forwards port close as a transport close event', () => {
    const listeners = {};
    const port = { on: (event, fn) => (listeners[event] = fn), postMessage: () => {}, close: () => {} };
    const transport = new ServerEventTransport(port);
    let closed = false;
    transport.on('close', () => (closed = true));
    listeners.close();
    assert.strictEqual(closed, true);
  });
});

test('parseCookies', async (t) => {
  await t.test('splits pairs on the first "=" only — values may contain "="', () => {
    const cookies = parseCookies('token=YWJjZGVm==; other=1');
    assert.strictEqual(cookies.token, 'YWJjZGVm==');
    assert.strictEqual(cookies.other, '1');
    const jwt = parseCookies('token=a.b.c=; x=y=z');
    assert.strictEqual(jwt.token, 'a.b.c=');
    assert.strictEqual(jwt.x, 'y=z');
  });

  await t.test('tolerates malformed items', () => {
    const cookies = parseCookies('lonely; =value;  spaced = padded ');
    assert.strictEqual(cookies.lonely, '');
    assert.strictEqual(cookies.spaced, 'padded');
  });
});

test('buildHeaders: cors.metaHeaders names the per-key meta headers CORS cannot wildcard', () => {
  const { buildHeaders } = require('../src/transport.js');
  const allow = (cors) => buildHeaders(cors, 'http://app.example')['Access-Control-Allow-Headers'];

  // Appended to the default, never replacing it: dropping x-wrpc-channel
  // here would silently disable cross-origin SSE.
  assert.strictEqual(
    allow({ metaHeaders: ['locale'] }),
    'Content-Type, x-wrpc-channel, last-event-id, x-wrpc-meta, x-wrpc-meta-locale',
  );

  // Normalized with the SAME rule the client uses, so a config written in
  // camelCase still grants the name that will actually arrive.
  assert.strictEqual(allow({ metaHeaders: ['userId'] }).endsWith(', x-wrpc-meta-user-id'), true);

  // An explicit list composes with metaHeaders rather than fighting it.
  assert.strictEqual(
    allow({ headers: 'Content-Type', metaHeaders: ['userId', 'traceId'] }),
    'Content-Type, x-wrpc-meta-user-id, x-wrpc-meta-trace-id',
  );

  // The array form of `headers` is the same value, spelled as a list.
  assert.strictEqual(allow({ headers: ['Content-Type', 'X-Token'] }), 'Content-Type, X-Token');

  // Absent or empty changes nothing.
  assert.strictEqual(allow({ metaHeaders: [] }), 'Content-Type, x-wrpc-channel, last-event-id, x-wrpc-meta');
});
