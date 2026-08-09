'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { ServerTransport, buildHeaders } = require('../src/transport.js');

const { http: ServerHttpTransport, ws: ServerWsTransport, event: ServerEventTransport } = ServerTransport.transport;

const fakeSocket = () => ({ remoteAddress: '127.0.0.1', destroy: () => {} });

const fakeReq = (overrides = {}) => ({
  socket: fakeSocket(),
  method: 'POST',
  headers: {},
  on: () => {},
  ...overrides,
});

const fakeRes = () => {
  const res = {
    writableEnded: false,
    headersSent: false,
    headers: null,
    statusCode: null,
    ended: false,
    cookies: [],
    writeHead(code, headers) {
      res.statusCode = code;
      res.headers = headers;
      res.headersSent = true;
    },
    end(buf) {
      res.ended = true;
      res.body = buf;
    },
    setHeader(name, value) {
      if (name === 'Set-Cookie') res.cookies.push(value);
    },
  };
  return res;
};

test('buildHeaders', () => {
  const defaults = buildHeaders();
  assert.strictEqual(defaults['Access-Control-Allow-Origin'], '*');

  const withoutOrigin = buildHeaders({});
  assert.strictEqual(withoutOrigin['Access-Control-Allow-Origin'], '*');

  const withCors = buildHeaders({ origin: 'https://example.com' });
  assert.strictEqual(withCors['Access-Control-Allow-Origin'], 'https://example.com');
});

test('ServerHttpTransport', async (t) => {
  await t.test('responds to OPTIONS requests immediately', () => {
    const res = fakeRes();
    const req = fakeReq({ method: 'OPTIONS' });
    const transport = new ServerHttpTransport(req, res);
    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.ended, true);
    assert.ok(transport);
  });

  await t.test('options() is a no-op once headers are already sent', () => {
    const res = fakeRes();
    res.headersSent = true;
    const req = fakeReq({ method: 'OPTIONS' });
    const transport = new ServerHttpTransport(req, res);
    assert.strictEqual(res.statusCode, null);
    transport.options();
    assert.strictEqual(res.statusCode, null);
  });

  await t.test('getCookies returns {} when there is no cookie header', () => {
    const transport = new ServerHttpTransport(fakeReq(), fakeRes());
    assert.deepStrictEqual(transport.getCookies(), {});
  });

  await t.test('getCookies parses the cookie header', () => {
    const req = fakeReq({ headers: { cookie: 'token=abc123; other=value' } });
    const transport = new ServerHttpTransport(req, fakeRes());
    assert.deepStrictEqual(transport.getCookies(), { token: 'abc123', other: 'value' });
  });

  await t.test('getCookies treats a valueless cookie item as an empty string', () => {
    const req = fakeReq({ headers: { cookie: 'flag' } });
    const transport = new ServerHttpTransport(req, fakeRes());
    assert.deepStrictEqual(transport.getCookies(), { flag: '' });
  });

  await t.test('error() falls back to the HTTP status text when no Error is given', () => {
    const transport = new ServerHttpTransport(fakeReq(), fakeRes());
    let sent = null;
    transport.send = (obj) => (sent = obj);
    transport.error(500);
    assert.strictEqual(sent.error.message, 'Internal Server Error');
  });

  await t.test('write() is a no-op once the response has already ended', () => {
    const res = fakeRes();
    res.writableEnded = true;
    const transport = new ServerHttpTransport(fakeReq(), res);
    transport.write('too late');
    assert.strictEqual(res.ended, false);
  });

  await t.test('write() passes an existing Buffer through unchanged', () => {
    const res = fakeRes();
    const transport = new ServerHttpTransport(fakeReq(), res);
    const buf = Buffer.from('already a buffer');
    transport.write(buf);
    assert.strictEqual(res.body, buf);
  });

  await t.test('error() falls back to "Unknown error" for a status code with no known text', () => {
    const transport = new ServerHttpTransport(fakeReq(), fakeRes());
    let sent = null;
    transport.send = (obj) => (sent = obj);
    transport.error(999);
    assert.strictEqual(sent.error.message, 'Unknown error');
  });

  await t.test('sendSessionCookie sets a Set-Cookie header scoped to the request host', () => {
    const req = fakeReq({ headers: { host: 'localhost:8000' } });
    const res = fakeRes();
    const transport = new ServerHttpTransport(req, res);
    transport.sendSessionCookie('my-token');
    assert.strictEqual(res.cookies.length, 1);
    assert.match(res.cookies[0], /^token=my-token;.*Domain=localhost$/);
  });

  await t.test('sendSessionCookie falls back when the host header is missing', () => {
    const req = fakeReq({ headers: {} });
    const res = fakeRes();
    const transport = new ServerHttpTransport(req, res);
    transport.sendSessionCookie('my-token');
    assert.match(res.cookies[0], /Domain=no-host-name-in-http-headers$/);
  });

  await t.test('removeSessionCookie clears the session cookie', () => {
    const req = fakeReq({ headers: { host: 'localhost' } });
    const res = fakeRes();
    const transport = new ServerHttpTransport(req, res);
    transport.removeSessionCookie();
    assert.match(res.cookies[0], /^token=deleted;.*Domain=localhost$/);
  });

  await t.test('close sends a 503 and destroys the socket', () => {
    const res = fakeRes();
    let destroyed = false;
    const req = fakeReq({ socket: { ...fakeSocket(), destroy: () => (destroyed = true) } });
    const transport = new ServerHttpTransport(req, res);
    transport.close();
    assert.strictEqual(res.statusCode, 503);
    assert.strictEqual(destroyed, true);
  });
});

test('ServerWsTransport forwards close from the connection', () => {
  const listeners = {};
  const connection = {
    on: (event, fn) => (listeners[event] = fn),
    send: () => {},
    terminate: () => {},
  };
  const req = fakeReq();
  const transport = new ServerWsTransport(req, connection);
  let closed = false;
  transport.on('close', () => (closed = true));
  listeners.close();
  assert.strictEqual(closed, true);
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
