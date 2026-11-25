'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const crypto = require('node:crypto');

const { MAGIC, WebsocketServer } = require('#ws');
const { ProtocolClient } = require('./protocolClient.js');

const parseStatusCode = (statusLine) => {
  if (!statusLine) return null;
  const parts = statusLine.split(' ');
  const code = parseInt(parts[1], 10);
  return Number.isFinite(code) ? code : null;
};

describe('handshake', () => {
  it('should upgrade successfully', async () => {
    const httpServer = http.createServer();
    const tinyWsServer = new WebsocketServer({ server: httpServer });
    tinyWsServer.on('connection', () => {
      // no-op: we only assert that handshake upgraded successfully
    });

    await new Promise((resolve) => httpServer.listen(0, resolve));
    const port = httpServer.address().port;

    const client = new ProtocolClient(`ws://localhost:${port}`);

    const opened = await new Promise((resolve) => {
      client.on('open', () => resolve(true));
      client.on('close', () => resolve(false));
    });

    assert.strictEqual(opened, true);

    // Close client and wait for its socket to finish before closing the server
    await new Promise((resolve) => {
      client.on('close', () => resolve());
      client.close();
    });

    await new Promise((resolve) => httpServer.close(resolve));
  });

  describe('an invalid upgrade request', () => {
    it('should not upgrade without an Upgrade header', async () => {
      const httpServer = http.createServer();
      const tinyWsServer = new WebsocketServer({ server: httpServer });
      tinyWsServer.on('connection', () => {});

      await new Promise((resolve) => httpServer.listen(0, resolve));
      const port = httpServer.address().port;

      const res = await ProtocolClient.attemptHandshake({
        host: 'localhost',
        port,
        headers: {
          // 'Upgrade' missing on purpose
          Connection: 'Upgrade',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': 'dGVzdC1rZXk=',
        },
        timeoutMs: 250,
      });

      // Accept either an explicit non-101 response or immediate
      // socket close without headers
      const code = parseStatusCode(res.statusLine);
      assert.ok(res.statusLine === '' || code !== 101, `expected no upgrade, got: ${res.statusLine}`);

      await new Promise((resolve) => httpServer.close(resolve));
    });

    it('should not upgrade without a Connection header', async () => {
      const httpServer = http.createServer();
      const tinyWsServer = new WebsocketServer({ server: httpServer });
      tinyWsServer.on('connection', () => {});

      await new Promise((resolve) => httpServer.listen(0, resolve));
      const port = httpServer.address().port;

      const res = await ProtocolClient.attemptHandshake({
        host: 'localhost',
        port,
        headers: {
          Upgrade: 'websocket',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': 'dGVzdC1rZXk=',
        },
        timeoutMs: 250,
      });

      const code = parseStatusCode(res.statusLine);
      assert.ok(res.statusLine === '' || code !== 101, `expected no upgrade, got: ${res.statusLine}`);

      await new Promise((resolve) => httpServer.close(resolve));
    });

    it('should not upgrade with a bad Sec-WebSocket-Version', async () => {
      const httpServer = http.createServer();
      const tinyWsServer = new WebsocketServer({ server: httpServer });
      tinyWsServer.on('connection', () => {});

      await new Promise((resolve) => httpServer.listen(0, resolve));
      const port = httpServer.address().port;

      const res = await ProtocolClient.attemptHandshake({
        host: 'localhost',
        port,
        headers: {
          Upgrade: 'websocket',
          Connection: 'Upgrade',
          'Sec-WebSocket-Version': '12',
          'Sec-WebSocket-Key': 'dGVzdC1rZXk=',
        },
        timeoutMs: 250,
      });

      const code = parseStatusCode(res.statusLine);
      assert.ok(res.statusLine === '' || code !== 101, `expected no upgrade, got: ${res.statusLine}`);

      await new Promise((resolve) => httpServer.close(resolve));
    });

    it('should not upgrade with a Sec-WebSocket-Key that is not base64', async () => {
      const httpServer = http.createServer();
      const tinyWsServer = new WebsocketServer({ server: httpServer });
      tinyWsServer.on('connection', () => {});

      await new Promise((resolve) => httpServer.listen(0, resolve));
      const port = httpServer.address().port;

      const res = await ProtocolClient.attemptHandshake({
        host: 'localhost',
        port,
        headers: {
          Upgrade: 'websocket',
          Connection: 'Upgrade',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': '%%%not-base64%%%',
        },
        timeoutMs: 250,
      });

      const code = parseStatusCode(res.statusLine);
      assert.ok(res.statusLine === '' || code !== 101, `expected no upgrade, got: ${res.statusLine}`);

      await new Promise((resolve) => httpServer.close(resolve));
    });

    it('should not upgrade with a Sec-WebSocket-Key too short', async () => {
      const httpServer = http.createServer();
      const tinyWsServer = new WebsocketServer({ server: httpServer });
      tinyWsServer.on('connection', () => {});

      await new Promise((resolve) => httpServer.listen(0, resolve));
      const port = httpServer.address().port;

      const res = await ProtocolClient.attemptHandshake({
        host: 'localhost',
        port,
        headers: {
          Upgrade: 'websocket',
          Connection: 'Upgrade',
          'Sec-WebSocket-Version': '13',
          // base64 is valid, but lower than 16 bytes after decoding
          'Sec-WebSocket-Key': Buffer.from('short').toString('base64'),
        },
        timeoutMs: 250,
      });

      const code = parseStatusCode(res.statusLine);
      assert.ok(res.statusLine === '' || code !== 101, `expected no upgrade, got: ${res.statusLine}`);

      await new Promise((resolve) => httpServer.close(resolve));
    });

    it('should not upgrade with a wrong Upgrade token value', async () => {
      const httpServer = http.createServer();
      const tinyWsServer = new WebsocketServer({ server: httpServer });
      tinyWsServer.on('connection', () => {});

      await new Promise((resolve) => httpServer.listen(0, resolve));
      const port = httpServer.address().port;

      const res = await ProtocolClient.attemptHandshake({
        host: 'localhost',
        port,
        headers: {
          Upgrade: 'notwebsocket',
          Connection: 'Upgrade',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
        },
        timeoutMs: 250,
      });

      const code = parseStatusCode(res.statusLine);
      assert.ok(res.statusLine === '' || code !== 101, `expected no upgrade, got: ${res.statusLine}`);

      await new Promise((resolve) => httpServer.close(resolve));
    });

    it('should not upgrade on Connection without an Upgrade token', async () => {
      const httpServer = http.createServer();
      const tinyWsServer = new WebsocketServer({ server: httpServer });
      tinyWsServer.on('connection', () => {});

      await new Promise((resolve) => httpServer.listen(0, resolve));
      const port = httpServer.address().port;

      const res = await ProtocolClient.attemptHandshake({
        host: 'localhost',
        port,
        headers: {
          Upgrade: 'websocket',
          Connection: 'keep-alive',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
        },
        timeoutMs: 250,
      });

      const code = parseStatusCode(res.statusLine);
      assert.ok(res.statusLine === '' || code !== 101, `expected no upgrade, got: ${res.statusLine}`);

      await new Promise((resolve) => httpServer.close(resolve));
    });

    it('should not upgrade without a Host header', async () => {
      const httpServer = http.createServer();
      const tinyWsServer = new WebsocketServer({ server: httpServer });
      tinyWsServer.on('connection', () => {});

      await new Promise((resolve) => httpServer.listen(0, resolve));
      const port = httpServer.address().port;

      const res = await ProtocolClient.attemptHandshake({
        host: 'localhost',
        port,
        includeHost: false,
        headers: {
          Upgrade: 'websocket',
          Connection: 'Upgrade',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
        },
        timeoutMs: 250,
      });

      const code = parseStatusCode(res.statusLine);
      assert.ok(res.statusLine === '' || code !== 101, `expected no upgrade, got: ${res.statusLine}`);

      await new Promise((resolve) => httpServer.close(resolve));
    });

    it('should not upgrade on a wrong HTTP method (POST)', async () => {
      const httpServer = http.createServer();
      const tinyWsServer = new WebsocketServer({ server: httpServer });
      tinyWsServer.on('connection', () => {});

      await new Promise((resolve) => httpServer.listen(0, resolve));
      const port = httpServer.address().port;

      const res = await ProtocolClient.attemptHandshake({
        host: 'localhost',
        port,
        method: 'POST',
        headers: {
          Upgrade: 'websocket',
          Connection: 'Upgrade',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
        },
        timeoutMs: 250,
      });

      const code = parseStatusCode(res.statusLine);
      assert.ok(res.statusLine === '' || code !== 101, `expected no upgrade, got: ${res.statusLine}`);

      await new Promise((resolve) => httpServer.close(resolve));
    });

    it('should not upgrade an HTTP/1.0 request', async () => {
      const httpServer = http.createServer();
      const tinyWsServer = new WebsocketServer({ server: httpServer });
      tinyWsServer.on('connection', () => {});

      await new Promise((resolve) => httpServer.listen(0, resolve));
      const port = httpServer.address().port;

      const res = await ProtocolClient.attemptHandshake({
        host: 'localhost',
        port,
        httpVersion: '1.0',
        headers: {
          Upgrade: 'websocket',
          Connection: 'Upgrade',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
        },
        timeoutMs: 250,
      });

      const code = parseStatusCode(res.statusLine);
      assert.ok(res.statusLine === '' || code !== 101, `expected no upgrade, got: ${res.statusLine}`);

      await new Promise((resolve) => httpServer.close(resolve));
    });
  });

  it('should send a correct Sec-WebSocket-Accept and no subprotocol', async () => {
    const httpServer = http.createServer();
    const tinyWsServer = new WebsocketServer({ server: httpServer });
    // Terminate immediately to avoid waiting for heartbeat timeouts
    tinyWsServer.on('connection', (ws) => ws.terminate());

    await new Promise((resolve) => httpServer.listen(0, resolve));
    const port = httpServer.address().port;

    // Do a raw handshake to inspect headers
    const key = Buffer.from('0123456789abcdef').toString('base64');
    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      headers: {
        Upgrade: 'websocket',
        Connection: 'Upgrade',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': key,
      },
      timeoutMs: 600,
    });

    // Compute expected accept
    const expected = crypto.createHash('sha1').update(key).update(MAGIC).digest('base64');

    const code = parseStatusCode(res.statusLine);
    assert.strictEqual(code, 101);
    assert.strictEqual(res.headers['sec-websocket-accept'], expected);
    // No subprotocol unless negotiated
    assert.strictEqual(res.headers['sec-websocket-protocol'], undefined);

    await new Promise((resolve) => httpServer.close(resolve));
  });

  it('should match the Connection token case-insensitively', async () => {
    const httpServer = http.createServer();
    const tinyWsServer = new WebsocketServer({ server: httpServer });
    // Terminate immediately to avoid waiting for heartbeat timeouts
    tinyWsServer.on('connection', (ws) => ws.terminate());

    await new Promise((resolve) => httpServer.listen(0, resolve));
    const port = httpServer.address().port;

    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      headers: {
        Upgrade: 'websocket',
        // mixed case and extra tokens
        Connection: 'keep-alive, UpGrAdE',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
      },
      timeoutMs: 600,
    });

    const code = parseStatusCode(res.statusLine);
    assert.strictEqual(code, 101);
    await new Promise((resolve) => httpServer.close(resolve));
  });

  it('should reject the upgrade when the path does not match', async () => {
    const httpServer = http.createServer();
    const tinyWsServer = new WebsocketServer({
      server: httpServer,
      path: '/api',
    });
    tinyWsServer.on('connection', () => {});

    await new Promise((resolve) => httpServer.listen(0, resolve));
    const port = httpServer.address().port;

    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/',
      headers: {
        Upgrade: 'websocket',
        Connection: 'Upgrade',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
      },
      timeoutMs: 600,
    });

    assert.strictEqual(parseStatusCode(res.statusLine), 404);
    await new Promise((resolve) => httpServer.close(resolve));
  });

  it('should let verifyClient reject the upgrade', async () => {
    const httpServer = http.createServer();
    const tinyWsServer = new WebsocketServer({
      server: httpServer,
      verifyClient: () => false,
    });
    tinyWsServer.on('connection', () => {});

    await new Promise((resolve) => httpServer.listen(0, resolve));
    const port = httpServer.address().port;

    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      headers: {
        Upgrade: 'websocket',
        Connection: 'Upgrade',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': Buffer.from('0123456789abcdef').toString('base64'),
      },
      timeoutMs: 600,
    });

    assert.strictEqual(parseStatusCode(res.statusLine), 403);
    await new Promise((resolve) => httpServer.close(resolve));
  });
});
