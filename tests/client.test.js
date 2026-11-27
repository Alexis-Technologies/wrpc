'use strict';

const http = require('node:http');
const timers = require('node:timers/promises');
const { Blob } = require('node:buffer');
const { describe, it, before, after, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');

const { jsonParse } = require('../src/utils.js');
const { WebsocketServer } = require('#ws');
const { WrpcClient } = require('../src/client.js');
const { chunkEncode, chunkDecode } = require('../src/chunks.js');

const { emitWarning } = process;
process.emitWarning = (warning, type, ...args) => {
  if (type === 'ExperimentalWarning') return;
  emitWarning(warning, type, ...args);
};
// Put back for whatever runs after this file in the same process.
after(() => {
  process.emitWarning = emitWarning;
});

const createWsServer = () => {
  const httpServer = http.createServer();
  const wsServer = new WebsocketServer({ server: httpServer });
  return { httpServer, wsServer };
};

const listen = (httpServer, port) =>
  new Promise((resolve, reject) => {
    httpServer.listen(port, (error) => (error ? reject(error) : resolve()));
  });

describe('Client / calls', () => {
  const api = {
    system: {
      introspect: { handler: async () => api },
    },
    test: {
      test: {
        handler: async () => {
          await timers.setTimeout(10);
          return { success: true };
        },
      },
      timeout: {
        handler: async () => {
          await timers.setTimeout(350);
          return { success: true };
        },
      },
      error: {
        handler: async () => {
          throw new Error('Error message');
        },
      },
    },
  };

  let serverWs = null;
  let httpServer = null;

  before(async () => {
    const { httpServer: server, wsServer: mockServer } = createWsServer();
    httpServer = server;
    await listen(httpServer, 8000);
    mockServer.on('connection', (ws) => {
      serverWs = ws;
      ws.on('message', async (raw) => {
        const packet = jsonParse(raw.toString()) || {};
        const { type, id, method } = packet;
        // Calls only: a client also says `cancel` for a call that timed out.
        if (type !== 'call') return;
        const [unit, name] = method.split('/');
        try {
          const result = await api[unit][name].handler();
          ws.send(JSON.stringify({ type: 'callback', id, result }));
        } catch ({ message }) {
          const reply = { type: 'callback', id, error: { code: 400, message } };
          ws.send(JSON.stringify(reply));
        }
      });
    });
  });

  after(() => void httpServer?.close());

  let client;

  beforeEach(async () => {
    const options = { callTimeout: 300 };
    client = await WrpcClient.connect('ws://localhost:8000/', options);
    await client.load('test');
  });

  afterEach(() => void client.close());

  it('should handle simple api calls', async () => {
    const result = await client.api.test.test();
    assert.deepStrictEqual(result, { success: true });
  });

  it('should reach a method whose unit was never loaded with client.call()', async () => {
    // No scaffolding involved: the target is addressed by wire string, which
    // is what an authenticate hook uses before load() has built `api`.
    const fresh = await WrpcClient.connect('ws://localhost:8000/', { callTimeout: 300 });
    try {
      assert.deepStrictEqual(await fresh.call('test/test'), { success: true });
      assert.strictEqual(fresh.api.test, undefined, 'call() must not scaffold anything');
    } finally {
      fresh.close();
    }
  });

  it('should handle parallel api calls', async () => {
    const promises = [];
    for (let i = 0; i < 10; i++) promises.push(client.api.test.test());
    const res = await Promise.all(promises);
    for (const r of res) assert.deepStrictEqual(r, { success: true });
  });

  it('should handle call timeouts', async () => {
    const promise = client.api.test.timeout();
    // Coded like every client-produced refusal: `error.code` checks work.
    await assert.rejects(promise, (error) => error.message === 'Request timeout' && error.code === 408);
  });

  it('should handle api errors', async () => {
    await assert.rejects(client.api.test.error(), (error) => error.message === 'Error message' && error.code === 400);
  });

  it('should emit error when the server sends invalid JSON', async () => {
    const errorPromise = new Promise((resolve) => {
      client.once('error', resolve);
    });
    assert.ok(serverWs, 'expected WebSocket connection from mock server');
    serverWs.send('not json');
    const error = await errorPromise;
    assert.match(error.message, /Invalid JSON packet/);
  });
});

describe('Client / stale callback', () => {
  const api = {
    system: {
      introspect: { handler: async () => api },
    },
    test: {
      test: {
        handler: async () => {
          await timers.setTimeout(10);
          return { success: true };
        },
      },
    },
  };

  let httpServer = null;

  before(async () => {
    const { httpServer: server, wsServer: mockServer } = createWsServer();
    httpServer = server;
    await listen(httpServer, 8010);
    mockServer.on('connection', (ws) => {
      ws.on('message', async (raw) => {
        const packet = jsonParse(raw.toString()) || {};
        const { type, id, method } = packet;
        const [unit, name] = method?.split('/') || [];
        if (type !== 'call') return;
        try {
          const handler = api[unit]?.[name]?.handler;
          const result = (handler ? await handler() : undefined) ?? { ok: true };
          ws.send(JSON.stringify({ type: 'callback', id, result }));
          ws.send(
            JSON.stringify({
              type: 'callback',
              id: 'stale-id-not-in-calls',
              result: null,
            }),
          );
        } catch ({ message }) {
          const errPacket = {
            type: 'callback',
            id,
            error: { code: 400, message },
          };
          ws.send(JSON.stringify(errPacket));
        }
      });
    });
  });

  after(() => void httpServer?.close());

  it('should throw on a stale callback for an unknown id', async () => {
    const client = await WrpcClient.connect('ws://localhost:8010/');
    const promise1 = new Promise((resolve) => {
      client.once('error', (error) => {
        assert.match(error.message, /Callback stale-id-not-in-calls not found/);
        resolve(error);
      });
      client.on('error', () => {});
    });
    const promise2 = new Promise((resolve) => client.once('error', resolve));
    await client.load('test');
    const result = await client.api.test.test();
    assert.deepStrictEqual(result, { success: true });
    const error = await promise1;
    assert.match(error.message, /Callback stale-id-not-in-calls not found/);
    const error2 = await promise2;
    assert.match(error2.message, /Callback stale-id-not-in-calls not found/);
    client.close();
  });
});

describe('Client / events', () => {
  const api = {
    system: {
      introspect: { handler: async () => api },
    },
    test: {
      echo: {
        handler: async () => {
          await timers.setTimeout(10);
          return { success: true };
        },
      },
    },
  };

  let httpServer = null;

  before(async () => {
    const { httpServer: server, wsServer: mockServer } = createWsServer();
    httpServer = server;
    await listen(httpServer, 8001);
    mockServer.on('connection', (ws) => {
      const pingInterval = setInterval(() => {
        const packet = { type: 'event', name: 'test/ping', data: { ping: true } };
        ws.send(JSON.stringify(packet));
      }, 100);
      ws.on('close', () => void clearInterval(pingInterval));
      ws.on('message', async (raw) => {
        const packet = jsonParse(raw.toString()) || {};
        if (packet.type === 'call' && packet.method === 'system/introspect') {
          const introspection = { type: 'callback', id: packet.id, result: api };
          ws.send(JSON.stringify(introspection));
          return;
        }
        const { type, name, data } = packet;
        if (type !== 'event') return;
        const [unit, event] = name.split('/');
        const result = await api[unit][event].handler(data);
        ws.send(JSON.stringify({ type: 'event', name, data: result }));
      });
    });
  });

  after(() => void httpServer?.close());

  let client;

  beforeEach(async () => {
    client = await WrpcClient.connect('ws://localhost:8001/');
    await client.load('test');
  });

  afterEach(() => void client.close());

  it('should handle events from the server', async () => {
    const ping = await new Promise((resolve) => client.api.test.on('ping', resolve));
    assert.deepStrictEqual(ping, { ping: true });
  });
});

describe('Client / stream', () => {
  const storage = new Map();

  const handleBinary = (chunk) => {
    const { id, payload } = chunkDecode(chunk);
    const stream = storage.get(id);
    if (!stream) return;
    const stringChunk = Buffer.from(payload).toString('utf8');
    stream.data.push(stringChunk);
  };

  const handleOutgoingStream = async (ws, { id, name, blob }) => {
    const initPacket = { type: 'stream', id, name, size: blob.size };
    const endPacket = { type: 'stream', id, status: 'end' };
    ws.send(JSON.stringify(initPacket));
    const reader = blob.stream().getReader();
    let chunk;
    while (!(chunk = await reader.read()).done) {
      ws.sendBinary(Buffer.from(chunkEncode(id, chunk.value)));
    }
    ws.send(JSON.stringify(endPacket));
  };

  const handleIncomingStream = ({ id, name, size, status }) => {
    const stream = storage.get(id);
    if (status) {
      if (!stream) throw new Error(`Stream ${id} is not initialized`);
      if (status === 'end') stream.status = 'ended';
      if (status === 'terminate') stream.status = 'terminated';
      return;
    }
    const valid = typeof name === 'string' && Number.isSafeInteger(size);
    if (!valid) throw new Error('Stream packet structure error');
    if (stream) throw new Error(`Stream ${id} is already initialized`);
    storage.set(id, { name, size, data: [], status: 'init' });
  };

  const api = {
    system: {
      introspect: { handler: async () => api },
    },
    test: {
      getStreamData: {
        handler: async ({ id }) => storage.get(id),
      },
      download: {
        handler: async ({ name }, ws) => {
          const id = randomUUID();
          const data = 'Some random data for upload to the client';
          const blob = new Blob([data]);
          handleOutgoingStream(ws, { id, name, blob });
          return { id };
        },
      },
    },
  };

  let httpServer = null;

  before(async () => {
    const { httpServer: server, wsServer: mockServer } = createWsServer();
    httpServer = server;
    await listen(httpServer, 8002);
    mockServer.on('connection', (ws) => {
      const pingInterval = setInterval(() => {
        const packet = { type: 'event', name: 'test/ping', data: { ping: true } };
        ws.send(JSON.stringify(packet));
      }, 100);
      ws.on('close', () => void clearInterval(pingInterval));
      ws.on('message', async (raw, isBinary) => {
        if (isBinary) return void handleBinary(new Uint8Array(raw));
        const packet = jsonParse(raw.toString()) || {};
        if (packet.type === 'call' && packet.method === 'system/introspect') {
          const introspection = { type: 'callback', id: packet.id, result: api };
          return void ws.send(JSON.stringify(introspection));
        }
        if (packet.type === 'stream') return void handleIncomingStream(packet);
        const { type, id, method, args } = packet;
        // Calls only: a client also says `cancel` for a call that timed out.
        if (type !== 'call') return;
        const [unit, name] = method.split('/');
        const result = await api[unit][name].handler(args, ws);
        ws.send(JSON.stringify({ type: 'callback', id, result }));
      });
    });
  });

  after(() => void httpServer?.close());

  let client;

  beforeEach(async () => {
    client = await WrpcClient.connect('ws://localhost:8002/');
    await client.load('test');
  });

  afterEach(() => void client.close());

  it('should handle file uploads', async () => {
    const data = 'Some random data for upload to the server';
    const name = 'upload-stream';
    const blob = new Blob([data]);
    blob.name = name;
    const stream = client.createBlobUploader(blob);
    await stream.upload();
    const uploadedFile = await client.api.test.getStreamData({ id: stream.id });
    assert.strictEqual(uploadedFile.name, name);
    assert.strictEqual(uploadedFile.size, blob.size);
    assert.strictEqual(uploadedFile.data.join(''), data);
    assert.strictEqual(uploadedFile.status, 'ended');
  });

  it('should handle file downloads', async () => {
    const name = 'download-stream';
    const { id } = await client.api.test.download({ name });
    const readable = client.getStream(id);
    const blob = await readable.toBlob();
    const data = await blob.text();
    assert.strictEqual(data, 'Some random data for upload to the client');
  });
});

describe('Client / different ID generation strategies', () => {
  const api = {
    system: {
      introspect: { handler: async () => api },
    },
    test: {
      test: {
        handler: async () => {
          await timers.setTimeout(10);
          return { success: true };
        },
      },
    },
  };

  let httpServer = null;

  before(async () => {
    const { httpServer: server, wsServer: mockServer } = createWsServer();
    httpServer = server;
    await listen(httpServer, 8004);
    mockServer.on('connection', (ws) => {
      ws.on('message', async (raw) => {
        const packet = jsonParse(raw.toString()) || {};
        const { type, id, method } = packet;
        // Calls only: a client also says `cancel` for a call that timed out.
        if (type !== 'call') return;
        const [unit, name] = method.split('/');
        try {
          const result = await api[unit][name].handler();
          ws.send(JSON.stringify({ type: 'callback', id, result }));
        } catch ({ message }) {
          const reply = { type: 'callback', id, error: { code: 400, message } };
          ws.send(JSON.stringify(reply));
        }
      });
    });
  });

  after(() => void httpServer?.close());

  it('should work with UUID generation', async () => {
    const client = await WrpcClient.connect('ws://localhost:8004/');
    await client.load('test');
    const result = await client.api.test.test();
    assert.deepStrictEqual(result, { success: true });
    client.close();
  });
});
