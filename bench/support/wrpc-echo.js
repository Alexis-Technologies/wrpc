'use strict';

const { randomUUID } = require('node:crypto');
const { Server } = require('../../src/server.js');

const noop = () => {};

class ProcedureMock {
  constructor({ access = 'public', handler }) {
    this.access = access;
    this.handler = handler;
  }

  // eslint-disable-next-line class-methods-use-this
  async enter() {}
  // eslint-disable-next-line class-methods-use-this
  leave() {}
  invoke(context, args) {
    return this.handler(args, context);
  }
}

const createApplication = (api) => {
  const introspect = (units = []) => {
    const result = {};
    for (const unit of units) {
      if (!api[unit]) continue;
      const methods = {};
      for (const name of Object.keys(api[unit])) methods[name] = {};
      result[unit] = methods;
    }
    return result;
  };

  return {
    console: { log: noop, info: noop, warn: noop, error: noop, debug: noop },
    static: { constructor: { name: 'Static' } },
    auth: { saveSession: async () => {} },
    getMethod: (unit, _ver, method) => {
      if (unit === 'system' && method === 'introspect') {
        return new ProcedureMock({ handler: async (units) => introspect(units) });
      }
      const def = api[unit]?.[method];
      if (!def) return null;
      return new ProcedureMock(def);
    },
  };
};

const createWrpcServer = async (api) => {
  const options = {
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    timeouts: { bind: 100 },
    queue: { concurrency: 100, size: 100, timeout: 5_000 },
    generateId: randomUUID,
  };
  const server = new Server(createApplication(api), options);
  await server.listen();
  const { port } = server.httpServer.address();
  return { server, port };
};

module.exports = { createWrpcServer };
