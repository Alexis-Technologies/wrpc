'use strict';

const { Server, defineRouter, procedure } = require('../../index.js');

const router = defineRouter({
  chat: {
    join: procedure({
      access: 'public',
      handler: async (context, { room }) => {
        context.client.join(room);
        return { ok: true };
      },
    }),
    shout: procedure({
      access: 'public',
      handler: async (context, { room, text }) => {
        context.server.to(room).emit('chat/message', { text });
        return { ok: true };
      },
    }),
  },
});

const server = new Server({ router, host: '127.0.0.1', port: 8000, protocol: 'http' });

server.listen().then(() => {
  console.log('wrpc chat example listening on http://127.0.0.1:8000/api');
  console.log('run `node build.js` once, then open public/index.html in a browser');
});
