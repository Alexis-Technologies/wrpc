/**
 * `Broadcast.emit` over a room whose clients do not all speak the same
 * protocol revision (docs/reference/protocol.md#versioning).
 *
 * An event holding bytes is ONE attachments frame for the whole fan-out —
 * that is the single-encode property the broadcast path has always had. A
 * 1.0 client negotiated revision 1 and reads no frame, so in a mixed room
 * the same event also exists as the JSON 1.0 sent: built on the first
 * revision-1 recipient and shared by the rest (the `plain` twin in
 * src/rpc/rooms.js). The rows below are what that costs:
 *
 *   - a text event must cost the same whatever the room is made of — the
 *     twin is only ever looked for when the packet holds bytes;
 *   - a bytes event to an all-2.x room is the baseline (one frame);
 *   - a mixed room pays ONE extra JSON.stringify per emit, not one per
 *     revision-1 member — compare it with the all-1.0 row, which is that
 *     stringify alone.
 *
 * Measured when the negotiation landed (Node 24, 100 members): a text event
 * 686K ops/s before and after; a 1 KB bytes event to an all-2.x room 287K
 * before, 297K after; a half-1.0 room 21.0K against 21.4K for an all-1.0
 * one — the whole cost is the stringify of a typed array, which is what 1.0
 * paid on every event, and it is paid once.
 *
 * Run with `node bench/broadcast-revisions.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { RpcServer } = require('../src/rpc/core.js');
const { defineRouter, procedure } = require('../src/rpc/router.js');

const MEMBERS = 100;

// Enough of the WrpcSocket shape for attachSocket; `send` is where the wire
// would be, so what is measured is the fan-out, not a socket.
let sink = 0;
const fakeSocket = (protocol) => ({
  protocol,
  on() {},
  once() {},
  off() {},
  send(data) {
    sink += data.length;
  },
  close() {},
  terminate() {},
});

const room = (protocols) => {
  const router = defineRouter({ unit: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });
  const rpc = new RpcServer({ router, logger: false, sse: false });
  for (let i = 0; i < MEMBERS; i++) {
    const client = rpc.attachSocket(fakeSocket(protocols[i % protocols.length]), { headers: {} });
    client.join('room');
  }
  return rpc;
};

const TEXT = { user: 'ada', text: 'a chat line of ordinary length', at: 1790000000000 };
const BYTES = { name: 'thumb.png', body: new Uint8Array(1024).fill(7) };

const main = async () => {
  console.log(`Broadcast over mixed revisions — Node ${process.version}, ${MEMBERS} members\n`);

  const modern = room(['wrpc.v2']);
  const mixed = room(['wrpc.v2', 'wrpc.v1']);
  const old = room(['wrpc.v1']);

  await bench('text event — all revision 2', () => modern.to('room').emit('chat/message', TEXT));
  await bench('text event — half revision 1', () => mixed.to('room').emit('chat/message', TEXT));
  await bench('1 KB bytes event — all revision 2 (one frame)', () => modern.to('room').emit('files/ready', BYTES));
  await bench('1 KB bytes event — half revision 1 (frame + JSON)', () => mixed.to('room').emit('files/ready', BYTES));
  await bench('1 KB bytes event — all revision 1 (JSON)', () => old.to('room').emit('files/ready', BYTES));

  await Promise.all([modern.close(), mixed.close(), old.close()]);
  if (sink < 0) console.log(sink);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
