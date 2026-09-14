'use strict';

// The built-in signaling unit: what a wrpc server adds to its router so its
// WS clients can find each other and exchange WebRTC descriptions and
// candidates through it. One unit, four methods and one inbound event:
//
//   whoami()                 -> { id }                   the caller's peer id
//   join({ room, data })     -> { id, room, members }    join a roster room
//   leave({ room })          -> { room, left }
//   members({ room })        -> [{ id, data }]           the roster, caller excluded
//   on.signal { to, room, message }                     relayed to `to` as `<unit>/signal`
//
// A peer's identity IS its signaling connection's client id (instance-
// prefixed, server-issued: nobody can claim another's) — the plan's decided
// trade-off. Stable application identity travels as the join `data`, which
// rides the roster and the join notification. Rooms live under `prefix` in
// the server's room registry, so a signaling room never collides with an
// application room of the same name, and every relay is bounded by a
// membership: `signal` reaches `to` only while both peers are in the room.
//
// Everything here is a plain router unit over public core seams — Client
// rooms, Broadcast, RpcServer.sendTo — so it clusters for free: sendTo
// addresses the instance a client id names, Broadcast rides the backplane,
// and the roster is cluster.fetchClients (which answers locally, at once,
// when there is no backplane).

const { procedure } = require('../rpc/router.js');
const { isSignalMessage, SIGNAL_MESSAGE_TYPES } = require('./signaler.js');

const DEFAULT_NAME = 'signaling';
const DEFAULT_PREFIX = 'rtc:';
const MAX_ROOM_LENGTH = 256;

const refusal = (message, code) => Object.assign(new Error(message), { code });

// Rooms are peer-named. Bounded, non-empty, and never a string that could
// address a prototype when used as an object key downstream.
const checkRoom = (room) => {
  if (typeof room !== 'string' || room.length === 0 || room.length > MAX_ROOM_LENGTH) {
    throw new TypeError(`room must be a non-empty string of at most ${MAX_ROOM_LENGTH} characters`);
  }
  return room;
};

// The per-room join data lives on client.data.rtc — a null-prototype map
// keyed by the UNPREFIXED room — so it travels inside the cluster's client
// descriptors (client.data is what fetchClients replicates) and the roster
// can be assembled from descriptors alone, local or remote.
const rtcData = (client) => (client.data.rtc ??= Object.create(null));

const dataFor = (descriptor, room) => {
  const rtc = descriptor.data?.rtc;
  if (typeof rtc !== 'object' || rtc === null || !Object.hasOwn(rtc, room)) return null;
  return rtc[room];
};

const inputRoom = (args) => {
  if (typeof args !== 'object' || args === null) throw new TypeError('expected { room }');
  return { room: checkRoom(args.room) };
};

const inputJoin = (args) => {
  const { room } = inputRoom(args);
  return { room, data: args.data === undefined ? null : args.data };
};

const inputSignal = (args) => {
  if (typeof args !== 'object' || args === null) throw new TypeError('expected { to, room, message }');
  const { to, message } = args;
  if (typeof to !== 'string' || to.length === 0) throw new TypeError('to must be a peer id');
  if (!isSignalMessage(message)) throw new TypeError(`message.type must be one of ${SIGNAL_MESSAGE_TYPES.join(', ')}`);
  const room = args.room === undefined || args.room === null ? null : checkRoom(args.room);
  return { to, room, message };
};

/**
 * The signaling unit as a router definition fragment: spread it into
 * defineRouter's units, `{ ...createSignalingUnit() }`. Options:
 *   name      unit name (default 'signaling'); the client helper must agree
 *   access    'session' (default) or 'public' — applied to every method
 *   authorize async (context, { action, room, data?, to?, message? }) — a
 *             hook run before join and before every relayed signal; return
 *             false to refuse with 403, or throw a coded error of your own
 *   relay     'room' (default): a signal reaches `to` only while sender and
 *             receiver share the room; 'any': any connected client by id
 *   prefix    the room-registry namespace (default 'rtc:')
 */
const createSignalingUnit = (options = {}) => {
  const {
    name = DEFAULT_NAME,
    access = 'session',
    authorize = null,
    relay = 'room',
    prefix = DEFAULT_PREFIX,
  } = options;
  if (typeof name !== 'string' || name.length === 0 || name.includes('/') || name.includes('.')) {
    throw new TypeError('createSignalingUnit: name must be a unit name without "/" or "."');
  }
  if (access !== 'session' && access !== 'public') {
    throw new TypeError("createSignalingUnit: access must be 'session' or 'public'");
  }
  if (authorize !== null && typeof authorize !== 'function') {
    throw new TypeError('createSignalingUnit: authorize must be a function');
  }
  if (relay !== 'room' && relay !== 'any') throw new TypeError("createSignalingUnit: relay must be 'room' or 'any'");
  if (typeof prefix !== 'string') throw new TypeError('createSignalingUnit: prefix must be a string');

  const roomOf = (room) => prefix + room;
  const event = (kind) => `${name}/${kind}`;

  const allow = async (context, info) => {
    if (authorize === null) return;
    const verdict = await authorize(context, info);
    if (verdict === false) throw refusal(`${name}: ${info.action} refused`, 403);
  };

  // The roster of `room` minus the asking client, from cluster descriptors:
  // one code path whether or not a backplane exists.
  const roster = async (server, room, selfId) => {
    const descriptors = await server.cluster.fetchClients({ room: roomOf(room) });
    const members = [];
    for (let i = 0; i < descriptors.length; i++) {
      const descriptor = descriptors[i];
      if (descriptor.id === selfId) continue;
      members.push({ id: descriptor.id, data: dataFor(descriptor, room) });
    }
    return members;
  };

  const unit = {
    whoami: procedure({
      access,
      signature: { returns: { id: 'string' } },
      handler: async (context) => ({ id: context.client.id }),
    }),

    join: procedure({
      access,
      input: inputJoin,
      signature: {
        args: { room: 'string', data: 'unknown' },
        returns: { id: 'string', room: 'string', members: 'object[]' },
      },
      handler: async (context, { room, data }) => {
        await allow(context, { action: 'join', room, data });
        const { client, server } = context;
        rtcData(client)[room] = data;
        // Join, announce, then read the roster: a peer joining at the same
        // moment is in the roster or hears the announcement (or, across a
        // backplane, both — the mesh treats a repeated join as one).
        const fresh = client.join(roomOf(room));
        if (fresh) server.to(roomOf(room)).except(client).emit(event('join'), { room, id: client.id, data });
        const members = await roster(server, room, client.id);
        return { id: client.id, room, members };
      },
    }),

    leave: procedure({
      access,
      input: inputRoom,
      signature: { args: { room: 'string' }, returns: { room: 'string', left: 'boolean' } },
      handler: async (context, { room }) => {
        const { client, server } = context;
        delete rtcData(client)[room];
        const left = client.leave(roomOf(room));
        if (left) server.to(roomOf(room)).emit(event('leave'), { room, id: client.id });
        return { room, left };
      },
    }),

    members: procedure({
      access,
      input: inputRoom,
      signature: { args: { room: 'string' }, returns: 'object[]' },
      handler: async (context, { room }) => roster(context.server, room, context.client.id),
    }),

    on: {
      signal: procedure({
        access,
        input: inputSignal,
        signature: { args: { to: 'string', room: 'string', message: 'object' } },
        handler: async (context, { to, room, message }) => {
          const { client, server } = context;
          if (relay === 'room') {
            if (room === null) throw refusal('signal: room is required', 400);
            if (!client.in(roomOf(room))) throw refusal(`signal: not a member of '${room}'`, 403);
          }
          await allow(context, { action: 'signal', room, to, message });
          const bounded = relay === 'room' ? { room: roomOf(room) } : {};
          const delivered = server.sendTo(to, event('signal'), { from: client.id, room, message }, bounded);
          if (!delivered) client.log.warn({ event: 'signaling.undeliverable', to, room, type: message.type });
        },
      }),
    },

    emits: {
      signal: { data: { from: 'string', room: 'string', message: 'object' } },
      join: { data: { room: 'string', id: 'string', data: 'unknown' } },
      leave: { data: { room: 'string', id: 'string' } },
    },
  };

  return { [name]: unit };
};

/**
 * The router-level connection hooks that make a dropped signaling
 * connection LEAVE its rooms in front of the other members. Compose into
 * defineRouter(units, { hooks }): `hooks: createSignalingHooks()`, or
 * append `.onDisconnect` to your own list. `name`/`prefix` must match the
 * unit's.
 */
const createSignalingHooks = (options = {}) => {
  const { name = DEFAULT_NAME, prefix = DEFAULT_PREFIX } = options;
  if (typeof name !== 'string' || name.length === 0) throw new TypeError('createSignalingHooks: name must be a string');
  if (typeof prefix !== 'string') throw new TypeError('createSignalingHooks: prefix must be a string');
  const leave = `${name}/leave`;
  return {
    onDisconnect: (client, payload) => {
      const rooms = payload?.rooms;
      if (!rooms || typeof rooms[Symbol.iterator] !== 'function') return;
      for (const full of rooms) {
        if (typeof full !== 'string' || !full.startsWith(prefix)) continue;
        client.server.to(full).emit(leave, { room: full.slice(prefix.length), id: client.id });
      }
    },
  };
};

module.exports = {
  createSignalingUnit,
  createSignalingHooks,
  DEFAULT_SIGNALING_NAME: DEFAULT_NAME,
  DEFAULT_SIGNALING_PREFIX: DEFAULT_PREFIX,
};
