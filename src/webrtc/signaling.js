'use strict';

// The built-in signaling unit: what a wrpc server adds to its router so its
// WS clients can find each other and exchange WebRTC descriptions and
// candidates through it. One unit, four methods and one inbound event:
//
//   whoami({ id?, instance? })  -> { id }                      identify: the caller's peer id
//   join({ room, data })        -> { id, room, members }       join a roster room
//   leave({ room })             -> { room, left }
//   members({ room })           -> [{ id, instance, address, data }]   the roster, caller excluded
//   on.signal { to, address?, room, message }    relayed to `to` as `<unit>/signal`
//   emits: signal { from, instance, address, to, toInstance, room, message }
//          join   { room, id, instance, address, data }
//          leave  { room, id, instance, address, reason: 'left' | 'disconnect' | 'replaced' }
//          replaced { id }      this connection lost its peer id to a newer one
//   with `assertions` configured, two more:
//   assert({ fingerprint })     -> { assertion, iat, exp }   a signed token: this peer id, this certificate
//   keys()                      -> { keys: [JWK] }           the public keys (access 'public')
//
// Identity. A peer's id is what the `identity` strategy says: by default the
// signaling connection's client id (instance-prefixed, server-issued —
// nobody can claim another's), or whatever the application derives from
// the context (a user id from the session, say), with the id the client
// proposed in whoami as one input. The id is stable across signaling
// reconnects when the strategy is; what tells two incarnations of the same
// id apart (a new tab, a replaced one) is `instance`, a discriminator the
// client generates once per signaler and the server merely carries.
//
// Addressing. RpcServer.sendTo routes by the instance prefix of a CLIENT id
// (an app-level id would route nowhere), so every roster member, join and
// signal carries `address` — the routable client id — and a relayed signal
// is delivered to the `address` the sender learned from the roster, with a
// cold by-id resolution (the local registry, then the room's descriptors)
// only for a connect() by name. Rooms live under `prefix` in the room
// registry, so a signaling room never collides with an application room,
// and every relay is bounded by a membership: `signal` reaches `to` only
// while both peers are in the room.
//
// Everything here is a plain router unit over public core seams — Client
// rooms, Broadcast, RpcServer.sendTo — so it clusters for free: the roster
// is cluster.fetchClients (which answers locally, at once, without a
// backplane), and the per-connection state travels as client.data.rtc.

const { procedure } = require('../rpc/router.js');
const { isSignalMessage, SIGNAL_MESSAGE_TYPES } = require('./signaler.js');
const { normalizeFingerprint } = require('./assertions.js');
const { createAssertionIssuer } = require('./assertionIssuer.js');

const DEFAULT_NAME = 'signaling';
const DEFAULT_PREFIX = 'rtc:';
const MAX_ROOM_LENGTH = 256;
const MAX_ID_LENGTH = 256;
const DUPLICATE = ['replace', 'refuse'];

const refusal = (message, code) => Object.assign(new Error(message), { code });

// Rooms are peer-named. Bounded, non-empty, and never a string that could
// address a prototype when used as an object key downstream.
const checkRoom = (room) => {
  if (typeof room !== 'string' || room.length === 0 || room.length > MAX_ROOM_LENGTH) {
    throw new TypeError(`room must be a non-empty string of at most ${MAX_ROOM_LENGTH} characters`);
  }
  return room;
};

// Ids and instances become Map keys, session tokens, log fields and URLs
// on every peer: bounded the same way.
const isId = (value) => typeof value === 'string' && value.length > 0 && value.length <= MAX_ID_LENGTH;

const checkId = (value, what) => {
  if (!isId(value)) throw new TypeError(`${what} must be a non-empty string of at most ${MAX_ID_LENGTH} characters`);
  return value;
};

const optionalId = (value, what) => (value === undefined || value === null ? null : checkId(value, what));

// The per-connection signaling state lives on client.data.rtc — a
// null-prototype record — so it travels inside the cluster's client
// descriptors (client.data is what fetchClients replicates) and the roster
// can be assembled from descriptors alone, local or remote.
//   { id, instance, since, revoked?, rooms: { [room]: joinData } }
const rtcOf = (client) => {
  const rtc = client.data?.rtc;
  return typeof rtc === 'object' && rtc !== null && typeof rtc.id === 'string' ? rtc : null;
};

const dataFor = (rtc, room) => {
  const rooms = rtc?.rooms;
  if (typeof rooms !== 'object' || rooms === null || !Object.hasOwn(rooms, room)) return null;
  return rooms[room];
};

const inputRoom = (args) => {
  if (typeof args !== 'object' || args === null) throw new TypeError('expected { room }');
  return { room: checkRoom(args.room) };
};

const inputJoin = (args) => {
  const { room } = inputRoom(args);
  return { room, data: args.data === undefined ? null : args.data };
};

const inputWhoami = (args) => {
  if (args === undefined || args === null) return { proposed: null, instance: null };
  if (typeof args !== 'object') throw new TypeError('expected { id?, instance? }');
  return { proposed: optionalId(args.id, 'id'), instance: optionalId(args.instance, 'instance') };
};

const inputAssert = (args) => {
  if (typeof args !== 'object' || args === null) throw new TypeError('expected { fingerprint }');
  const fingerprint = normalizeFingerprint(args.fingerprint);
  if (fingerprint === null) throw new TypeError("fingerprint must look like 'sha-256 AB:CD:...'");
  return { fingerprint };
};

const inputSignal = (args) => {
  if (typeof args !== 'object' || args === null) throw new TypeError('expected { to, room, message }');
  const { to, message } = args;
  checkId(to, 'to');
  if (!isSignalMessage(message)) throw new TypeError(`message.type must be one of ${SIGNAL_MESSAGE_TYPES.join(', ')}`);
  const room = args.room === undefined || args.room === null ? null : checkRoom(args.room);
  return { to, address: optionalId(args.address, 'address'), room, message };
};

/**
 * The signaling unit as a router definition fragment: spread it into
 * defineRouter's units, `{ ...createSignalingUnit() }`. Options:
 *   name      unit name (default 'signaling'); the client helper must agree
 *   access    'session' (default) or 'public' — applied to every method
 *   identity  async (context, { proposed }) -> string — the peer id of a
 *             connection; default: its client id, the proposal ignored
 *   duplicate 'replace' (default): a second connection identifying as an id
 *             this instance already holds takes it over, the first is told
 *             `replaced`; 'refuse': the second is answered 409. Node-local:
 *             a cluster-wide claim belongs in the identity strategy.
 *   authorize async (context, { action, room, data?, to?, message? }) — a
 *             hook run before join and before every relayed signal; return
 *             false to refuse with 403, or throw a coded error of your own
 *   relay     'room' (default): a signal reaches `to` only while sender and
 *             receiver share the room; 'any': any connected client by id
 *   prefix    the room-registry namespace (default 'rtc:')
 *   assertions { key, kid?, ttl?, issuer?, claims? } — issue trust
 *             assertions (see assertions.js): `key` a private ES256 JWK or
 *             CryptoKey pair, `claims(context)` extra claims to sign in
 *             (roles, say). Adds `assert` and the public `keys` method.
 */
const createSignalingUnit = (options = {}) => {
  const {
    name = DEFAULT_NAME,
    access = 'session',
    identity = null,
    duplicate = 'replace',
    authorize = null,
    relay = 'room',
    prefix = DEFAULT_PREFIX,
    assertions = null,
  } = options;
  if (typeof name !== 'string' || name.length === 0 || name.includes('/') || name.includes('.')) {
    throw new TypeError('createSignalingUnit: name must be a unit name without "/" or "."');
  }
  if (access !== 'session' && access !== 'public') {
    throw new TypeError("createSignalingUnit: access must be 'session' or 'public'");
  }
  if (identity !== null && typeof identity !== 'function') {
    throw new TypeError('createSignalingUnit: identity must be a function');
  }
  if (!DUPLICATE.includes(duplicate)) {
    throw new TypeError(`createSignalingUnit: duplicate must be one of ${DUPLICATE.join(', ')}`);
  }
  if (authorize !== null && typeof authorize !== 'function') {
    throw new TypeError('createSignalingUnit: authorize must be a function');
  }
  if (relay !== 'room' && relay !== 'any') throw new TypeError("createSignalingUnit: relay must be 'room' or 'any'");
  if (typeof prefix !== 'string') throw new TypeError('createSignalingUnit: prefix must be a string');
  if (assertions !== null && (typeof assertions !== 'object' || Array.isArray(assertions))) {
    throw new TypeError('createSignalingUnit: assertions must be an object');
  }
  if (assertions !== null && assertions.claims !== undefined && typeof assertions.claims !== 'function') {
    throw new TypeError('createSignalingUnit: assertions.claims must be a function');
  }
  const issuer =
    assertions === null
      ? null
      : createAssertionIssuer({
          key: assertions.key,
          kid: assertions.kid,
          ttl: assertions.ttl,
          issuer: assertions.issuer,
        });
  const extraClaims = assertions?.claims ?? null;

  const roomOf = (room) => prefix + room;
  const event = (kind) => `${name}/${kind}`;

  // The peers identified on THIS instance, by peer id. Cleared when the
  // connection closes, unless the id was taken over meanwhile.
  const registry = new Map();

  const allow = async (context, info) => {
    if (authorize === null) return;
    const verdict = await authorize(context, info);
    if (verdict === false) throw refusal(`${name}: ${info.action} refused`, 403);
  };

  // Re-checked after every await: a call from a connection that lost its
  // id while the call was in flight must not act under it.
  const owns = (client, rtc) => {
    if (rtc.revoked === true || registry.get(rtc.id) !== client) throw refusal(`${name}: replaced`, 409);
  };

  const member = (rtc, address, room) => ({ id: rtc.id, instance: rtc.instance, address, data: dataFor(rtc, room) });

  // Takes `id` away from `holder` for a newer connection. The same
  // incarnation coming back (a reconnect whose old socket is not yet dead)
  // leaves silently — its id is still present; a different one is
  // announced as a `replaced` leave, so the other peers drop the links they
  // hold to the old incarnation.
  const replace = (holder, server, incoming) => {
    const rtc = rtcOf(holder);
    rtc.revoked = true;
    const same = rtc.instance !== null && rtc.instance === incoming;
    for (const full of holder.rooms) {
      if (!full.startsWith(prefix)) continue;
      holder.leave(full);
      if (same) continue;
      server.to(full).emit(event('leave'), {
        room: full.slice(prefix.length),
        id: rtc.id,
        instance: rtc.instance,
        address: holder.id,
        reason: 'replaced',
      });
    }
    rtc.rooms = Object.create(null);
    holder.sendEvent(event('replaced'), { id: rtc.id });
  };

  // One identification per connection: whoami does it explicitly, join and
  // signal lazily. Idempotent for the same id; a different proposal is 409.
  const identify = async (context, proposed = null, instance = null) => {
    const { client, server } = context;
    const current = rtcOf(client);
    if (current) {
      if (current.revoked === true) throw refusal(`${name}: replaced`, 409);
      if (proposed !== null && proposed !== current.id) throw refusal(`${name}: already identified`, 409);
      return current;
    }
    const id = identity === null ? client.id : await identity(context, { proposed });
    if (!isId(id)) throw refusal(`${name}: identity must return a non-empty string`, 500);
    // Two whoami calls in flight on one connection: the second to land
    // adopts the first's answer.
    const raced = rtcOf(client);
    if (raced) {
      if (raced.id !== id) throw refusal(`${name}: already identified`, 409);
      return raced;
    }
    const holder = registry.get(id);
    if (holder && holder !== client) {
      if (duplicate === 'refuse') throw refusal(`${name}: '${id}' is already connected`, 409);
      replace(holder, server, instance);
    }
    const rtc = { __proto__: null, id, instance, since: Date.now(), rooms: Object.create(null) };
    client.data.rtc = rtc;
    registry.set(id, client);
    client.once('close', () => {
      if (registry.get(id) === client) registry.delete(id);
    });
    return rtc;
  };

  // The roster of `room` minus the asking connection, from cluster
  // descriptors: one code path whether or not a backplane exists. Two
  // descriptors with one peer id (a duplicate on another node) collapse to
  // the newest.
  const roster = async (server, room, self) => {
    const descriptors = await server.cluster.fetchClients({ room: roomOf(room) });
    const members = new Map();
    for (let i = 0; i < descriptors.length; i++) {
      const descriptor = descriptors[i];
      if (descriptor.id === self.id) continue;
      const rtc = rtcOf(descriptor);
      if (rtc?.revoked === true) continue;
      const id = rtc ? rtc.id : descriptor.id;
      if (id === self.peer) continue;
      const since = rtc ? rtc.since : 0;
      const known = members.get(id);
      if (known && known.since >= since) continue;
      members.set(id, {
        since,
        member: { id, instance: rtc ? rtc.instance : null, address: descriptor.id, data: dataFor(rtc, room) },
      });
    }
    const list = [];
    for (const entry of members.values()) list.push(entry.member);
    return list;
  };

  // Where a signal to peer `to` goes: the local holder of the id, else the
  // `address` the sender learned from the roster (sendTo routes it by its
  // instance prefix), else — room relay only — the room's descriptors.
  const resolve = async (server, to, address, room) => {
    const local = registry.get(to);
    if (local) return { address: local.id, instance: rtcOf(local).instance };
    if (address !== null && !server.getClient(address)) return { address, instance: null };
    if (relay !== 'room') return null;
    const descriptors = await server.cluster.fetchClients({ room: roomOf(room) });
    let found = null;
    for (let i = 0; i < descriptors.length; i++) {
      const rtc = rtcOf(descriptors[i]);
      if (!rtc || rtc.revoked === true || rtc.id !== to) continue;
      if (!found || rtc.since > found.since) {
        found = { since: rtc.since, address: descriptors[i].id, instance: rtc.instance };
      }
    }
    return found;
  };

  const unit = {
    whoami: procedure({
      access,
      input: inputWhoami,
      signature: { args: { id: 'string', instance: 'string' }, returns: { id: 'string' } },
      handler: async (context, { proposed, instance }) => {
        const rtc = await identify(context, proposed, instance);
        return { id: rtc.id };
      },
    }),

    join: procedure({
      access,
      input: inputJoin,
      signature: {
        args: { room: 'string', data: 'unknown' },
        returns: { id: 'string', room: 'string', members: 'object[]' },
      },
      handler: async (context, { room, data }) => {
        const { client, server } = context;
        const rtc = await identify(context);
        await allow(context, { action: 'join', room, data });
        owns(client, rtc);
        rtc.rooms[room] = data;
        // Join, announce, then read the roster: a peer joining at the same
        // moment is in the roster or hears the announcement (or, across a
        // backplane, both — the mesh treats a repeated join as one).
        const fresh = client.join(roomOf(room));
        if (fresh) {
          const announce = { room, ...member(rtc, client.id, room) };
          server.to(roomOf(room)).except(client).emit(event('join'), announce);
        }
        const members = await roster(server, room, { id: client.id, peer: rtc.id });
        return { id: rtc.id, room, members };
      },
    }),

    leave: procedure({
      access,
      input: inputRoom,
      signature: { args: { room: 'string' }, returns: { room: 'string', left: 'boolean' } },
      handler: async (context, { room }) => {
        const { client, server } = context;
        const rtc = rtcOf(client);
        if (rtc) delete rtc.rooms[room];
        const left = client.leave(roomOf(room));
        if (left && rtc?.revoked !== true) {
          server.to(roomOf(room)).emit(event('leave'), {
            room,
            id: rtc ? rtc.id : client.id,
            instance: rtc ? rtc.instance : null,
            address: client.id,
            reason: 'left',
          });
        }
        return { room, left };
      },
    }),

    members: procedure({
      access,
      input: inputRoom,
      signature: { args: { room: 'string' }, returns: 'object[]' },
      handler: async (context, { room }) => {
        const { client } = context;
        const rtc = rtcOf(client);
        return roster(context.server, room, { id: client.id, peer: rtc ? rtc.id : client.id });
      },
    }),

    on: {
      signal: procedure({
        access,
        input: inputSignal,
        signature: { args: { to: 'string', address: 'string', room: 'string', message: 'object' } },
        handler: async (context, { to, address, room, message }) => {
          const { client, server } = context;
          const rtc = await identify(context);
          if (relay === 'room') {
            if (room === null) throw refusal('signal: room is required', 400);
            if (!client.in(roomOf(room))) throw refusal(`signal: not a member of '${room}'`, 403);
          }
          await allow(context, { action: 'signal', room, to, message });
          owns(client, rtc);
          const target = await resolve(server, to, address, room);
          owns(client, rtc);
          const bounded = relay === 'room' ? { room: roomOf(room) } : {};
          const payload = {
            from: rtc.id,
            instance: rtc.instance,
            address: client.id,
            to,
            toInstance: target ? target.instance : null,
            room,
            message,
          };
          const delivered = target !== null && server.sendTo(target.address, event('signal'), payload, bounded);
          if (!delivered) client.log.warn({ event: 'signaling.undeliverable', to, room, type: message.type });
        },
      }),
    },

    ...(issuer === null
      ? {}
      : {
          assert: procedure({
            access,
            input: inputAssert,
            signature: {
              args: { fingerprint: 'string' },
              returns: { assertion: 'string', iat: 'number', exp: 'number' },
            },
            handler: async (context, { fingerprint }) => {
              const { client } = context;
              // In order, not Promise.all: identify() may be the connection's
              // FIRST identification (a client that calls assert before
              // whoami), and the claims hook reads the result from
              // context.client.data.rtc. Once identified, identify() settles
              // on a microtask, so running them together would save nothing
              // and would run the hook for a connection about to be refused.
              const rtc = await identify(context);
              const custom = extraClaims === null ? null : await extraClaims(context);
              owns(client, rtc);
              if (custom !== null && (typeof custom !== 'object' || Array.isArray(custom))) {
                throw refusal(`${name}: assertions.claims must return an object`, 500);
              }
              return issuer.sign({ ...custom, sub: rtc.id, fp: fingerprint });
            },
          }),
          // Public on purpose: a peer fetches the keys before it has a
          // session of its own, and they are public keys.
          keys: procedure({
            access: 'public',
            signature: { returns: { keys: 'object[]' } },
            handler: async () => ({ keys: await issuer.publicKeys() }),
          }),
        }),

    emits: {
      signal: {
        data: {
          from: 'string',
          instance: 'string',
          address: 'string',
          to: 'string',
          toInstance: 'string',
          room: 'string',
          message: 'object',
        },
      },
      join: { data: { room: 'string', id: 'string', instance: 'string', address: 'string', data: 'unknown' } },
      leave: { data: { room: 'string', id: 'string', instance: 'string', address: 'string', reason: 'string' } },
      replaced: { data: { id: 'string' } },
    },
  };

  return { [name]: unit };
};

/**
 * The router-level connection hooks that make a dropped signaling
 * connection LEAVE its rooms in front of the other members — unless its id
 * was taken over by a newer connection meanwhile, in which case the peer is
 * still there and nothing is announced. Compose into defineRouter(units,
 * { hooks }): `hooks: createSignalingHooks()`, or append `.onDisconnect` to
 * your own list. `name`/`prefix` must match the unit's.
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
      const rtc = rtcOf(client);
      if (rtc?.revoked === true) return;
      const id = rtc ? rtc.id : client.id;
      const instance = rtc ? rtc.instance : null;
      for (const full of rooms) {
        if (typeof full !== 'string' || !full.startsWith(prefix)) continue;
        client.server.to(full).emit(leave, {
          room: full.slice(prefix.length),
          id,
          instance,
          address: client.id,
          reason: 'disconnect',
        });
      }
    },
  };
};

module.exports = {
  createSignalingUnit,
  createSignalingHooks,
  DEFAULT_SIGNALING_NAME: DEFAULT_NAME,
  DEFAULT_SIGNALING_PREFIX: DEFAULT_PREFIX,
  MAX_ID_LENGTH,
};
