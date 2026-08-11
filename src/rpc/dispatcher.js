'use strict';

const { jsonParse } = require('../utils.js');
const { WrpcReadable } = require('../streams.js');
const { chunkDecode } = require('../chunks.js');
const { runSubscription } = require('./subscriptions.js');

const DEFAULT_VERSION = '*';

// A cancelled call answers with 499 (nginx's "client closed request"): the
// caller asked for it, so it is neither a server fault nor a success.
const CANCELLED = 499;

// A batch frame is a JSON array of packets. Unbounded, one frame could ask
// for unbounded work — the cap is what keeps a single message from being a
// denial of service.
const DEFAULT_MAX_BATCH = 128;

const isError = (err) => err?.constructor?.name?.includes('Error') || false;

const split = (s, separator) => {
  const i = s.indexOf(separator);
  if (i < 0) return [s, ''];
  return [s.slice(0, i), s.slice(i + separator.length)];
};

const parseParams = (params) => Object.fromEntries(new URLSearchParams(params));

// 'unit/name', 'unit.ver/name' -> { unit, version, name }.
// Split on the FIRST dot only: 'unit.1.2/m' must look up version '1.2'
// (a guaranteed miss -> 404), not silently truncate to version '1'
const parseTarget = (target) => {
  const [unitName, name] = split(target, '/');
  const dot = unitName.indexOf('.');
  const unit = dot < 0 ? unitName : unitName.slice(0, dot);
  const version = dot < 0 ? DEFAULT_VERSION : unitName.slice(dot + 1);
  return { unit, version, name };
};

const handleRpc = async (client, packet, router) => {
  const { id, method, args } = packet;
  const { unit, version, name: methodName } = parseTarget(method);
  const proc = router.getProcedure(unit, version, methodName);
  if (!proc) return void client.error(404, { id });
  if (client.calls.has(id)) {
    return void client.error(400, { id, error: new Error(`Call ${id} is already in flight`) });
  }
  // The controller is what `{type:'cancel'}` and a disconnect reach: the
  // handler sees it as ctx.signal, and once it is aborted the call has
  // already been answered, so nothing this returns may be sent.
  //
  // Registered BEFORE the first await: a cancel batched behind the call in
  // the same frame is dispatched in the same turn, and would otherwise find
  // nothing to cancel.
  const controller = new AbortController();
  client.calls.set(id, controller);
  try {
    await client.sessionReady;
    if (controller.signal.aborted) return;
    if (!client.session && proc.access !== 'public') {
      return void client.error(403, { id });
    }
    const context = client.createContext(controller.signal);
    const result = await proc.invoke(context, args);
    if (controller.signal.aborted) return;
    if (isError(result)) {
      const { code } = result;
      return void client.error(code, { id, error: result });
    }
    client.send({ type: 'callback', id, result }, { method });
  } catch (error) {
    if (controller.signal.aborted) return;
    let code = error.code === 'ETIMEOUT' ? 408 : 500;
    if (typeof error.code === 'number') code = error.code;
    return void client.error(code, { id, error });
  } finally {
    if (client.calls.get(id) === controller) client.calls.delete(id);
  }
};

// Cancellation is best-effort by nature: a handler that never looks at
// ctx.signal keeps running. What IS guaranteed is that the caller gets its
// 499 immediately and that whatever the handler eventually returns is
// dropped rather than delivered late.
const handleCancel = (client, packet) => {
  const { id } = packet;
  const controller = client.calls.get(id);
  if (!controller) return; // unknown or already settled: nothing to cancel
  client.calls.delete(id);
  controller.abort(new Error('Cancelled by the caller'));
  client.error(CANCELLED, { id, error: new Error('Cancelled by the caller') });
};

// A subscription's terminal packet is `end`, refusals included: answering
// with a `callback` would make the client look for a call it never made.
const refuse = (client, id, code, message) => {
  client.send({ type: 'end', id, error: { message, code } });
  client.warn(`SUBSCRIBE\t${id}\t${code}\t${message}`);
};

const handleSubscribe = async (client, packet, router) => {
  const { id, method, args, lastEventId } = packet;
  const { unit, version, name: methodName } = parseTarget(method);
  const proc = router.getProcedure(unit, version, methodName);
  if (!proc) return void refuse(client, id, 404, `${method} not found`);
  if (!proc.subscription) {
    return void refuse(client, id, 400, `${method} is not a subscription`);
  }
  // Re-subscribing an id this client already holds is a RESUME, not a
  // collision: an SSE channel outlives its stream, so a reconnecting peer
  // re-opens its subscriptions on the very client that still has them.
  // Refusing would strand the old generator and kill the new subscription.
  const previous = client.subscriptions.get(id);
  if (previous) {
    client.subscriptions.delete(id);
    previous.abort(new Error('Replaced by a re-subscribe'));
  }
  if (client.subscriptions.size >= client.maxSubscriptions) {
    return void refuse(client, id, 429, 'Too many subscriptions');
  }
  // Registered before the first await, so an unsubscribe dispatched in the
  // same turn has something to find.
  const controller = new AbortController();
  client.subscriptions.set(id, controller);
  await client.sessionReady;
  if (controller.signal.aborted) {
    client.subscriptions.delete(id);
    return;
  }
  if (!client.session && proc.access !== 'public') {
    client.subscriptions.delete(id);
    return void refuse(client, id, 403, 'Forbidden');
  }
  const context = client.createContext(controller.signal);
  const options = { id, procedure: proc, context, args, lastEventId, signal: controller.signal };
  let terminal = { type: 'end', id };
  try {
    terminal = await runSubscription(client, options);
  } catch (error) {
    const code = typeof error.code === 'number' ? error.code : 500;
    terminal = { type: 'end', id, error: { message: error.message, code } };
  } finally {
    if (client.subscriptions.get(id) === controller) client.subscriptions.delete(id);
  }
  client.send(terminal);
};

const handleUnsubscribe = (client, packet) => {
  const { id } = packet;
  const controller = client.subscriptions.get(id);
  if (!controller) return; // already finished, or never existed
  client.subscriptions.delete(id);
  // The pump notices the abort, stops pulling, and answers with `end`.
  controller.abort(new Error('Unsubscribed by the client'));
};

const handleStream = async (client, packet) => {
  const { id, name, size, status } = packet;
  const tag = `${id}/${name}`;
  try {
    // Request/response transports cannot carry streams; without this the
    // call would register a stream and never answer, hanging the request
    if (!client.persistent) throw new Error('Streams require a persistent connection');
    if (!client.binary) throw new Error('Streams require a binary transport');
    const stream = client.streams.get(id);
    if (status) {
      if (!stream) throw new Error(`Stream ${tag} is not initialized`);
      if (status === 'end') await stream.close();
      if (status === 'terminate') await stream.terminate();
      return void client.streams.delete(id);
    }
    const valid = typeof name === 'string' && Number.isSafeInteger(size);
    if (!valid) throw new Error('Stream packet structure error');
    if (stream) throw new Error(`Stream ${tag} is already initialized`);
    {
      const stream = new WrpcReadable(id, name, size);
      client.streams.set(id, stream);
    }
  } catch (error) {
    client.error(400, { id, error });
  }
};

const handleBinary = async (client, data) => {
  const { id, payload } = chunkDecode(data);
  try {
    const upstream = client.streams.get(id);
    if (upstream) {
      await upstream.push(payload);
      return;
    }
    const error = new Error(`Stream ${id} is not initialized`);
    client.error(400, { id, error });
  } catch (error) {
    client.error(400, { id, error });
  }
};

// Inbound events (client -> server) are fire-and-forget: the packet carries
// no id, so there is nothing to answer on and nothing to answer with. A
// rejected or failing event is therefore reported to the server log — the
// wire stays silent, which is what makes an event an event.
const handleEvent = async (client, packet, router) => {
  const { name: target, data } = packet;
  // Request/response transports cannot carry events. Without this the
  // handler would run and the HTTP request would never be answered — the
  // response is only written when something calls send/error — pinning the
  // socket and its server-side Client open for as long as the peer keeps
  // the connection. `handleStream` guards the same hazard.
  if (!client.persistent) {
    return void client.error(400, { error: new Error('Events require a persistent connection') });
  }
  const { unit, version, name } = parseTarget(target);
  const handler = router.getEventHandler(unit, version, name);
  if (!handler) return void client.warn(`EVENT\t${target}\tno handler`);
  await client.sessionReady;
  if (!client.session && handler.access !== 'public') {
    return void client.warn(`EVENT\t${target}\tsession required`);
  }
  try {
    const context = client.createContext();
    await handler.invoke(context, data);
  } catch (error) {
    client.warn(`EVENT\t${target}\t${error.stack ?? error.message}`);
  }
};

// Every dispatch below is fire-and-forget, so a rejection has nobody to
// reach: under node's default --unhandled-rejections=throw one would take
// the process down. The handlers answer their own errors; this is the last
// line of defence for the ones that cannot (a broken transport, a console
// that throws), and it must never throw itself.
const contain = (client, label) => (error) => {
  try {
    client.warn(`${label}\t${error?.stack ?? error}`);
  } catch {
    // Nothing left to report through.
  }
};

// Only a connection that stays open can carry a stream of values or a
// cancellation that arrives after the call: on a request/response transport
// these have to be refused, or the request is never answered.
const needsConnection = (client, id, what) => {
  if (client.persistent) return false;
  client.error(400, { id, error: new Error(`${what} requires a persistent connection`) });
  return true;
};

const handlePacket = (client, packet, router) => {
  const { id, type, method, name } = packet;
  // The target is type-checked, not just truthiness-checked: a non-string
  // `method`/`name` would throw inside parseTarget, and these calls are not
  // awaited. A malformed packet has to fall through to the error below.
  if (type === 'call' && id && typeof method === 'string') {
    return void handleRpc(client, packet, router).catch(contain(client, 'CALL'));
  } else if (type === 'subscribe' && id && typeof method === 'string') {
    if (!client.persistent) {
      return void refuse(client, id, 400, 'Subscriptions require a persistent connection');
    }
    return void handleSubscribe(client, packet, router).catch(contain(client, 'SUBSCRIBE'));
  } else if (type === 'unsubscribe' && id) {
    if (needsConnection(client, id, 'Subscriptions')) return;
    return void handleUnsubscribe(client, packet);
  } else if (type === 'cancel' && id) {
    if (needsConnection(client, id, 'Cancellation')) return;
    return void handleCancel(client, packet);
  } else if (type === 'stream' && id) {
    return void handleStream(client, packet).catch(contain(client, 'STREAM'));
  } else if (type === 'event' && typeof name === 'string' && name) {
    return void handleEvent(client, packet, router).catch(contain(client, 'EVENT'));
  } else if (type === 'ping') {
    // App-level heartbeat: a browser WebSocket cannot see protocol pings,
    // so liveness is measured with packets the client can observe.
    return void client.send({ type: 'pong' });
  } else if (type === 'pong' && client.persistent) {
    return; // answer to a server-initiated ping; liveness is the transport's
  }
  // The id travels with the refusal when the packet carried one: an HTTP
  // batch answers positionally, so an id-less error would lose this slot and
  // shift every answer after it.
  const error = new Error('Packet structure error');
  client.error(500, { id: typeof id === 'string' ? id : '', error });
};

// A JSON array is a batch frame: several packets in one message, each
// answered on its own (on a request/response transport the answers come
// back as an array in the same order — see ServerHttpTransport).
const handleMessage = (client, data, router, options = {}) => {
  const packet = jsonParse(data) || {};
  if (!Array.isArray(packet)) return void handlePacket(client, packet, router);
  const { maxBatch = DEFAULT_MAX_BATCH } = options;
  if (packet.length === 0 || packet.length > maxBatch) {
    const error = new Error(`Batch size must be between 1 and ${maxBatch}`);
    return void client.error(400, { error });
  }
  for (const item of packet) {
    handlePacket(client, item && typeof item === 'object' ? item : {}, router);
  }
};

module.exports = {
  handleRpc,
  handleStream,
  handleBinary,
  handleEvent,
  handleSubscribe,
  handleUnsubscribe,
  handleCancel,
  handlePacket,
  handleMessage,
  isError,
  split,
  parseTarget,
  parseParams,
  DEFAULT_MAX_BATCH,
  CANCELLED,
};
