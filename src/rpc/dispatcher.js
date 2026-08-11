'use strict';

const { jsonParse } = require('../utils.js');
const { WrpcReadable } = require('../streams.js');
const { chunkDecode } = require('../chunks.js');

const DEFAULT_VERSION = '*';

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
  await client.sessionReady;
  if (!client.session && proc.access !== 'public') {
    return void client.error(403, { id });
  }
  try {
    const context = client.createContext();
    const result = await proc.invoke(context, args);
    if (isError(result)) {
      const { code } = result;
      return void client.error(code, { id, error: result });
    }
    client.send({ type: 'callback', id, result }, { method });
  } catch (error) {
    let code = error.code === 'ETIMEOUT' ? 408 : 500;
    if (typeof error.code === 'number') code = error.code;
    return void client.error(code, { id, error });
  }
};

const handleStream = async (client, packet) => {
  const { id, name, size, status } = packet;
  const tag = `${id}/${name}`;
  try {
    // Request/response transports cannot carry streams; without this the
    // call would register a stream and never answer, hanging the request
    if (!client.persistent) throw new Error('Streams require a persistent connection');
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

const handleMessage = (client, data, router) => {
  const packet = jsonParse(data) || {};
  const { id, type, method, name } = packet;
  // The target is type-checked, not just truthiness-checked: a non-string
  // `method`/`name` would throw inside parseTarget, and these calls are not
  // awaited. A malformed packet has to fall through to the error below.
  if (type === 'call' && id && typeof method === 'string') {
    return void handleRpc(client, packet, router).catch(contain(client, 'CALL'));
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
  const error = new Error('Packet structure error');
  client.error(500, { error });
};

module.exports = {
  handleRpc,
  handleStream,
  handleBinary,
  handleEvent,
  handleMessage,
  isError,
  split,
  parseTarget,
  parseParams,
};
