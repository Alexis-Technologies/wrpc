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

const handleRpc = async (client, packet, router) => {
  const { id, method, args } = packet;
  const [unitName, methodName] = split(method, '/');
  // Split on the FIRST dot only: 'unit.1.2/m' must look up version '1.2'
  // (a guaranteed miss -> 404), not silently truncate to version '1'
  const dot = unitName.indexOf('.');
  const unit = dot < 0 ? unitName : unitName.slice(0, dot);
  const version = dot < 0 ? DEFAULT_VERSION : unitName.slice(dot + 1);
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

const handleMessage = (client, data, router) => {
  const packet = jsonParse(data) || {};
  const { id, type, method } = packet;
  if (type === 'call' && id && method) {
    return void handleRpc(client, packet, router);
  } else if (type === 'stream' && id) {
    return void handleStream(client, packet);
  }
  const error = new Error('Packet structure error');
  client.error(500, { error });
};

module.exports = {
  handleRpc,
  handleStream,
  handleBinary,
  handleMessage,
  isError,
  split,
  parseParams,
};
