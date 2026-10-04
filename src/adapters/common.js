'use strict';

const { destroy: destroyStream } = require('node:stream');

const { WRPC_V1 } = require('../wire.js');

// The streaming half of the abstract HTTP call, implemented once for every
// host that hands over a node ServerResponse (the built-in shell, express
// and fastify all do). Returning null means the response is already gone.
//
// It is what SSE rides on: `respond` answers with a body, `stream` keeps the
// response open and writes into it.
const nodeStream =
  (res) =>
  ({ status, headers }) => {
    if (res.writableEnded || res.destroyed) return null;
    res.writeHead(status, headers);
    // Without this the first frame can sit in node's header buffer until
    // enough body accumulates — which for a live stream is forever.
    res.flushHeaders?.();
    return {
      write: (chunk) => res.write(chunk),
      end: () => res.end(),
      onClose: (listener) => void res.on('close', listener),
      onDrain: (listener) => void res.on('drain', listener),
    };
  };

const { isOriginAllowed } = require('../transport.js');
const { STATUS_CODES } = require('../status.js');

// Shared plumbing for everything that turns a host framework's request into
// the abstract call description RpcServer.handleHttpCall consumes:
// { method, url, headers, body, remoteAddress, respond, onAbort }.
// Node-only by construction — adapters are never bundled for the browser.

const MAX_BODY_SIZE = 10 * 1024 * 1024;

const getPathname = (url) => (url ? url.split('?')[0] : '/');

// The one upgrade gate every host shares. With an explicit ws.path the
// engine already gates the pathname, and the default RPC-path gate would
// 403 every upgrade to a custom path — keep only the origin check then.
// A ws.verifyClient supplied by the app replaces the gate entirely.
const createUpgradeGate = ({ rpc, cors = null, ws = {} }) => {
  if (ws.verifyClient) return ws.verifyClient;
  const checkPath = ws.path === undefined;
  return ({ req }) => {
    if (checkPath) {
      const pathname = getPathname(req.url);
      if (pathname !== '/' && rpc.matchPath(pathname) === null) return false;
    }
    return isOriginAllowed(cors, req.headers.origin);
  };
};

// The shared 400 for a body that could not be received (too big, aborted):
// an id-less callback packet, same shape on every host.
const respondBodyError = (respond, error) => {
  const packet = { type: 'callback', id: '', error: { message: error.message, code: 400 } };
  respond({ status: 400, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(packet) });
};

const prematureClose = () => {
  const error = new Error('Premature close');
  error.code = 'ERR_STREAM_PREMATURE_CLOSE';
  return error;
};

// Listeners on the request, not `for await`: the async iterator builds an
// end-of-stream watcher and a paused-mode reader around every request —
// ~760k against ~1.07M one-chunk bodies/s (bench/http-call.js), and a
// twentieth of a REST call's CPU under bench/http-comparison.js's load.
// Same outcomes as the loop it replaces: the body (null when empty), the
// stream's own error, a premature close, and — past the limit — the stream
// destroyed and the request refused. The error listener stays once the body
// is settled, as the loop's did: a late error is swallowed, never thrown.
const receiveBody = (stream, limit = MAX_BODY_SIZE) => {
  if (!Number.isSafeInteger(limit) || limit < 0) {
    return Promise.reject(new TypeError('Body size limit must be a non-negative safe integer'));
  }
  if (stream.errored) return Promise.reject(stream.errored);
  if (stream.readableEnded) return Promise.resolve(null);
  if (stream.destroyed) return Promise.reject(prematureClose());
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let settled = false;
    const settle = () => {
      settled = true;
      stream.off('data', onData);
      stream.off('end', onEnd);
      stream.off('close', onClose);
    };
    const onData = (chunk) => {
      size += chunk.length;
      if (size <= limit) return void chunks.push(chunk);
      settle();
      // node's own stream destroyer, as the loop's return called it: a
      // server request is let go of its socket first, so the 400 still
      // reaches the peer over a connection that stays usable.
      destroyStream(stream);
      reject(new Error('Body size limit exceeded'));
    };
    const onEnd = () => {
      settle();
      resolve(chunks.length === 0 ? null : chunks.length === 1 ? chunks[0] : Buffer.concat(chunks, size));
    };
    const onError = (error) => {
      if (settled) return;
      settle();
      reject(error);
    };
    const onClose = () => {
      settle();
      reject(prematureClose());
    };
    stream.on('data', onData);
    stream.on('end', onEnd);
    stream.on('error', onError);
    stream.on('close', onClose);
    // A 'data' listener does not restart a stream something upstream
    // paused by hand; the loop read it regardless, and so does this.
    if (stream.readableFlowing === false) stream.resume();
  });
};

// Frameworks that own body parsing (fastify, express + express.json()) hand
// back an already-parsed value, but the core re-parses the JSON packet
// itself — so give it back the JSON text rather than an object it would
// stringify-by-accident inside jsonParse.
const normalizeBody = (body) => {
  if (body === undefined || body === null) return null;
  if (typeof body === 'string' || Buffer.isBuffer(body)) return body;
  if (ArrayBuffer.isView(body)) return Buffer.from(body.buffer, body.byteOffset, body.byteLength);
  try {
    return JSON.stringify(body);
  } catch {
    return null; // circular / non-serializable: the core sees "no body"
  }
};

const statusLine = (code) => `${code} ${STATUS_CODES[code] ?? 'Unknown'}`;

// The core answers with `Set-Cookie` as an array and Content-Length as a
// number; frameworks want strings and repeated header lines.
const eachHeader = (headers, visit) => {
  // for...in over the core's own header literal: Object.entries allocated an
  // array plus a [name, value] pair per header, per HTTP response.
  for (const name in headers) {
    const value = headers[name];
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) visit(name, String(value[i]));
      continue;
    }
    visit(name, String(value));
  }
};

// What a shell adds to its engine's attach options so the WebSocket
// negotiation agrees with the core (protocol.md#versioning). An engine
// selects the newest revision offered, and knows nothing of the server
// above it: a server that sends and reads no framed messages (`attachments:
// false`, a packet codec — `rpc.revision` 1) narrows it to `wrpc.v1`, so a
// 2.x client does not send a frame it would be refused. An application that
// configured its own `protocols`/`handleProtocols` owns the negotiation.
const revisionProtocols = (rpc, ws = {}) =>
  rpc.revision === 1 && !ws.protocols && !ws.handleProtocols ? { protocols: [WRPC_V1] } : null;

module.exports = {
  MAX_BODY_SIZE,
  receiveBody,
  normalizeBody,
  statusLine,
  eachHeader,
  nodeStream,
  getPathname,
  createUpgradeGate,
  revisionProtocols,
  respondBodyError,
};
