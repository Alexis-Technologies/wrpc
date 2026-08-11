'use strict';

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

const http = require('node:http');

// Shared plumbing for everything that turns a host framework's request into
// the abstract call description RpcServer.handleHttpCall consumes:
// { method, url, headers, body, remoteAddress, respond, onAbort }.
// Node-only by construction — adapters are never bundled for the browser.

const MAX_BODY_SIZE = 10 * 1024 * 1024;

const receiveBody = async (stream, limit = MAX_BODY_SIZE) => {
  if (!Number.isSafeInteger(limit) || limit < 0) {
    throw new TypeError('Body size limit must be a non-negative safe integer');
  }
  const chunks = [];
  let size = 0;
  for await (const chunk of stream) {
    size += chunk.length;
    if (size > limit) throw new Error('Body size limit exceeded');
    chunks.push(chunk);
  }
  if (chunks.length === 0) return null;
  if (chunks.length === 1) return chunks[0];
  return Buffer.concat(chunks, size);
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

const statusLine = (code) => `${code} ${http.STATUS_CODES[code] ?? 'Unknown'}`;

// The core answers with `Set-Cookie` as an array and Content-Length as a
// number; frameworks want strings and repeated header lines.
const eachHeader = (headers, visit) => {
  for (const [name, value] of Object.entries(headers)) {
    if (Array.isArray(value)) {
      for (const item of value) visit(name, String(item));
      continue;
    }
    visit(name, String(value));
  }
};

module.exports = { MAX_BODY_SIZE, receiveBody, normalizeBody, statusLine, eachHeader, nodeStream };
