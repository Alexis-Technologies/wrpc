'use strict';

// The broker binding's framing: what rides in a `direct` message's headers
// around an unmodified wrpc packet. Documented in
// docs/reference/protocol.md#broker-binding.
//
//   stateless  client --request{correlationId, replyTo}--> service address (group)
//              client <--response{correlationId}---------- the instance that took it
//
//   session    client --hello{correlationId: session, replyTo}--> service address (group)
//              client <--welcome{wrpc-inbox}------------------- the instance that took it
//              client --packet|chunk{wrpc-seq}--> that instance's inbox, both ways
//              either side --bye--> the other
//
// A session's frames are numbered per direction from 1: the direct
// capability is at-most-once, and a gap is a lost connection, not a
// silently missing packet.

const { toText } = require('../ids.js');

const HEADER_KIND = 'wrpc-kind';
const HEADER_SEQ = 'wrpc-seq';
const HEADER_INBOX = 'wrpc-inbox';
const HEADER_REASON = 'wrpc-reason';
// Per-message compression (src/compression): on a `request`, a `hello` and
// a `welcome`, the codecs the sender holds — ids joined by commas, in its
// order of preference; on a `response`, `packet` or `chunk`, the ONE codec
// its body IS compressed with.
const HEADER_ENC = 'wrpc-enc';
const RESERVED_PREFIX = 'wrpc-';

const KIND = Object.freeze({
  REQUEST: 'request',
  RESPONSE: 'response',
  HELLO: 'hello',
  WELCOME: 'welcome',
  PACKET: 'packet',
  CHUNK: 'chunk',
  BYE: 'bye',
});

const DEFAULT_PREFIX = 'wrpc';

/** The service address a name maps to: `wrpc.<service>` unless given. */
const serviceAddress = (service, address, label) => {
  if (address !== undefined && address !== null) {
    if (typeof address !== 'string' || address.length === 0) {
      throw new TypeError(`${label}: address must be a non-empty string`);
    }
    return address;
  }
  if (typeof service !== 'string' || service.length === 0) {
    throw new TypeError(`${label}: service must be a non-empty string`);
  }
  return `${DEFAULT_PREFIX}.${service}`;
};

// What a peer declared about itself (authorization, x-wrpc-meta, ...), with
// the binding's own header names removed — a peer cannot forge a frame kind
// or a sequence number through its connection headers — and lower-cased,
// the HTTP spelling every token carrier reads.
const peerHeaders = (headers) => {
  const out = {};
  if (!headers) return out;
  for (const key of Object.keys(headers)) {
    const name = key.toLowerCase();
    if (name.startsWith(RESERVED_PREFIX)) continue;
    const value = headers[key];
    if (value === undefined || value === null) continue;
    out[name] = String(value);
  }
  return out;
};

const seqOf = (headers) => {
  const value = Number(headers?.[HEADER_SEQ]);
  return Number.isSafeInteger(value) && value > 0 ? value : 0;
};

// Sealed frames (../sealing.js): what a frame is bound to — where it was
// sent, what kind it is, whose conversation, which frame of it. The kind and
// the sequence number stay readable (the binding routes by them before it
// can open anything) and are part of this string, so neither can be
// rewritten; everything else a frame carried in its headers — the peer's
// `authorization`, `x-wrpc-meta`, the codec id, the inbox, the reason — is
// inside.
const contextOf = (address, kind, correlationId, seq) =>
  `${address}\0${kind ?? ''}\0${correlationId ?? ''}\0${seq ?? ''}`;

const sealFrame = (sealing, address, correlationId, headers, body) => {
  if (sealing === null) return { headers, body };
  const inner = { ...headers };
  const kind = inner[HEADER_KIND];
  const seq = inner[HEADER_SEQ];
  delete inner[HEADER_KIND];
  delete inner[HEADER_SEQ];
  const outer = seq === undefined ? { [HEADER_KIND]: kind } : { [HEADER_KIND]: kind, [HEADER_SEQ]: seq };
  return sealing.seal(contextOf(address, kind, correlationId, seq), inner, body, outer);
};

/** The frame as it was before sealing, `{ refused: reason }`, or the message itself when nothing seals. */
const openFrame = (sealing, address, message) => {
  if (sealing === null) return message;
  const kind = message.headers?.[HEADER_KIND];
  const seq = message.headers?.[HEADER_SEQ];
  const result = sealing.open(contextOf(address, kind, message.correlationId, seq), message);
  if (result.refused !== undefined || result.sealed === false) return result.refused === undefined ? message : result;
  const headers = { ...result.headers, [HEADER_KIND]: kind };
  if (seq !== undefined) headers[HEADER_SEQ] = seq;
  return { headers, body: result.body, correlationId: message.correlationId, replyTo: message.replyTo, sealed: true };
};

// A packet frame's body is text, a chunk frame's bytes — normalized here so
// neither side trusts the broker to have preserved the JS type.
const packetBody = (body) => toText(body);

module.exports = {
  HEADER_KIND,
  HEADER_SEQ,
  HEADER_INBOX,
  HEADER_REASON,
  HEADER_ENC,
  KIND,
  serviceAddress,
  peerHeaders,
  seqOf,
  packetBody,
  sealFrame,
  openFrame,
};
