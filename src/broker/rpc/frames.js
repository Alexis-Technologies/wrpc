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

// A packet frame's body is text, a chunk frame's bytes — normalized here so
// neither side trusts the broker to have preserved the JS type.
const packetBody = (body) => toText(body);

module.exports = {
  HEADER_KIND,
  HEADER_SEQ,
  HEADER_INBOX,
  HEADER_REASON,
  KIND,
  serviceAddress,
  peerHeaders,
  seqOf,
  packetBody,
};
