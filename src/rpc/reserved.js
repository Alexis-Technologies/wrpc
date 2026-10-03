'use strict';

// Header names a peer may not DECLARE for itself, because they say what the
// connection — or a proxy in front of it — knows about the sender, and a
// declaration is the sender's word. Require-free: handshake.js (the ws
// carriers) and, through the core, src/encryption/httpServer.js (the
// headers inside a sealed request) share it — src/encryption/ never
// requires src/rpc/, so the core hands the list over.
//
// Two lists, because the two carriers differ in what they may carry. A
// sealed request carries the REAL request — its content-* and x-wrpc-*
// included — so its list is the AMBIENT facts alone: what a proxy says about
// the connection and, behind an identity-aware proxy, about the user. Their ABSENCE
// is a fact too: a declared one is dropped whether or not the outer
// request carried it. The ws handshake's list is the wider one: a
// declaration there rides beside the real headers and may not stand in for
// anything the handshake itself owns.

// What a proxy, a CDN or a browser says about the CONNECTION: the
// forwarded chain, the client-ip spellings (Cloudflare, Akamai, Fastly,
// Fly), what a browser's fetch sets and script cannot (sec-*), the origin.
const CONNECTION_NAMES =
  'host|origin|forwarded|via|x-real-ip|x-client-ip|true-client-ip|cf-connecting-ip|fastly-client-ip|fly-client-ip';
const CONNECTION_PREFIXES = 'sec-|proxy-|x-forwarded-';

// What an identity-aware proxy says about the USER it authenticated — an
// OAuth2 proxy (`x-auth-request-*`, not the wider `x-auth-`, which an
// application's own `x-auth-token` lives in), AWS ALB OIDC
// (`x-amzn-oidc-*`), Google IAP (`x-goog-authenticated-user-*`,
// `x-goog-iap-*`), Azure Easy Auth (`x-ms-client-principal*`), the
// `remote-user` of Apache/nginx auth modules. A page declaring one would be
// declaring who the proxy said it is — inside a sealed request as much as on
// a handshake, which is why both lists are built from these pieces: the
// sealed list once lacked every name here.
const IDENTITY_NAMES = 'remote-user';
const IDENTITY_PREFIXES = 'x-auth-request-|x-amzn-oidc-|x-goog-authenticated-user-|x-goog-iap-|x-ms-client-principal';

const AMBIENT_HEADERS = new RegExp(
  `^(?:${CONNECTION_NAMES}|${IDENTITY_NAMES})$|^(?:${CONNECTION_PREFIXES}|${IDENTITY_PREFIXES})`,
);

// The handshake's list adds what the handshake itself owns.
const RESERVED_DECLARED = new RegExp(
  `^(?:cookie|${CONNECTION_NAMES}|${IDENTITY_NAMES})$|^(?:content-|x-wrpc-|${CONNECTION_PREFIXES}|${IDENTITY_PREFIXES})`,
);

module.exports = { AMBIENT_HEADERS, RESERVED_DECLARED };
