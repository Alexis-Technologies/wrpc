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
// included — so its list is the AMBIENT facts alone: what a proxy sets
// (the forwarded chain, the client-ip spellings of the CDNs), what a
// browser's fetch sets and script cannot (sec-*), the origin. Their ABSENCE
// is a fact too: a declared one is dropped whether or not the outer
// request carried it. The ws handshake's list is the wider one: a
// declaration there rides beside the real headers and may not stand in for
// anything the handshake itself owns.

const AMBIENT_HEADERS =
  /^(?:host|origin|forwarded|via|x-real-ip|x-client-ip|true-client-ip|cf-connecting-ip)$|^(?:sec-|proxy-|x-forwarded-)/;

// Beside the connection facts, the names an identity-aware proxy sets ABOUT
// the user it authenticated — an OAuth2 proxy (`x-auth-request-*`, not the
// wider `x-auth-`, which an application's own `x-auth-token` lives in), AWS
// ALB OIDC (`x-amzn-oidc-*`), Google IAP (`x-goog-authenticated-user-*`,
// `x-goog-iap-*`), Azure Easy Auth (`x-ms-client-principal*`), the
// `remote-user` of Apache/nginx auth modules, and the client-ip spellings
// of Fastly and Fly. A page declaring one of these would be declaring who
// the proxy said it is.
const RESERVED_DECLARED =
  /^(?:cookie|host|origin|forwarded|via|x-real-ip|x-client-ip|true-client-ip|cf-connecting-ip|fastly-client-ip|fly-client-ip|remote-user)$|^(?:sec-|content-|proxy-|x-wrpc-|x-forwarded-|x-auth-request-|x-amzn-oidc-|x-goog-authenticated-user-|x-goog-iap-|x-ms-client-principal)/;

module.exports = { AMBIENT_HEADERS, RESERVED_DECLARED };
