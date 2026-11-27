'use strict';

// What an upgrade request says before there is a connection: the path it
// asks for and the subprotocol it gets. One reading for every engine — the
// built-in server, the uWebSockets.js adapter — and for the adapters that
// gate an upgrade they do not own.

const { WRPC_V1, WRPC_V2, CARRIER_PROTOCOL } = require('../wire.js');

const getPathname = (url) => (url ? url.split('?')[0] : '/');

// What a `verifyClient` gate answered, read fail-closed: `true` admits and
// nothing else does. A promise is truthy, so an `async` gate used to admit
// every peer whatever it resolved to; it is refused as 'async' — the engine
// says so once — and its rejection is kept from going unhandled.
const gateVerdict = (verifyClient, info) => {
  const verdict = verifyClient(info);
  if (verdict === true) return 'admit';
  const isPending = verdict !== null && typeof verdict === 'object' && typeof verdict.then === 'function';
  if (!isPending) return 'refuse';
  verdict.then(undefined, () => {});
  return 'async';
};

/**
 * The subprotocol to answer an offer with (the `Sec-WebSocket-Protocol`
 * header as it arrived): a name, `''` for none, or `false` when the
 * application's `handleProtocols` refused the handshake.
 *
 * Carrier tokens (wire.js) are DATA riding the offer, never a protocol to
 * select: they leave the list before the application sees it, and one that
 * comes back anyway is not echoed — `(offered) => offered.at(-1)` would
 * otherwise reflect a Bearer credential into the response headers.
 *
 * With no application configuration the newest wrpc revision offered is
 * answered — a 2.x client offers both, a 1.0 client `wrpc.v1` alone.
 * Required, not a nicety: a browser fails the whole connection when it
 * offered subprotocols and the server selected none.
 */
const negotiateProtocol = (header, { protocols, handleProtocols }, request) => {
  if (!header) return '';
  const tokens = header.split(',');
  const offered = [];
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i].trim();
    if (token.length > 0 && !CARRIER_PROTOCOL.test(token)) offered.push(token);
  }
  if (handleProtocols) {
    const selected = handleProtocols(offered, request);
    if (selected === false) return false;
    return typeof selected === 'string' && !CARRIER_PROTOCOL.test(selected) ? selected : '';
  }
  if (protocols) {
    for (let i = 0; i < offered.length; i++) if (protocols.includes(offered[i])) return offered[i];
    return '';
  }
  if (offered.includes(WRPC_V2)) return WRPC_V2;
  if (offered.includes(WRPC_V1)) return WRPC_V1;
  return '';
};

module.exports = { getPathname, gateVerdict, negotiateProtocol };
