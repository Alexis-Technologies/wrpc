'use strict';

// The Node barrel of @alexify/wrpc/webrtc: the browser surface plus what
// only a server needs — the signaling unit and its connection hooks, and
// attachChannel, the attachPort of WebRTC.

const { buildMeta } = require('../rpc/client.js');
const { createSignalingUnit, createSignalingHooks } = require('./signaling.js');
const { createAssertionIssuer, generateAssertionKeys } = require('./assertionIssuer.js');
const { RtcPeerTransport } = require('./transport.js');

/**
 * A raw data channel the application negotiated itself, attached to an
 * RpcServer (anything with `attach(transport, { meta })`) — the level under
 * RtcLink and WrpcPeer. The core knows no framing, which is why this lives
 * here and not on the server: it builds the host half of the transport
 * over the channel and hands it to `server.attach`. No session at attach (a
 * channel carries no request; what the application knows goes in
 * `headers`/`data`), no ICE restart or redial (the peer connection is the
 * application's). `peer` is the client's source; `maxMessageSize` is what
 * the far side's channel accepts (the 16 KiB interop floor by default —
 * negotiateMessageSize(pc.sctp) for more). A framing error from the peer
 * is warned on the client's log as 'channel.error' and closes the channel.
 */
const attachChannel = (server, channel, options = {}) => {
  if (typeof server?.attach !== 'function') throw new TypeError('attachChannel: a server with attach() is required');
  const { peer, headers, data, remoteAddress, maxMessageSize, framing, highWaterMark, lowWaterMark, compression } =
    options;
  let client = null;
  const transport = new RtcPeerTransport(channel, {
    peer,
    maxMessageSize,
    framing,
    highWaterMark,
    lowWaterMark,
    compression,
    onError: (error) => client?.log.warn({ event: 'channel.error', peer: transport.source, err: error }),
  });
  const observed = headers || data || remoteAddress ? buildMeta({ headers, data, remoteAddress }) : null;
  client = server.attach(transport, { meta: observed });
  return client;
};

module.exports = {
  ...require('./browser.js'),
  createSignalingUnit,
  createSignalingHooks,
  createAssertionIssuer,
  generateAssertionKeys,
  attachChannel,
};
