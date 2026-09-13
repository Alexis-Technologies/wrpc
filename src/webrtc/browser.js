'use strict';

// The browser barrel of @alexify/wrpc/webrtc: everything a page needs to be
// a peer. The Node barrel (index.js) adds the server-side signaling unit.
// Requiring transport.js registers `WrpcClient.transport.webrtc`.

const { isRtcAdapter, isRtcPeerConnection, isRtcDataChannel, createW3cAdapter } = require('./port.js');
const framing = require('./framing.js');
const { RtcLink, normalizeChannels, DEFAULT_CHANNELS, MAX_CHANNEL_ID } = require('./link.js');
const { ClientRtcTransport, RtcPeerTransport } = require('./transport.js');
const { PeerHost } = require('./host.js');

module.exports = {
  isRtcAdapter,
  isRtcPeerConnection,
  isRtcDataChannel,
  createW3cAdapter,
  RtcLink,
  normalizeChannels,
  DEFAULT_CHANNELS,
  MAX_CHANNEL_ID,
  ClientRtcTransport,
  RtcPeerTransport,
  PeerHost,
  FrameEncoder: framing.FrameEncoder,
  FrameDecoder: framing.FrameDecoder,
  FramingError: framing.FramingError,
  negotiateMessageSize: framing.negotiateMessageSize,
  KIND_TEXT: framing.KIND_TEXT,
  KIND_BINARY: framing.KIND_BINARY,
};
