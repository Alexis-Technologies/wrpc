'use strict';

// The browser barrel of @alexify/wrpc/webrtc: everything a page needs to be
// a peer. The Node barrel (index.js) adds the server-side signaling unit.
// Requiring transport.js registers `WrpcClient.transport.webrtc`.

const { isRtcAdapter, isRtcPeerConnection, isRtcDataChannel, createW3cAdapter } = require('./port.js');
const framing = require('./framing.js');
const { RtcLink, normalizeChannels, DEFAULT_CHANNELS, MAX_CHANNEL_ID } = require('./link.js');
const { ClientRtcTransport, RtcPeerTransport } = require('./transport.js');
const { PeerHost } = require('./host.js');
const signaler = require('./signaler.js');
const assertions = require('./assertions.js');
// A browser peer defines its router with these; the main browser entry
// leaves them out for its byte budget, and they are already in this bundle
// (the host dispatches over a Router).
const { defineRouter, procedure } = require('../rpc/router.js');
const { tracked, createEventLog } = require('../rpc/subscriptions.js');
const { WrpcPeer, PeerLink } = require('./peer.js');
const { Mesh } = require('./mesh.js');

const { buildDictionary } = require('../rpc/dictionary.js');

module.exports = {
  defineRouter,
  buildDictionary,
  procedure,
  tracked,
  createEventLog,
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
  WrpcPeer,
  PeerLink,
  Mesh,
  WrpcSignaler: signaler.WrpcSignaler,
  wrpcSignaler: signaler.wrpcSignaler,
  isSignaler: signaler.isSignaler,
  hasRoster: signaler.hasRoster,
  hasAssertions: signaler.hasAssertions,
  isSignalMessage: signaler.isSignalMessage,
  SIGNAL_MESSAGE_TYPES: signaler.SIGNAL_MESSAGE_TYPES,
  createAssertionVerifier: assertions.createAssertionVerifier,
  sdpFingerprint: assertions.sdpFingerprint,
  normalizeFingerprint: assertions.normalizeFingerprint,
  isAssertion: assertions.isAssertion,
  AssertionError: assertions.AssertionError,
  FrameEncoder: framing.FrameEncoder,
  FrameDecoder: framing.FrameDecoder,
  FramingError: framing.FramingError,
  negotiateMessageSize: framing.negotiateMessageSize,
  KIND_TEXT: framing.KIND_TEXT,
  KIND_BINARY: framing.KIND_BINARY,
};
