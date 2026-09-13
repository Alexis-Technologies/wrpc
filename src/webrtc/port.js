'use strict';

// The WebRTC port: what the peer-to-peer transport is written against.
//
// It is wrpc's OWN structural contract, not a binding to any library — a
// W3C-shaped subset, so a browser satisfies it natively and Node satisfies
// it through whatever implementation the application injects
// (node-datachannel's polyfill is W3C-shaped as-is; werift, with its own
// event API, needs a thin wrapper — see docs/guide/webrtc.md). Nothing is
// required here, on purpose: the file is browser-safe by construction and
// the validators are the only runtime check, in the style of isEngine and
// isBackplane (duck typing per the zero-dependency injection rule).
//
//   interface RtcAdapter {
//     name?: string
//     createPeerConnection(configuration: object): RtcPeerConnectionLike
//   }
//
//   interface RtcPeerConnectionLike {
//     createDataChannel(label, { negotiated: true, id, ordered? }): RtcDataChannelLike
//     createOffer(options?: { iceRestart?: boolean }): Promise<{ type, sdp }>
//     createAnswer(): Promise<{ type, sdp }>
//     setLocalDescription(description?): Promise<void>   // no argument = implicit, per perfect negotiation
//     setRemoteDescription(description): Promise<void>
//     addIceCandidate(candidate | null): Promise<void>
//     restartIce?(): void
//     close(): void
//     readonly localDescription, signalingState, connectionState, iceConnectionState
//     readonly sctp: { maxMessageSize: number } | null
//     addEventListener / removeEventListener:
//       'negotiationneeded' | 'icecandidate' | 'connectionstatechange' | 'iceconnectionstatechange'
//   }
//
//   interface RtcDataChannelLike {
//     readonly id, label, readyState: 'connecting' | 'open' | 'closing' | 'closed', bufferedAmount
//     bufferedAmountLowThreshold: number
//     binaryType: 'arraybuffer' | 'blob'
//     send(data: string | ArrayBuffer | ArrayBufferView): void
//     close(): void
//     addEventListener / removeEventListener: 'open' | 'message' | 'close' | 'error' | 'bufferedamountlow'
//   }

const PC_METHODS = [
  'createDataChannel',
  'createOffer',
  'createAnswer',
  'setLocalDescription',
  'setRemoteDescription',
  'addIceCandidate',
  'close',
  'addEventListener',
  'removeEventListener',
];

const CHANNEL_METHODS = ['send', 'close', 'addEventListener', 'removeEventListener'];

const hasMethods = (value, names) => {
  if (typeof value !== 'object' || value === null) return false;
  for (let i = 0; i < names.length; i++) {
    if (typeof value[names[i]] !== 'function') return false;
  }
  return true;
};

const isRtcAdapter = (value) =>
  typeof value === 'object' && value !== null && typeof value.createPeerConnection === 'function';

// `sctp` may be null (before the connection is up, or on an implementation
// that never reports it) — the framing layer falls back to the interop floor.
const isRtcPeerConnection = (value) => hasMethods(value, PC_METHODS) && typeof value.signalingState === 'string';

const isRtcDataChannel = (value) => hasMethods(value, CHANNEL_METHODS) && typeof value.readyState === 'string';

/**
 * The default adapter: any W3C-shaped RTCPeerConnection constructor. In a
 * browser the global one; in Node whatever the application hands over
 * (`createW3cAdapter(require('node-datachannel/polyfill'))`).
 */
const createW3cAdapter = ({ RTCPeerConnection } = globalThis) => {
  if (typeof RTCPeerConnection !== 'function') {
    throw new TypeError(
      'createW3cAdapter: an RTCPeerConnection constructor is required — ' +
        'a browser has one; in Node inject one, e.g. createW3cAdapter(require("node-datachannel/polyfill"))',
    );
  }
  return {
    name: 'w3c',
    createPeerConnection(configuration = {}) {
      const pc = new RTCPeerConnection(configuration);
      if (!isRtcPeerConnection(pc)) {
        throw new TypeError('createW3cAdapter: the constructor did not produce an RTCPeerConnection-shaped object');
      }
      return pc;
    },
  };
};

module.exports = { isRtcAdapter, isRtcPeerConnection, isRtcDataChannel, createW3cAdapter };
