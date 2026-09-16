'use strict';

// The WebTransport port: what the server half is written against.
//
// It is wrpc's OWN structural contract, not a binding to any library — the
// W3C WebTransport session shape, so a session from any implementation
// satisfies it as-is (@fails-components/webtransport's does; a browser's
// WebTransport object does too, which is what lets the fake in tests be one
// class for both ends) and one with its own API needs a thin wrapper
// (quico's node-stream shape — see quico.js). Nothing is required here, on
// purpose: the validators are the only runtime check, in the style of
// isEngine, isBackplane and isRtcAdapter (duck typing per the
// zero-dependency injection rule). Node never speaks HTTP/3 itself: the
// implementation is the application's to inject, never this package's to
// depend on.
//
//   interface WtSession {
//     readonly ready?: Promise<void>          // settled before a server session is handed over
//     readonly closed: Promise<{ closeCode: number, reason: string }>
//     readonly incomingBidirectionalStreams: ReadableStream<WtStream>
//     readonly incomingUnidirectionalStreams?: ReadableStream<ReadableStream<Uint8Array>>
//     createBidirectionalStream(): Promise<WtStream>
//     createUnidirectionalStream?(): Promise<WritableStream<Uint8Array>>
//     readonly datagrams?: WtDatagrams         // absent on an implementation without them
//     close(info?: { closeCode?: number, reason?: string }): void
//   }
//
//   interface WtStream {
//     readonly readable: ReadableStream<Uint8Array>
//     readonly writable: WritableStream<Uint8Array>
//   }
//
//   interface WtDatagrams {
//     readonly readable: ReadableStream<Uint8Array>
//     readonly writable?: WritableStream<Uint8Array>      // the legacy stream, or
//     createWritable?(): WritableStream<Uint8Array>        // the newer factory
//     readonly maxDatagramSize?: number
//   }

const isObject = (value) => typeof value === 'object' && value !== null;
const isReadable = (value) => isObject(value) && typeof value.getReader === 'function';
const isWritable = (value) => isObject(value) && typeof value.getWriter === 'function';

/** A `{ readable, writable }` pair of WHATWG streams. */
const isWtStream = (value) => isObject(value) && isReadable(value.readable) && isWritable(value.writable);

/** The datagram duplex: a readable and a writable (or the newer `createWritable()`), `maxDatagramSize` optional. */
const isWtDatagrams = (value) =>
  isObject(value) &&
  isReadable(value.readable) &&
  (isWritable(value.writable) || typeof value.createWritable === 'function') &&
  (value.maxDatagramSize === undefined || typeof value.maxDatagramSize === 'number');

/** The session shape attachSession() takes — either end of a WebTransport. */
const isWtSession = (value) =>
  isObject(value) &&
  typeof value.close === 'function' &&
  typeof value.createBidirectionalStream === 'function' &&
  isReadable(value.incomingBidirectionalStreams) &&
  isObject(value.closed) &&
  typeof value.closed.then === 'function' &&
  (value.datagrams === undefined || value.datagrams === null || isWtDatagrams(value.datagrams));

module.exports = { isWtSession, isWtStream, isWtDatagrams };
