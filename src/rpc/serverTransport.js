'use strict';

// The base every server-side transport extends: the outbound half of a
// connection as the dispatcher and Client see it — send(obj) → write(text),
// error(code) → a callback packet, and the 'close'/'drain' events a
// persistent one emits. On its own file so a transport that lives in a
// browser bundle (the WebRTC peer half) does not drag the HTTP and
// WebSocket transports, cookies and CORS in with it; src/transport.js
// re-exports it, so nothing else changed its require path.

const { Emitter } = require('../utils.js');
const { wireError } = require('./errors.js');

class ServerTransport extends Emitter {
  // Which wire this is, for log entries and metric attributes. Subclasses
  // override it; the base value covers a transport nobody labelled.
  kind = 'unknown';

  constructor(source) {
    // No listener cap: transports are fan-out points — every backpressured
    // outbound stream on the connection parks a once('drain'|'close')
    // listener here, and the default cap of 10 would throw on the 11th
    // concurrently stalled stream.
    super({ maxListeners: Number.MAX_SAFE_INTEGER });
    this.source = source;
  }

  error(code = 500, { id = '', error = null } = {}) {
    const packet = { type: 'callback', id, error: wireError(code, error) };
    return this.send(packet, code);
  }

  // Returns the transport's backpressure signal (false = above the
  // high-water mark) so a producer — a subscription pump, a stream — can
  // wait for 'drain' instead of buffering without limit.
  //
  // `text` is the already-serialized form of `obj` when the dispatcher's
  // compiled-serializer fast path built one (see handleRpc); passing both
  // keeps the object available to the overrides that need it (batch
  // collection, REST unwrapping) while the plain path skips a stringify.
  send(obj, code = 200, text = null) {
    // An injected codec (RpcServer options.codec, assigned per transport)
    // re-frames every packet; it wins over precompiled `text` by
    // construction — the server refuses codec + serializers up front.
    if (this.codec) return this.write(this.codec.encode(obj), code);
    return this.write(text ?? JSON.stringify(obj), code);
  }
}

// The shape of a transport a host can attach by itself — persistent
// (`connection` set) and announcing inbound traffic as 'packet' (text) and
// 'chunk' (bytes) events. What PeerHost.attach and RpcServer.attach both
// check; structural, so a transport need not extend ServerTransport.
const isInboundTransport = (transport) =>
  typeof transport === 'object' &&
  transport !== null &&
  typeof transport.write === 'function' &&
  typeof transport.close === 'function' &&
  typeof transport.on === 'function' &&
  typeof transport.once === 'function' &&
  Boolean(transport.connection);

module.exports = { ServerTransport, isInboundTransport };
