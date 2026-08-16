'use strict';

const { ServerTransport } = require('../transport.js');
const { generateUUID } = require('../runtime/node.js');
const { createLoggerWriter } = require('../logging.js');

// Server-Sent Events as a wrpc transport.
//
// SSE is one-way, so a channel is two halves that find each other by id:
//   GET  {basePath}/events?channel=<id>   the server -> client stream
//   POST {basePath} + x-wrpc-channel: <id>   the client -> server direction
// Both halves belong to ONE server-side Client, which is what lets a
// subscription started by a POST deliver its values down the GET stream.
// The POST answers 202 with no body; every reply — callbacks included —
// comes back on the stream, exactly like the Service Worker port transport.
//
// Serverless-friendly by construction: no upgrade, no long-lived socket
// beyond the response body, and nothing but HTTP in either direction.
//
// What it cannot do is binary: SSE frames are text, so wrpc's binary streams
// are refused on this transport rather than silently corrupted.

const { CHANNEL_HEADER } = require('./constants.js');

const DEFAULT_RETENTION = 30 * 1000;
const DEFAULT_REPLAY = 100;
// The replay buffer is per-channel memory held for `retention` even with no
// stream attached — a byte budget caps it where a frame-count cap cannot
// (100 frames of 1 MB each is not "bounded").
const DEFAULT_REPLAY_BYTES = 1024 * 1024;
const DEFAULT_HEARTBEAT = 15 * 1000;
const DEFAULT_RETRY = 2000;
// One GET is one channel held for `retention` even after the response dies,
// so channel creation is memory a peer can buy with plain HTTP requests —
// both caps are generous but present, like maxSubscriptions.
const DEFAULT_MAX_CHANNELS = 10000;
const DEFAULT_MAX_CHANNELS_PER_ADDRESS = 100;

const SSE_HEADERS = {
  'Content-Type': 'text/event-stream',
  'Cache-Control': 'no-cache, no-transform',
  Connection: 'keep-alive',
  // Nginx buffers proxied responses by default, which turns a live stream
  // into one delivered at the end of time.
  'X-Accel-Buffering': 'no',
};

// A JSON packet never contains a raw newline (they are escaped inside
// strings), so one `data:` line always suffices.
const frame = (id, payload) => `id: ${id}\ndata: ${payload}\n\n`;

class ServerSseTransport extends ServerTransport {
  kind = 'sse';

  // Persistent — it carries events and subscriptions — but text-only.
  binary = false;

  #writer = null;
  #closed = false;

  constructor(channelId, remoteAddress = '') {
    super(remoteAddress);
    this.channelId = channelId;
    this.connection = this; // what Client.persistent checks
  }

  get attached() {
    return this.#writer !== null;
  }

  attach(writer) {
    this.#writer = writer;
    // A producer parked on backpressure while this channel was detached
    // writes nothing, so the new response's own 'drain' can never fire —
    // a fresh sink IS the relief, and has to say so.
    this.emit('drain');
  }

  /** True while `writer` is the one currently attached. */
  isCurrent(writer) {
    return this.#writer === writer;
  }

  detach() {
    this.#writer = null;
  }

  /**
   * Writes one SSE event. Returns the sink's backpressure signal so the
   * subscription pump can wait, exactly as it does on a socket.
   */
  writeFrame(text) {
    if (!this.#writer || this.#closed) return false;
    try {
      return this.#writer.write(text) !== false;
    } catch {
      // The response died under us; the close listener will clean up.
      return false;
    }
  }

  write(data) {
    if (typeof data !== 'string') {
      throw new Error('SSE transport carries text only: binary streams need a WebSocket');
    }
    return this.emitPacket(data);
  }

  // Set by the channel, which owns the id sequence and the replay buffer.
  emitPacket(payload) {
    return this.onPacket ? this.onPacket(payload) : false;
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    this.emit('close');
  }
}

// One channel: a Client, its transport, the outbound id sequence and the
// replay buffer that makes Last-Event-ID mean something.
//
// `key` is the identity the channel was created under (the session token
// read from the opening GET's cookie, or '' for an anonymous peer). Every
// re-attach and every POST must present the same key: without the check the
// channel id alone is a bearer token for a session-carrying Client, and ids
// travel in URLs, which proxies and logs keep.
class SseChannel {
  constructor({ id, key = '', client, transport, replay, replayBytes, retention }) {
    this.id = id;
    this.key = key;
    this.client = client;
    this.transport = transport;
    this.retention = retention;
    this.writer = null;
    this.buffer = [];
    this.replay = replay;
    this.replayBytes = replayBytes;
    this.bytes = 0;
    this.cursor = 0;
    this.timer = null;
    transport.onPacket = (payload) => this.push(payload);
  }

  push(payload) {
    const id = this.cursor++;
    const text = frame(id, payload);
    this.buffer.push({ id, text });
    this.bytes += text.length;
    // Two budgets, both honest: frame count AND bytes.
    while (
      this.buffer.length > 0 &&
      (this.buffer.length > this.replay || (this.replayBytes > 0 && this.bytes > this.replayBytes))
    ) {
      const dropped = this.buffer.shift();
      this.bytes -= dropped.text.length;
    }
    return this.transport.writeFrame(text);
  }

  /**
   * Replays everything the peer says it did not see — or says so when it
   * CANNOT. Frames the ring buffer already dropped are gone: silently
   * resuming past them used to look like "nothing was missed". The `gap`
   * frame is the client's signal to drop its channel state and start over.
   */
  resume(lastEventId) {
    const cursor = Number(lastEventId);
    if (!Number.isFinite(cursor)) return;
    const oldest = this.buffer.length > 0 ? this.buffer[0].id : this.cursor;
    if (cursor < oldest - 1) {
      this.transport.writeFrame(`event: gap\ndata: ${JSON.stringify({ oldest })}\n\n`);
      return;
    }
    for (const entry of this.buffer) {
      if (entry.id <= cursor) continue;
      this.transport.writeFrame(entry.text);
      // The sink died mid-replay: the rest would go nowhere.
      if (!this.transport.attached) break;
    }
  }
}

class SseChannels {
  #channels = new Map();
  #byAddress = new Map(); // remoteAddress -> live channel count
  #options;
  #addClient;
  #channelKey;
  #log;
  #otel;

  constructor({ addClient, channelKey = null, log = globalThis.console, otel = null, ...options } = {}) {
    this.#addClient = addClient;
    // Extracts the identity a request presents (the session token from its
    // cookie); injected by the core, which owns the SessionManager. Without
    // one every request presents the same identity and only the unguessable
    // id protects the channel.
    this.#channelKey = channelKey ?? (() => '');
    this.#log = createLoggerWriter(log);
    this.#otel = otel;
    this.#options = {
      retention: options.retention ?? DEFAULT_RETENTION,
      replay: options.replay ?? DEFAULT_REPLAY,
      heartbeat: options.heartbeat ?? DEFAULT_HEARTBEAT,
      retry: options.retry ?? DEFAULT_RETRY,
      replayBytes: options.replayBytes ?? DEFAULT_REPLAY_BYTES,
      maxChannels: options.maxChannels ?? DEFAULT_MAX_CHANNELS,
      maxChannelsPerAddress: options.maxChannelsPerAddress ?? DEFAULT_MAX_CHANNELS_PER_ADDRESS,
    };
  }

  get size() {
    return this.#channels.size;
  }

  get(channelId) {
    return this.#channels.get(channelId) ?? null;
  }

  /** True when `headers` present the identity the channel was created under. */
  authorized(channel, requestHeaders) {
    return channel.key === this.#channelKey(requestHeaders ?? {});
  }

  #refuse(call, headers, code, message) {
    const body = Buffer.from(JSON.stringify({ type: 'callback', id: '', error: { message, code } }));
    call.respond({ status: code, headers: { ...headers, 'Content-Type': 'application/json' }, body });
  }

  /**
   * Opens (or re-attaches) the server -> client half.
   * `call.stream` is the seam every adapter implements; a host that cannot
   * stream gets an honest 501 instead of a response that never arrives.
   *
   * Channel ids are SERVER-minted: a request without one gets a fresh
   * channel and learns the id from the `ready` frame. A request naming an
   * id re-attaches to that channel — 409 when it is unknown (expired,
   * another instance, or a guess), 403 when the request's cookie does not
   * present the identity the channel was created under.
   *
   * `headers` are RESPONSE headers (CORS and the rest); the REQUEST headers a
   * new channel's client is built from come off `call` itself.
   */
  open(call, { channelId = null, lastEventId = null, headers = {} } = {}) {
    if (typeof call.stream !== 'function') {
      return void call.respond({
        status: 501,
        headers: { 'Content-Type': 'application/json' },
        body: Buffer.from(
          JSON.stringify({ type: 'callback', id: '', error: { message: 'SSE unsupported', code: 501 } }),
        ),
      });
    }
    const existing = channelId ? this.#channels.get(channelId) : null;
    if (channelId && !existing) {
      // Distinguishable from every other refusal: the client reacts to 409
      // by dropping its stale channel state and starting a fresh one.
      return void this.#refuse(call, headers, 409, 'Unknown channel');
    }
    if (existing && !this.authorized(existing, call.headers)) {
      return void this.#refuse(call, headers, 403, 'Channel belongs to another session');
    }
    if (!existing) {
      const refusal = this.#capacity(call.remoteAddress ?? '');
      if (refusal) return void this.#refuse(call, headers, refusal.code, refusal.message);
    }
    const channel = existing ?? this.#create(call);
    if (existing && channel.timer) {
      clearTimeout(channel.timer);
      channel.timer = null;
    }
    const writer = call.stream({ status: 200, headers: { ...headers, ...SSE_HEADERS } });
    if (!writer) {
      // The host could not open the stream. A channel created for it would
      // sit there with no writer, no retention timer and no close listener —
      // unreachable and never collected.
      if (!existing) channel.transport.close();
      return;
    }
    // Replacing a live writer: the superseded response is nobody's now, so
    // end it rather than leaking it open until a proxy times it out.
    const previous = channel.writer;
    channel.writer = writer;
    if (previous && previous !== writer) {
      try {
        previous.end();
      } catch (error) {
        this.#log.error({ err: error, event: 'sse.supersede', channel: channel.id });
      }
    }
    channel.transport.attach(writer);
    // The response's own high-water mark is the channel's backpressure, so
    // the subscription pump waits on it exactly as it does on a socket.
    writer.onDrain?.(() => channel.transport.emit('drain'));
    // The client needs its channel id before it can POST anything, and this
    // frame is the ONLY place the server hands the id out.
    writer.write(`retry: ${this.#options.retry}\n\n`);
    writer.write(`event: ready\ndata: ${JSON.stringify({ channel: channel.id })}\n\n`);
    if (existing && lastEventId !== null) channel.resume(lastEventId);
    this.#beat(channel, writer);
    writer.onClose?.(() => this.#detach(channel, writer));
    return channel;
  }

  #capacity(address) {
    const { maxChannels, maxChannelsPerAddress } = this.#options;
    if (maxChannels > 0 && this.#channels.size >= maxChannels) {
      return { code: 503, message: 'Too many channels' };
    }
    const held = this.#byAddress.get(address) ?? 0;
    if (maxChannelsPerAddress > 0 && held >= maxChannelsPerAddress) {
      return { code: 429, message: 'Too many channels from this address' };
    }
    return null;
  }

  // The GET that opens a channel is the channel's only handshake, so its
  // headers are handed on: they carry the cookie the client's session is
  // restored from, exactly as an upgrade's headers do for a socket. The id
  // is minted here — never taken from the request — so holding one proves
  // the server said it, and the cookie's token is captured as the key every
  // later request must present again.
  #create(call) {
    const channelId = generateUUID();
    const address = call.remoteAddress ?? '';
    const transport = new ServerSseTransport(channelId, address);
    const client = this.#addClient(transport, call.headers ?? {});
    const key = this.#channelKey(call.headers ?? {});
    const channel = new SseChannel({ id: channelId, key, client, transport, ...this.#options });
    this.#channels.set(channelId, channel);
    this.#byAddress.set(address, (this.#byAddress.get(address) ?? 0) + 1);
    // Every teardown path — retention timeout, close(), a dead response —
    // ends at transport.close(), which is idempotent, so this is the one
    // place the gauge can come back down exactly once.
    this.#otel?.recordSseChannel(1);
    transport.once('close', () => {
      this.#channels.delete(channelId);
      const held = this.#byAddress.get(address) ?? 0;
      if (held <= 1) this.#byAddress.delete(address);
      else this.#byAddress.set(address, held - 1);
      this.#otel?.recordSseChannel(-1);
    });
    return channel;
  }

  // Comment frames keep proxies (and some mobile networks) from deciding an
  // idle response is a dead one.
  #beat(channel, writer) {
    if (!(this.#options.heartbeat > 0)) return;
    const timer = setInterval(() => {
      // Scoped to this writer: a superseded stream's heartbeat must not keep
      // writing into the one that replaced it.
      if (channel.transport.isCurrent(writer)) return void channel.transport.writeFrame(': ping\n\n');
      clearInterval(timer);
    }, this.#options.heartbeat);
    timer.unref?.();
    writer.onClose?.(() => clearInterval(timer));
  }

  // A dropped stream is usually a network blip, not a goodbye: the channel
  // (and everything it is subscribed to) is held for `retention` so a
  // reconnect can re-attach and replay instead of starting over.
  #detach(channel, writer) {
    // Scoped to the writer that actually closed. A response replaced by a
    // reconnect closes LATER than the one that replaced it, and detaching on
    // that would tear down a perfectly live stream.
    if (!channel.transport.isCurrent(writer)) return;
    channel.transport.detach();
    channel.writer = null;
    if (channel.timer) clearTimeout(channel.timer);
    channel.timer = setTimeout(() => {
      this.#channels.delete(channel.id);
      channel.transport.close();
    }, channel.retention);
    channel.timer.unref?.();
  }

  /** Ends every channel; used when the server itself closes. */
  close() {
    for (const channel of Array.from(this.#channels.values())) {
      if (channel.timer) clearTimeout(channel.timer);
      this.#channels.delete(channel.id);
      try {
        // End the response too: a stream left open holds the socket, and a
        // host waiting on `server.close()` would wait forever.
        channel.writer?.end();
        channel.writer = null;
        channel.transport.detach();
        channel.transport.close();
      } catch (error) {
        this.#log.error({ err: error, event: 'sse.close', channel: channel.id });
      }
    }
  }
}

module.exports = {
  ServerSseTransport,
  SseChannel,
  SseChannels,
  SSE_HEADERS,
  CHANNEL_HEADER,
  DEFAULT_RETENTION,
  DEFAULT_REPLAY,
};
