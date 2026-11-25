'use strict';

const { WrpcClient, ClientTransport, metaHeaders } = require('../client.js');
const { refusedStatus } = require('../client/core.js');
const { CHANNEL_HEADER } = require('./constants.js');
const { ConfigError } = require('../errors.js');

// The client half of the SSE transport. Browser-safe: `fetch`, streams and
// TextDecoder only — no node builtins, and deliberately not `EventSource`,
// which cannot set headers, cannot be aborted cleanly, and reconnects on its
// own schedule instead of the client's.
//
//   require('@alexify/wrpc/sse');                       // registers it
//   await WrpcClient.connect(url, { transport: 'sse' });
//
// Calls go out as POSTs; every answer comes back down the one event stream.
//
// The channel id is SERVER-minted: the first GET carries none, the server
// answers with a `ready` frame naming the channel, and only then can this
// transport POST. A reconnect presents the id again (by header — URLs end
// up in logs) together with the same cookies; a 409 means the channel is
// gone (retention expired, another instance), and the transport starts
// over with a fresh one.

// Incremental SSE parser (WHATWG "event stream" rules): fields are
// `field: value` lines, a blank line dispatches, `data:` lines accumulate
// with newlines between them, and a leading colon is a comment.
//
// A chunk is scanned ONCE, where it arrives, and a line still open at its
// end is kept as the pieces it came in — joined when the line ends. It used
// to be `buffer += chunk`, the whole buffer scanned again from its start,
// then `buffer = buffer.slice(…)`: a line that did not end cost the square
// of its length — 250 KiB in 1 KiB chunks 0.1 s, 1 MiB 1.65 s, 2 MiB 6.4 s
// of a page's main thread — and nothing bounded it. What an event may hold
// while it is being put together is `maxMessage` characters now (16 MiB,
// the default of the other carriers): past it the stream is an error.
const MAX_MESSAGE = 16 * 1024 * 1024;

class SseParser {
  #pieces = [];
  #open = 0;
  #held = 0;
  #cr = false;
  #max;
  #data = [];
  #event = '';
  #id = null;

  constructor({ maxMessage = MAX_MESSAGE } = {}) {
    this.#max = maxMessage;
  }

  /** Feeds a chunk and returns the events it completed. */
  push(chunk) {
    const events = [];
    let start = 0;
    // The chunk before ended on a \r, which ended its line there and then:
    // a \n that opens this one is the second half of that \r\n.
    if (this.#cr) {
      this.#cr = false;
      if (chunk.charCodeAt(0) === 10) start = 1;
    }
    for (let i = start; i < chunk.length; i++) {
      const code = chunk.charCodeAt(i);
      if (code !== 10 && code !== 13) continue;
      const event = this.#line(this.#take(chunk, start, i));
      if (event) events.push(event);
      if (code === 13) {
        if (i + 1 === chunk.length) this.#cr = true;
        else if (chunk.charCodeAt(i + 1) === 10) i++;
      }
      start = i + 1;
    }
    if (start < chunk.length) {
      this.#pieces.push(start === 0 ? chunk : chunk.slice(start));
      this.#open += chunk.length - start;
      this.#bound();
    }
    return events;
  }

  // The line that ends at `end` of this chunk: what earlier chunks left
  // open, and this chunk's part of it.
  #take(chunk, start, end) {
    if (this.#open === 0) return chunk.slice(start, end);
    this.#pieces.push(chunk.slice(start, end));
    const line = this.#pieces.join('');
    this.#pieces = [];
    this.#open = 0;
    return line;
  }

  #bound() {
    if (this.#held + this.#open > this.#max) throw new Error('SSE event exceeds maxMessage');
  }

  #line(line) {
    if (line === '') return this.#dispatch();
    if (line.startsWith(':')) return null; // comment (the proxy heartbeat)
    const colon = line.indexOf(':');
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? '' : line.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    if (field === 'data') {
      this.#data.push(value);
      this.#held += value.length;
      this.#bound();
    } else if (field === 'event') this.#event = value;
    // Per the spec an id containing U+0000 is ignored. Written as an
    // escape on purpose: a literal NUL in a source file turns it into a
    // binary blob for grep and most editors.
    else if (field === 'id' && !value.includes('\u0000')) this.#id = value;
    // `retry` is honoured by the client's own backoff, not by this parser.
    return null;
  }

  #dispatch() {
    if (this.#data.length === 0) {
      this.#event = '';
      return null;
    }
    const event = { id: this.#id, event: this.#event || 'message', data: this.#data.join('\n') };
    this.#data = [];
    this.#held = 0;
    this.#event = '';
    return event;
  }
}

const joinUrl = (base, path) => (base.endsWith('/') ? base.slice(0, -1) : base) + path;

class ClientSseTransport extends ClientTransport {
  // Carries `options.encryption`, per request like http — see open().
  // Sealed per request, like http: needs the option's `fetch`.
  static encrypts = 'request';

  // The stream can die without a close frame just like a socket can.
  heartbeat = true;

  // Revision 1 against any server: an event stream carries no frame, so a
  // packet holding bytes leaves as the JSON 1.0 made of it.
  revision = 1;

  #controller = null;
  // The channel reference, exactly as the `ready` frame gave it: presented
  // again on every POST and re-attach, forgotten with the channel. Opaque
  // on purpose — a 2.x server puts the channel's secret after its id there
  // (`<id>.<secret>`), a 1.0 server the id alone, and this side has no
  // reason to tell them apart.
  #channel = null;
  #lastEventId = null;
  #parser = null;
  #reading = null;
  #onReady = null;
  // Connection-phase headers and metadata, resolved per open — real request
  // headers on both the stream GET and every packet POST.
  #headers = null;
  #meta = null;
  // Injectable fetch, resolved per open like headers/meta. Always CALLED
  // unbound (never as this.#fetch(...) via a stored `this`) — native fetch
  // throws "Illegal invocation" in browsers when invoked with a receiver
  // other than the global object, which a plain method call would hand it.
  #fetch = null;
  // What one event of the stream may hold while it is put together
  // (`maxMessage`; the parser's own default when the option is not set).
  #maxMessage = undefined;

  get eventsUrl() {
    return joinUrl(this.url, '/events');
  }

  async open(options = {}) {
    if (options.maxMessage > 0) this.#maxMessage = options.maxMessage;
    this.#headers = options.headers ?? null;
    // A header BLOCK, not one encoded value: the client's metaFormat picks
    // the spelling, both legs below just spread the result.
    this.#meta = options.meta ? metaHeaders(options.meta, options.metaPrefixed) : null;
    this.#fetch = options.fetch ?? globalThis.fetch;
    // Session encryption has no session here: every request — the stream
    // GET and each packet POST — is sealed on its own (HPKE), and the
    // stream comes back sealed frame by frame, all inside a wrapped fetch.
    const { encryption = null } = options;
    if (encryption !== null) {
      if (typeof encryption.fetch !== 'function') {
        throw new ConfigError('options.encryption has no serverKey to seal a request to — the sse transport needs one');
      }
      this.#fetch = encryption.fetch(this.#fetch, this.url);
    }
    if (this.active) return;
    if (this.#reading) return this.#reading;
    const opening = this.#open(true);
    this.#reading = opening;
    try {
      await opening;
    } finally {
      this.#reading = null;
    }
  }

  async #open(retryOnGone) {
    const controller = new AbortController();
    this.#controller = controller;
    this.#parser = new SseParser({ maxMessage: this.#maxMessage });
    // Declared first, wire headers after: the protocol's own always win.
    const headers = { ...this.#headers, ...this.#meta, accept: 'text/event-stream' };
    // A reconnect presents the channel and where it stopped; the server
    // replays what this channel did not acknowledge.
    if (this.#channel !== null) headers[CHANNEL_HEADER] = this.#channel;
    if (this.#lastEventId !== null) headers['last-event-id'] = this.#lastEventId;
    const doFetch = this.#fetch;
    const response = await doFetch(this.eventsUrl, { headers, signal: controller.signal, cache: 'no-store' });
    // 409: the channel this transport remembers no longer exists. Its
    // replay state is worthless now — drop it and start a fresh channel;
    // the WrpcClient above re-loads and re-subscribes on 'open'/'reconnect'.
    if (response.status === 409 && retryOnGone && this.#channel !== null) {
      await response.body?.cancel?.();
      this.#channel = null;
      this.#lastEventId = null;
      return this.#open(false);
    }
    if (!response.ok || !response.body) {
      throw new Error(`SSE stream refused with ${response.status}`);
    }
    // Not connected until the server names the channel: a POST before the
    // `ready` frame would have no channel to belong to.
    const consuming = this.#consume(response.body);
    void consuming.catch((error) => {
      if (!controller.signal.aborted) this.emit('error', error);
    });
    await new Promise((resolve, reject) => {
      let settled = false;
      const fail = (error) => {
        if (settled) return;
        settled = true;
        reject(error);
      };
      this.#onReady = () => {
        if (settled) return;
        settled = true;
        resolve();
      };
      controller.signal.addEventListener('abort', () => fail(new Error('SSE stream aborted')), { once: true });
      // A stream that ends (or dies) before `ready` never connected. After
      // `ready` both callbacks are no-ops: the consume loop owns the stream.
      consuming.then(() => fail(new Error('SSE stream ended before ready')), fail);
    });
    this.active = true;
    this.emit('open');
  }

  async #consume(body) {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        for (const event of this.#parser.push(decoder.decode(value, { stream: true }))) {
          this.#receive(event);
        }
      }
    } catch (error) {
      // A stream that is an error is let go of: the server stops sending
      // into a body nobody reads. Best effort — it may be gone already.
      void reader.cancel().catch(() => {});
      throw error;
    } finally {
      this.#down();
    }
  }

  #receive(event) {
    if (event.id !== null) this.#lastEventId = event.id;
    if (event.event === 'gap') {
      // The server no longer holds what this channel missed: resuming would
      // silently skip frames. Drop the channel state and start over — the
      // client above re-loads and re-subscribes, and each subscription
      // resumes (or honestly refuses to) from its own lastEventId. Said out
      // loud: this is event loss, not routine reconnection.
      // Without the channel: the reference holds its credential.
      this.log?.warn({ event: 'sse.gap' });
      this.#channel = null;
      this.#lastEventId = null;
      return void this.close();
    }
    if (event.event === 'ready') {
      const ready = JSON.parse(event.data);
      // The server mints the reference; this frame is the only place it
      // is learned.
      if (ready.channel) this.#channel = ready.channel;
      if (this.#onReady) {
        this.#onReady();
        this.#onReady = null;
      }
      return;
    }
    this.emit('message', event.data);
  }

  #down() {
    if (!this.active) return;
    this.active = false;
    this.emit('close');
  }

  close() {
    if (!this.active && !this.#controller) return;
    const controller = this.#controller;
    this.#controller = null;
    this.#down();
    controller?.abort();
  }

  terminate() {
    this.close();
  }

  write(data) {
    if (!this.active || this.#channel === null) throw new Error('Not connected');
    // Only a binary packet codec gets here with bytes: revision 1 sends a
    // packet's bytes as JSON.
    if (typeof data !== 'string') throw new ConfigError('SSE carries text only: a binary codec needs a WebSocket');
    const headers = {
      ...this.#headers,
      ...this.#meta,
      'Content-Type': this.codec?.contentType ?? 'application/json',
      [CHANNEL_HEADER]: this.#channel,
    };
    const doFetch = this.#fetch;
    const post = async () => {
      const response = await doFetch(this.url, { method: 'POST', headers, body: data });
      // 202 is the expected answer: everything a call produces comes back
      // on the event stream, not in this response.
      if (response.status === 202) {
        await response.body?.cancel?.();
        return;
      }
      const text = await response.text();
      if (response.ok) return void (text && this.emit('message', text));
      // 409: the channel died server-side (retention, restart, another
      // instance). This stream is now an orphan — close it so the client
      // above reconnects and starts a fresh channel (the 'close' settles
      // everything in flight).
      if (response.status === 409) return void this.close();
      // A refused POST answers nothing on the event stream, so the exact
      // calls this request carried settle now instead of waiting out
      // callTimeout — same contract as the plain HTTP transport.
      this.emit('error', new Error(`SSE post failed with ${response.status}: ${text}`));
      this.failPackets(data, response.status);
    };
    post().catch((error) => {
      this.emit('error', error);
      // A sealed POST refused in plaintext: the sealing layer's closed set
      // of statuses (400, 409, 426) reaches the calls, a 409 from it never
      // closes the channel (that branch above is the plain transport's
      // "channel gone"), anything else is a 503.
      this.failPackets(data, refusedStatus(error));
    });
    return true;
  }
}

WrpcClient.transport.sse = ClientSseTransport;

module.exports = { ClientSseTransport, SseParser, CHANNEL_HEADER };
