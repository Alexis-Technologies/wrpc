'use strict';

const { WrpcClient, ClientTransport, metaHeaders } = require('../client.js');
const { CHANNEL_HEADER } = require('./constants.js');

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
class SseParser {
  #buffer = '';
  #data = [];
  #event = '';
  #id = null;

  /** Feeds a chunk and returns the events it completed. */
  push(chunk) {
    this.#buffer += chunk;
    const events = [];
    while (true) {
      const end = this.#lineEnd();
      if (end < 0) break;
      const [line, next] = end;
      const raw = this.#buffer.slice(0, line);
      this.#buffer = this.#buffer.slice(next);
      const event = this.#line(raw);
      if (event) events.push(event);
    }
    return events;
  }

  // Returns [lineLength, consumed] for the first complete line, honouring
  // \n, \r and \r\n — and refusing to split a \r that might still be the
  // first half of a \r\n in the next chunk.
  #lineEnd() {
    for (let i = 0; i < this.#buffer.length; i++) {
      const char = this.#buffer[i];
      if (char === '\n') return [i, i + 1];
      if (char !== '\r') continue;
      if (i + 1 === this.#buffer.length) return -1; // might be \r\n, wait
      return [i, this.#buffer[i + 1] === '\n' ? i + 2 : i + 1];
    }
    return -1;
  }

  #line(line) {
    if (line === '') return this.#dispatch();
    if (line.startsWith(':')) return null; // comment (the proxy heartbeat)
    const colon = line.indexOf(':');
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? '' : line.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    if (field === 'data') this.#data.push(value);
    else if (field === 'event') this.#event = value;
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
    this.#event = '';
    return event;
  }
}

const joinUrl = (base, path) => (base.endsWith('/') ? base.slice(0, -1) : base) + path;

class ClientSseTransport extends ClientTransport {
  // The stream can die without a close frame just like a socket can.
  heartbeat = true;

  #controller = null;
  #channel = null;
  #lastEventId = null;
  #parser = null;
  #reading = null;
  #onReady = null;
  // Connection-phase headers and metadata, resolved per open — real request
  // headers on both the stream GET and every packet POST.
  #headers = null;
  #meta = null;

  get eventsUrl() {
    return joinUrl(this.url, '/events');
  }

  async open(options = {}) {
    this.#headers = options.headers ?? null;
    // A header BLOCK, not one encoded value: the client's metaFormat picks
    // the spelling, both legs below just spread the result.
    this.#meta = options.meta ? metaHeaders(options.meta, options.metaPrefixed) : null;
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
    this.#parser = new SseParser();
    // Declared first, wire headers after: the protocol's own always win.
    const headers = { ...this.#headers, ...this.#meta, accept: 'text/event-stream' };
    // A reconnect presents the channel and where it stopped; the server
    // replays what this channel did not acknowledge.
    if (this.#channel !== null) headers[CHANNEL_HEADER] = this.#channel;
    if (this.#lastEventId !== null) headers['last-event-id'] = this.#lastEventId;
    const response = await fetch(this.eventsUrl, { headers, signal: controller.signal, cache: 'no-store' });
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
      this.log?.warn({ event: 'sse.gap', channel: this.#channel });
      this.#channel = null;
      this.#lastEventId = null;
      this.close();
      return;
    }
    if (event.event === 'ready') {
      const ready = JSON.parse(event.data);
      // The server mints the id; this frame is the only place it is learned.
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
    const headers = {
      ...this.#headers,
      ...this.#meta,
      'Content-Type': this.codec?.contentType ?? 'application/json',
      [CHANNEL_HEADER]: this.#channel,
    };
    const post = async () => {
      const response = await fetch(this.url, { method: 'POST', headers, body: data });
      // 202 is the expected answer: everything a call produces comes back
      // on the event stream, not in this response.
      if (response.status === 202) return void (await response.body?.cancel?.());
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
      this.failPackets(data, 503);
    });
    return true;
  }
}

WrpcClient.transport.sse = ClientSseTransport;

module.exports = { ClientSseTransport, SseParser, CHANNEL_HEADER };
