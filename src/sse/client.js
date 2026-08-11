'use strict';

const { WrpcClient, ClientTransport } = require('../client.js');
const { generateUUID } = require('../runtime/node.js');

// The client half of the SSE transport. Browser-safe: `fetch`, streams and
// TextDecoder only — no node builtins, and deliberately not `EventSource`,
// which cannot set headers, cannot be aborted cleanly, and reconnects on its
// own schedule instead of the client's.
//
//   require('@alexify/wrpc/sse');                       // registers it
//   await WrpcClient.connect(url, { transport: 'sse' });
//
// Calls go out as POSTs; every answer comes back down the one event stream.

const CHANNEL_HEADER = 'x-wrpc-channel';

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

  constructor(url) {
    super(url);
    this.channelId = generateUUID();
  }

  get eventsUrl() {
    return `${joinUrl(this.url, '/events')}?channel=${encodeURIComponent(this.channelId)}`;
  }

  async open() {
    if (this.active) return;
    if (this.#reading) return this.#reading;
    const controller = new AbortController();
    this.#controller = controller;
    this.#parser = new SseParser();
    const headers = { accept: 'text/event-stream' };
    // The native resume header: on a re-open the server replays what this
    // channel did not acknowledge.
    if (this.#lastEventId !== null) headers['last-event-id'] = this.#lastEventId;
    const opening = (async () => {
      const response = await fetch(this.eventsUrl, { headers, signal: controller.signal, cache: 'no-store' });
      if (!response.ok || !response.body) {
        throw new Error(`SSE stream refused with ${response.status}`);
      }
      this.active = true;
      this.emit('open');
      void this.#consume(response.body).catch((error) => {
        if (!controller.signal.aborted) this.emit('error', error);
      });
    })();
    this.#reading = opening;
    try {
      await opening;
    } finally {
      this.#reading = null;
    }
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
    if (event.event === 'ready') {
      const ready = JSON.parse(event.data);
      // The server may hand back a different channel than requested.
      if (ready.channel) this.#channel = ready.channel;
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
    if (!this.active) throw new Error('Not connected');
    const headers = { 'Content-Type': 'application/json', [CHANNEL_HEADER]: this.#channel ?? this.channelId };
    const post = async () => {
      const response = await fetch(this.url, { method: 'POST', headers, body: data });
      // 202 is the expected answer: everything a call produces comes back
      // on the event stream, not in this response.
      if (response.status === 202) return void (await response.body?.cancel?.());
      const text = await response.text();
      if (response.ok) return void (text && this.emit('message', text));
      this.emit('error', new Error(`SSE post failed with ${response.status}: ${text}`));
    };
    post().catch((error) => this.emit('error', error));
    return true;
  }
}

WrpcClient.transport.sse = ClientSseTransport;

module.exports = { ClientSseTransport, SseParser, CHANNEL_HEADER };
