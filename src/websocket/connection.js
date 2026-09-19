'use strict';

const { EventEmitter } = require('node:events');
const { createLoggerWriter } = require('../logging.js');

const crypto = require('node:crypto');

const { OPCODES, CLOSE_CODES, RSV1 } = require('./constants.js');
const { Frame, EMPTY_PING, EMPTY_PONG, encodeFrame, encodeFrameFrom } = require('./frame.js');
const { FrameParser, isValidUTF8 } = require('./frameParser.js');
const { SegmentQueue } = require('./segments.js');
const { PreparedFrames } = require('./prepared.js');
const { DeflateContext } = require('./deflateContext.js');
const permessageDeflate = require('./permessageDeflate.js');

// The write-queue pressure signal when a socket does not say (a test
// double): net.Socket's own default.
const DEFAULT_HIGH_WATER_MARK = 16 * 1024;

const MAX_BUFFER = 1024 * 1024 * 100;
// Up to this payload size a data frame is encoded as ONE contiguous buffer
// (header + payload) and written once; above it the payload is written as
// its own chunk behind a separate header, because copying a large payload
// costs more than the second write it saves (bench/send-path.js).
const SINGLE_WRITE_MAX = 16 * 1024;
// Module-wide utf8 scratch for sendText: written and copied out within one
// synchronous call, so sharing it between connections is safe. 48 KiB
// covers a 16 K-char message at the 3-bytes-per-unit worst case, i.e.
// every message that can take the single-write path.
const SCRATCH_SIZE = 48 * 1024;
const SCRATCH = Buffer.allocUnsafeSlow(SCRATCH_SIZE);
// The inflated-size cap for permessage-deflate, separate from MAX_BUFFER on
// purpose: MAX_BUFFER bounds bytes that already crossed the wire, while a
// compression bomb turns a few KB on the wire into whatever this allows —
// so its default has to be small enough to survive, not merely "large".
const MAX_PAYLOAD = 1024 * 1024 * 16;
const CLOSE_TIMEOUT = 1000;
// How long the answering side waits for the peer's FIN before destroying.
const CLOSE_GRACE = 200;
const MAX_HEADER_SIZE = 14; // 2 base + 8 extended length + 4 mask key

class Connection extends EventEmitter {
  #socket;
  #log;
  #isClient;
  #queue = new SegmentQueue();
  #pendingHeader = null;
  #maxBuffer;
  #maxPayload;
  #maxBackpressure;
  #fragmentThreshold;
  #deflate;
  #allowedRsv = 0;
  #closeTimeout;
  #closing = false;
  #closeSent = false;
  #closeReceived = false;
  #fragments = null;
  #closeTimer = null;
  #needsDrain = false;
  #paused = false;
  #closeCode = CLOSE_CODES.CONNECTION_CLOSED_ABNORMALLY;
  #closeReason = '';
  // Set when WE failed the connection (a protocol error, an oversized or
  // undecodable message): RFC 6455 7.1.7 — nothing the peer sends after
  // that is acted on except its Close, which lets us hang up sooner. A
  // ping in particular is not answered any more (Autobahn 4.1.3–4.2.5).
  #failed = false;
  // Context takeover (a live zlib stream per direction) when negotiated.
  #context = null;
  // Ordering queues around an asynchronous deflate/inflate — context
  // takeover, or `async` past its threshold. Outbound: every write issued
  // while a compress is in flight waits behind it, so frames leave in send
  // order; `#outboxBytes` counts them into bufferedAmount so backpressure
  // stays honest while they wait. Inbound: a message decoded off the loop
  // is delivered in arrival order, the ones behind it held until it lands.
  #outbox = [];
  #outboxBytes = 0;
  #draining = false;
  #inbox = [];
  #coalesce;
  #corked = false;
  // Write coalescing: the first write of an event-loop turn corks the socket
  // and this uncorks it on the next tick, so every frame produced in that
  // turn — the N callbacks of a batch, a burst of events — leaves in ONE
  // writev instead of N syscalls. The idiom node:http uses. The `send()`
  // boolean stays honest: a corked write still reports writableLength
  // against the high-water mark, and 'drain' is unchanged.
  #uncork = () => {
    this.#corked = false;
    if (!this.#socket.destroyed) this.#socket.uncork();
  };

  constructor(socket, head, options = {}) {
    super();
    this.#socket = socket;

    const {
      isClient = false,
      maxBuffer = MAX_BUFFER,
      maxPayload = MAX_PAYLOAD,
      closeTimeout = CLOSE_TIMEOUT,
      // Finite by default: an unbounded outbound buffer lets a peer that
      // stops reading (or floods pings, each answered with a pong) grow the
      // socket's write queue without limit. `maxBuffer` is the ceiling —
      // the connection can hold one biggest-allowed message in flight —
      // and 0 opts back into unbounded.
      maxBackpressure = maxBuffer,
      fragmentThreshold = 0,
      protocol = '',
      deflate = null,
      // Off for a bare Connection so a write is on the socket the moment
      // send() returns; WebsocketServer turns it on for the connections it
      // creates (a server answering many peers is where bursts happen).
      coalesce = false,
      // The writer WebsocketServer hands down, already childed with this
      // connection's peer. A bare Connection gets the disabled one, whose
      // methods are frozen no-ops — so the guarded paths below cost nothing
      // when nobody is listening.
      logger = false,
    } = options;
    this.#log = createLoggerWriter(logger);
    this.#isClient = isClient;
    this.#maxBuffer = maxBuffer;
    this.#maxPayload = maxPayload;
    this.#closeTimeout = closeTimeout;
    this.#maxBackpressure = maxBackpressure;
    this.#fragmentThreshold = fragmentThreshold;
    this.#deflate = deflate;
    if (deflate && (deflate.serverTakeover || deflate.clientTakeover)) {
      this.#context = new DeflateContext({
        windowBits: deflate.windowBits,
        level: deflate.level,
        memLevel: deflate.memLevel,
        server: deflate.serverTakeover,
        client: deflate.clientTakeover,
      });
    }
    this.#coalesce = coalesce && typeof socket.cork === 'function';
    if (deflate) this.#allowedRsv = RSV1;
    this.protocol = protocol;
    this.#init(head);
  }

  get bufferedAmount() {
    return (this.#socket.writableLength ?? 0) + this.#outboxBytes;
  }

  #highWaterMark() {
    return this.#socket.writableHighWaterMark ?? DEFAULT_HIGH_WATER_MARK;
  }

  get remoteAddress() {
    return this.#socket.remoteAddress;
  }

  #init(head) {
    this.#socket.on('data', (data) => this.#receive(data));
    this.#socket.on('drain', () => {
      if (!this.#needsDrain) return;
      // Bytes still waiting behind a compress keep the pressure on; the
      // queue pump announces the drain once they have left.
      if (this.#outboxBytes > 0 && this.bufferedAmount >= this.#highWaterMark()) return;
      this.#needsDrain = false;
      this.emit('drain');
    });
    this.#socket.on('error', (error) => {
      if (this.#socket.destroyed) return;
      this.emit('error', error);
      this.terminate();
    });
    this.#socket.on('close', () => {
      if (this.#closeTimer) clearTimeout(this.#closeTimer);
      this.emit('close', this.#closeCode, this.#closeReason);
    });

    // received data before upgrade
    if (head && head.length > 0) this.#receive(head);
  }

  // Receive-side flow control: stops/restarts socket reads so a slow
  // consumer propagates pressure down to the peer through TCP. While
  // paused, inbound pongs are not read either — liveness checks must not
  // treat a paused connection as dead (see the heartbeat's isPaused skip).
  get isPaused() {
    return this.#paused;
  }

  pause() {
    if (this.#socket.destroyed) return;
    this.#paused = true;
    this.#socket.pause();
  }

  resume() {
    if (this.#socket.destroyed) return;
    this.#paused = false;
    this.#socket.resume();
  }

  #receive(data) {
    if (this.#closeReceived && this.#closing) return;

    this.#queue.push(data);

    if (this.#queue.length > this.#maxBuffer) {
      const error = new Error('Buffer overflow, closing connection');
      this.#fault('ws.overflow', error, { queued: this.#queue.length, max: this.#maxBuffer });
      if (this.#isClient) {
        this.#failed = true;
        return void this.sendClose(CLOSE_CODES.MESSAGE_TOO_BIG, 'Message too big');
      }
      const frame = Frame.errorClose('MESSAGE_TOO_BIG');
      return void this.#fail(frame);
    }

    this.#processFrames();
  }

  #processFrames() {
    while (true) {
      // RFC 6455 5.5.2 exempts an endpoint from answering a Ping received
      // after a Close, and #answerClose has already half-closed the socket —
      // so anything pipelined behind the peer's Close in the same segment is
      // not just pointless to handle, it would write past end().
      if (this.#closeReceived) break;
      if (!this.#pendingHeader) {
        const headerBytes = this.#queue.peek(MAX_HEADER_SIZE);
        const result = FrameParser.parseHeader(headerBytes, { allowedRsv: this.#allowedRsv });
        const { value, error } = result;
        if (error) return void this.#processFrameParserError(error);
        if (!value) break;

        if (!this.#isClient && !value.masked) {
          const closeFrame = Frame.protocolErrorClose('UNMASKED');
          return void this.#fail(closeFrame);
        }
        if (this.#isClient && value.masked) {
          const closeFrame = Frame.protocolErrorClose('MASKED', this.#isClient);
          return void this.#fail(closeFrame);
        }
        // A frame that can never fit is rejected on its header, before the
        // payload is buffered.
        if (!this.#trackMessageSize(value.length)) return;

        this.#queue.consume(value.headerSize);
        this.#pendingHeader = value;
      }

      const header = this.#pendingHeader;
      if (this.#queue.length < header.length) break;
      this.#pendingHeader = null;

      const payload = this.#queue.consume(header.length);
      const frame = new Frame(header.fin, header.opcode, header.masked, payload, header.mask, header.rsv);
      if (frame.masked) frame.unmaskPayload();
      if (this.#closing && !frame.isControlFrame) continue;
      if (frame.isControlFrame) {
        this.#processControlFrame(frame);
      } else {
        this.#processDataFrame(frame);
      }
    }
  }

  #processControlFrame(frame) {
    const { error } = FrameParser.checkControlFrame(frame);
    if (error) return void this.#processFrameParserError(error);

    const { opcode } = frame;
    if (opcode === OPCODES.PING) {
      if (this.#failed) return;
      this.emit('ping', frame.payload);
      return void this.sendPong(frame.payload);
    }
    if (opcode === OPCODES.PONG) return void this.emit('pong', frame.payload);
    if (opcode === OPCODES.CLOSE) {
      this.#closeReceived = true;
      const { code, reason } = frame.getCloseDetails().value;
      this.#closeCode = code ?? CLOSE_CODES.NO_CODE_RECEIVED;
      this.#closeReason = reason;
      if (!this.#closeSent) {
        return void this.#answerClose(code, reason);
      }
      return void this.terminate();
    }
  }

  #processDataFrame(frame) {
    const { error } = FrameParser.checkDataFrame(frame);
    if (error) return void this.#processFrameParserError(error);
    this.#handleDataFrame(frame);
  }

  #processFrameParserError(error) {
    const { code } = error;
    const [type, subtype] = code.split('-');
    this.#fault('ws.frame', error, { close: code });
    const frame =
      type === 'PROTOCOL_ERROR'
        ? Frame.protocolErrorClose(subtype, this.#isClient)
        : Frame.errorClose(type, this.#isClient);
    this.#fail(frame);
  }

  #trackMessageSize(size) {
    const tooBig = size > this.#maxBuffer;
    if (tooBig) {
      const error = new Error('Message too big');
      this.#fault('ws.too-big', error, { size, max: this.#maxBuffer });
      if (this.#isClient) {
        this.#failed = true;
        this.sendClose(CLOSE_CODES.MESSAGE_TOO_BIG, 'Message too big');
      } else {
        const frame = Frame.errorClose('MESSAGE_TOO_BIG');
        this.#fail(frame);
      }
    }
    return !tooBig;
  }

  #handleDataFrame(frame) {
    const { opcode, payload } = frame;
    const compressed = (frame.rsv & RSV1) !== 0;
    if (!this.#fragments) {
      // Continuation frame without a started fragmented message
      if (opcode === OPCODES.CONTINUATION) {
        const error = new Error('Protocol error: Unexpected CONTINUATION without start');
        this.#fault('ws.protocol', error);
        const frame = Frame.protocolErrorClose('COMMON', this.#isClient);
        return void this.#fail(frame);
      }
      if (frame.fin) {
        // single frame
        if (compressed) return void this.#emitInflated(opcode, payload);
        const isBinary = opcode === OPCODES.BINARY;
        this.#emitMessage(frame.payload, isBinary);
      } else {
        if (!this.#trackMessageSize(payload.length)) return;
        this.#fragments = {
          opcode,
          compressed,
          payloads: [payload],
          totalSize: payload.length,
        };
      }
    } else if (opcode === OPCODES.CONTINUATION) {
      // continue fragments
      const totalSize = this.#fragments.totalSize + payload.length;
      if (!this.#trackMessageSize(totalSize)) return;
      this.#fragments.totalSize = totalSize;
      this.#fragments.payloads.push(frame.payload);
      if (!frame.fin) return;
      const { opcode: firstOpcode, compressed: wasCompressed } = this.#fragments;
      const fullPayload = Buffer.concat(this.#fragments.payloads);
      this.#fragments = null;
      if (wasCompressed) return void this.#emitInflated(firstOpcode, fullPayload);
      const isBinary = firstOpcode === OPCODES.BINARY;
      const isText = firstOpcode === OPCODES.TEXT;
      if (isText && !isValidUTF8(fullPayload)) {
        const error = new Error('Invalid UTF-8 in text frame');
        this.#fault('ws.invalid-utf8', error);
        const frame = Frame.errorClose('INVALID_PAYLOAD', this.#isClient);
        return void this.#fail(frame);
      }
      this.#emitMessage(fullPayload, isBinary);
    } else {
      const error = new Error('Protocol error: Unexpected data frame during fragments');
      this.#fault('ws.protocol', error);
      const frame = Frame.protocolErrorClose('COMMON', this.#isClient);
      return void this.#fail(frame);
    }
  }

  // A message decoded synchronously while others are still inflating off
  // the loop waits behind them: the inbox is what keeps arrival order.
  #emitMessage(data, isBinary) {
    if (this.#inbox.length > 0) {
      this.#inbox.push({ done: true, error: null, data, isBinary, isText: false });
      return;
    }
    this.emit('message', data, isBinary);
  }

  #emitInflated(opcode, payload) {
    // The dedicated inflated-size cap. zlib's maxOutputLength stops the
    // inflation the moment output would exceed it, so a compression bomb
    // costs at most `maxPayload` of memory and CPU before the close.
    const limit = Math.min(this.#maxPayload, this.#maxBuffer);
    const isBinary = opcode === OPCODES.BINARY;
    const isText = opcode === OPCODES.TEXT;
    const deflate = this.#deflate;
    const viaContext = this.#context !== null && deflate.clientTakeover === true;
    const async = deflate.async ?? null;
    if (viaContext || (async !== null && payload.length >= async.threshold)) {
      const item = { done: false, error: null, data: null, isBinary, isText };
      this.#inbox.push(item);
      const settle = (error, inflated) => {
        item.done = true;
        item.error = error ?? null;
        item.data = inflated ?? null;
        this.#deliver();
      };
      if (viaContext) this.#context.decompress(payload, limit, settle);
      else permessageDeflate.decompressAsync(payload, limit, settle);
      return;
    }
    let inflated = null;
    try {
      inflated = permessageDeflate.decompress(payload, limit);
    } catch (error) {
      return void this.#failInflate(error);
    }
    if (isText && !isValidUTF8(inflated)) {
      const error = new Error('Invalid UTF-8 in text frame');
      this.#fault('ws.invalid-utf8', error);
      return void this.#fail(Frame.errorClose('INVALID_PAYLOAD', this.#isClient));
    }
    this.#emitMessage(inflated, isBinary);
  }

  // Every way this connection dies of its own accord used to be an 'error'
  // event and nothing else. A server rarely listens for 'error' on an
  // individual socket, so the operator's view was a connection that simply
  // vanished — the classic "it just disconnects sometimes" report, with
  // nothing in the log to work from.
  //
  // `warn`, not `error`: a peer sending a bad frame or overrunning a limit
  // is that peer's problem, and a server with many of them must not have its
  // log dominated by them. The limits (maxPayload, maxBuffer,
  // maxBackpressure) are the operator's to raise, which is exactly why they
  // have to be told the limit was what closed the connection.
  #fault(event, error, extra = null) {
    this.#log.warn({ ...extra, err: error, event });
    this.emit('error', error);
  }

  #failInflate(error) {
    const type = error.code === 'ERR_BUFFER_TOO_LARGE' ? 'MESSAGE_TOO_BIG' : 'INVALID_PAYLOAD';
    this.#fault('ws.inflate', error, { close: type });
    this.#fail(Frame.errorClose(type, this.#isClient));
  }

  // Delivers inbox messages in order as far as they have landed.
  #deliver() {
    const inbox = this.#inbox;
    while (inbox.length > 0 && inbox[0].done) {
      const item = inbox.shift();
      if (this.#closing) continue;
      if (item.error !== null) return void this.#failInflate(item.error);
      if (item.isText && !isValidUTF8(item.data)) {
        const error = new Error('Invalid UTF-8 in text frame');
        this.#fault('ws.invalid-utf8', error);
        return void this.#fail(Frame.errorClose('INVALID_PAYLOAD', this.#isClient));
      }
      this.emit('message', item.data, item.isBinary);
    }
  }

  // `options.compress === false` sends this one message uncompressed even
  // when deflate was negotiated and the payload clears the threshold.
  send(data, options = null) {
    if (typeof data === 'string') return this.sendText(data, options);
    if (Buffer.isBuffer(data)) return this.sendBinary(data, options);
    throw new TypeError('send() accepts only string or Buffer');
  }

  // A message prepared for fan-out: `{ text, frames, compress }` where
  // `frames` is the engine-owned cache slot (see prepared.js). The slot is
  // claimed when empty and reused when it already holds our own cache; a
  // slot another engine claimed means a mixed room, and the text goes the
  // ordinary way. Client connections and fragmenting ones do too — a client
  // frame needs a fresh mask, and a fragmented one is not one buffer.
  sendPrepared(message) {
    if (this.#closing) return false;
    if (this.#exceedsBackpressure()) return false;
    if (this.#isClient || this.#fragmentThreshold) return this.send(message.text, message);
    let frames = message.frames;
    if (frames === null) {
      frames = message.frames = new PreparedFrames(message.text);
    } else if (!(frames instanceof PreparedFrames)) {
      return this.send(message.text, message);
    }
    const deflate = this.#deflate;
    if (deflate !== null && message.compress !== false && frames.length >= deflate.threshold) {
      // Context takeover is per connection by construction: the shared
      // frame cannot serve it, the shared utf8 payload still can.
      if (this.#context !== null && deflate.serverTakeover === true) {
        return this.#enqueueCompress(frames.opcode, frames.payload, null);
      }
      const async = deflate.async ?? null;
      if (async !== null && frames.length >= async.threshold) {
        return this.#enqueueCompress(frames.opcode, frames.payload, frames);
      }
      return this.#write(frames.deflated(deflate.windowBits));
    }
    return this.#write(frames.plain());
  }

  #write(buffer) {
    // Behind an in-flight compress every write waits its turn (the pump
    // writes with #draining set, so its own frames pass through).
    if (this.#outbox.length > 0 && !this.#draining) {
      return this.#enqueue({ buffer, payload: null, shared: null, opcode: 0, started: false });
    }
    if (this.#coalesce && !this.#corked) {
      this.#corked = true;
      this.#socket.cork();
      process.nextTick(this.#uncork);
    }
    const ok = this.#socket.write(buffer);
    if (!ok) this.#needsDrain = true;
    return ok;
  }

  #writeFrame(frame) {
    if (this.#isClient) frame.maskPayload();
    this.#socket.cork();
    this.#write(frame.header);
    const ok = this.#write(frame.payload);
    this.#socket.uncork();
    return ok;
  }

  #exceedsBackpressure() {
    if (!this.#maxBackpressure) return false;
    if (this.bufferedAmount <= this.#maxBackpressure) return false;
    const error = new Error('Backpressure limit exceeded, terminating connection');
    this.#fault('ws.backpressure', error, { buffered: this.bufferedAmount, max: this.#maxBackpressure });
    this.terminate();
    return true;
  }

  #shouldCompress(length, options) {
    const deflate = this.#deflate;
    return deflate !== null && length >= deflate.threshold && (options === null || options.compress !== false);
  }

  #sendData(opcode, payload, options) {
    let rsv = 0;
    if (this.#shouldCompress(payload.length, options)) {
      const deflate = this.#deflate;
      const async = deflate.async ?? null;
      const takeover = this.#context !== null && deflate.serverTakeover === true;
      if (takeover || (async !== null && payload.length >= async.threshold)) {
        return this.#enqueueCompress(opcode, payload, null);
      }
      payload = permessageDeflate.compress(payload, deflate.windowBits);
      rsv = RSV1;
    }
    return this.#emitFrames(opcode, payload, rsv);
  }

  // --- The outbound ordering queue ---------------------------------------

  #enqueue(item) {
    this.#outbox.push(item);
    this.#outboxBytes += item.buffer !== null ? item.buffer.length : item.payload.length;
    const ok = this.bufferedAmount < this.#highWaterMark();
    if (!ok) this.#needsDrain = true;
    return ok;
  }

  // A message whose deflate runs off the loop (context takeover, or
  // `async` past its threshold; `shared` is a fan-out's PreparedFrames
  // whose one deflate every recipient waits on). Queued behind whatever
  // is already waiting, then the pump starts it.
  #enqueueCompress(opcode, payload, shared) {
    const ok = this.#enqueue({ buffer: null, payload, shared, opcode, started: false });
    this.#pump();
    return ok;
  }

  #pump() {
    const outbox = this.#outbox;
    while (outbox.length > 0) {
      const head = outbox[0];
      if (head.buffer !== null) {
        outbox.shift();
        this.#outboxBytes -= head.buffer.length;
        this.#draining = true;
        this.#write(head.buffer);
        this.#draining = false;
        continue;
      }
      if (head.started) return;
      head.started = true;
      return void this.#startCompress(head);
    }
    // Everything queued has reached the socket: if the queue was what held
    // the pressure, say so now; a socket above its own mark says it later.
    if (this.#needsDrain && (this.#socket.writableLength ?? 0) < this.#highWaterMark()) {
      this.#needsDrain = false;
      this.emit('drain');
    }
  }

  #startCompress(job) {
    const finish = (error, compressed, frame) => {
      // The connection went away meanwhile: the queue was dropped with it.
      if (this.#outbox[0] !== job) return;
      if (error) {
        this.emit('error', error);
        return void this.terminate();
      }
      this.#outbox.shift();
      this.#outboxBytes -= job.payload.length;
      this.#draining = true;
      if (frame !== null) this.#write(frame);
      else this.#emitFrames(job.opcode, compressed, RSV1);
      this.#draining = false;
      this.#pump();
    };
    if (job.shared !== null) {
      job.shared.deflatedAsync(this.#deflate.windowBits, (error, frame) => finish(error, null, frame));
    } else if (this.#context !== null && this.#deflate.serverTakeover === true) {
      this.#context.compress(job.payload, (error, compressed) => finish(error, compressed, null));
    } else {
      permessageDeflate.compressAsync(job.payload, this.#deflate.windowBits, (error, compressed) =>
        finish(error, compressed, null),
      );
    }
  }

  #dropQueues() {
    this.#outbox.length = 0;
    this.#outboxBytes = 0;
    this.#inbox.length = 0;
    if (this.#context !== null) this.#context.close();
  }

  // Frames an already-compressed (or plain) payload: one contiguous buffer
  // up to SINGLE_WRITE_MAX, header + payload above it, fragments when a
  // fragmentThreshold asks for them.
  #emitFrames(opcode, payload, rsv) {
    const threshold = this.#fragmentThreshold;
    if (!threshold || payload.length <= threshold) {
      if (payload.length <= SINGLE_WRITE_MAX) {
        const mask = this.#isClient ? crypto.randomBytes(4) : null;
        return this.#write(encodeFrame(opcode, rsv, payload, mask));
      }
      return this.#writeFrame(new Frame(true, opcode, false, payload, null, rsv));
    }
    let ok = true;
    // One cork for the WHOLE fragmented message: per-frame cork/uncork
    // (inside #writeFrame) flushed a TCP write per fragment; nested corks
    // are ref-counted, so this outer pair batches them into one flush.
    this.#socket.cork();
    for (let offset = 0; offset < payload.length; offset += threshold) {
      const end = Math.min(offset + threshold, payload.length);
      const fin = end === payload.length;
      const op = offset === 0 ? opcode : OPCODES.CONTINUATION;
      const frameRsv = offset === 0 ? rsv : 0;
      ok = this.#writeFrame(new Frame(fin, op, false, payload.subarray(offset, end), null, frameRsv));
    }
    this.#socket.uncork();
    return ok;
  }

  // Data send methods return false when the socket did not take the bytes
  // without exceeding its high-water mark — wait for 'drain' before more.
  sendText(message, options = null) {
    if (this.#closing) return false;
    if (this.#exceedsBackpressure()) return false;
    // A message that fits the scratch buffer even at 3 bytes per UTF-16
    // unit is utf8-encoded ONCE into it; the frame then copies those bytes
    // behind its header. Encoding straight into the frame would need
    // Buffer.byteLength first — a second pass over the string that costs
    // more than the memcpy it saves at typical packet sizes, and
    // Buffer.from + a separate header costs an allocation and two writes
    // (bench/send-path.js, "sendText 200 B").
    if (message.length * 3 <= SCRATCH_SIZE) {
      const length = SCRATCH.write(message, 0, 'utf8');
      const threshold = this.#fragmentThreshold;
      if (length <= SINGLE_WRITE_MAX && (!threshold || length <= threshold) && !this.#shouldCompress(length, options)) {
        const mask = this.#isClient ? crypto.randomBytes(4) : null;
        return this.#write(encodeFrameFrom(OPCODES.TEXT, 0, SCRATCH, length, mask));
      }
      // Compressed, fragmented or large: a copy the scratch can outlive.
      return this.#sendData(OPCODES.TEXT, Buffer.from(SCRATCH.subarray(0, length)), options);
    }
    return this.#sendData(OPCODES.TEXT, Buffer.from(message, 'utf8'), options);
  }

  sendBinary(buffer, options = null) {
    if (this.#closing) return false;
    if (this.#exceedsBackpressure()) return false;
    if (!Buffer.isBuffer(buffer)) buffer = Buffer.from(buffer);
    return this.#sendData(OPCODES.BINARY, buffer, options);
  }

  sendPing(payload) {
    if (this.#closing) return false;
    if (this.#exceedsBackpressure()) return false;
    if (payload) return this.#writeFrame(Frame.ping(payload));
    return this.#fastPing();
  }

  // No #closing guard: RFC 6455 5.5.3 requires a pong unless a Close frame
  // was received, and #processFrames stops draining input once
  // #closeReceived is set — including frames already queued behind the Close.
  //
  // The backpressure guard DOES apply: a peer that floods pings without
  // reading the pongs grows the write queue without limit, and RFC
  // compliance is not a suicide pact — past the cap the connection is
  // terminated like any other non-reading peer.
  sendPong(payload) {
    if (this.#exceedsBackpressure()) return false;
    if (payload) return this.#writeFrame(Frame.pong(payload));
    return this.#fastPong();
  }

  #fastPing() {
    const buf = this.#isClient ? Frame.emptyClientPingBuffer() : EMPTY_PING;
    return this.#write(buf);
  }

  #fastPong() {
    const buf = this.#isClient ? Frame.emptyClientPongBuffer() : EMPTY_PONG;
    return this.#write(buf);
  }

  // A close WE initiated because the peer broke the protocol: the close
  // frame goes out and the connection is failed (see #failed).
  #fail(frameBuffer) {
    this.#failed = true;
    this.#close(frameBuffer);
  }

  #close(frameBuffer) {
    if (this.#closing) return;
    this.#closing = true;
    this.#closeSent = true;
    this.#fragments = null;
    this.#dropQueues();

    this.#socket.write(frameBuffer);

    if (this.#closeTimer) {
      clearTimeout(this.#closeTimer);
    }
    this.#closeTimer = setTimeout(() => {
      this.#socket.end();
      setTimeout(() => {
        this.#socket.destroy();
      }, 200);
    }, this.#closeTimeout);
  }

  // Answers a peer-initiated Close and hangs up.
  //
  // RFC 6455 5.5.1: the side ANSWERING a Close closes the TCP connection
  // immediately — it does not wait for the initiator to do it. Arming the
  // full close timeout here instead left both peers waiting for the other
  // to hang up, so every graceful disconnect cost `closeTimeout` (a second
  // of latency on top of each reconnect, and on every test teardown).
  //
  // end() rather than destroy(): the echo has to flush before the FIN, and
  // the short grace timer only covers a peer that never answers the FIN.
  #answerClose(code = 1000, reason = '') {
    if (this.#closing) return;
    this.#closing = true;
    this.#closeSent = true;
    this.#fragments = null;
    this.#dropQueues();
    const frame = Frame.close(code, reason);
    if (this.#isClient) frame.maskPayload();
    this.#socket.write(frame.toBuffer());
    this.#socket.end();
    if (this.#closeTimer) clearTimeout(this.#closeTimer);
    this.#closeTimer = setTimeout(() => {
      this.#socket.destroy();
    }, CLOSE_GRACE);
  }

  sendClose(code = 1000, reason = '') {
    const frame = Frame.close(code, reason);
    if (this.#isClient) frame.maskPayload();
    this.#close(frame.toBuffer());
  }

  // WrpcSocket engine-contract alias for sendClose
  close(code, reason) {
    this.sendClose(code, reason);
  }

  terminate() {
    if (this.#closeTimer) {
      clearTimeout(this.#closeTimer);
      this.#closeTimer = null;
    }
    this.#dropQueues();
    if (!this.#socket.destroyed) this.#socket.destroy();
  }
}

module.exports = { Connection, CLOSE_TIMEOUT, MAX_PAYLOAD };
