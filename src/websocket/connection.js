'use strict';

const { EventEmitter } = require('node:events');
const { createLoggerWriter } = require('../logging.js');

const crypto = require('node:crypto');

const { OPCODES, CLOSE_CODES, RSV1 } = require('./constants.js');
const { Frame, EMPTY_PING, EMPTY_PONG, encodeFrame, encodeFrameFrom } = require('./frame.js');
const { FrameParser, isValidUTF8, PARSE_ERR_CODES, ParseError } = require('./frame-parser.js');
const { SegmentQueue } = require('./segments.js');
const { PreparedFrames } = require('./prepared.js');
const { DeflateContext } = require('./deflate-context.js');
const permessageDeflate = require('./permessage-deflate.js');
const { Deque } = require('../deque.js');
const { ConfigError, ArgumentError, TransportError } = require('../errors.js');

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

// The inflate queue's marks (messages): held at INBOX_HIGH — INBOX_ASYNC_HIGH
// on the threadpool path, where inflates run in parallel — and taken up
// again at INBOX_LOW. Bytes are bounded by maxPayload alongside.
const INBOX_HIGH = 32;
const INBOX_ASYNC_HIGH = 4;
const INBOX_LOW = 8;

// What ONE connection may hand to its listeners in one turn of the event
// loop before the rest of what it sent waits for the next: this many
// messages, or this many of their bytes (after inflation — what a listener
// is given, not what crossed the wire). libuv reads a socket up to 32 times
// in one iteration, 64 KiB each, and every frame in those two megabytes was
// dispatched before a timer or another connection had a turn: 40,000
// pipelined calls made a timer 567 ms late with permessage-deflate on (a
// synchronous deflate per answer), and 100 frames of 16 KiB that inflate to
// 16 MiB each made it 229 ms late — 14 ms and 2 ms under these marks, the
// bursts themselves served as fast as before (bench/ws-fairness.js). A batch frame carries up to 128 calls, which is
// why bytes count beside messages.
const TURN_BUDGET = Object.freeze({ messages: 512, bytes: 256 * 1024 });

// The close frame a ParseError's code ends the connection with: 1009 and
// 1007 for theirs, 1002 with the reason that names the violation for the
// rest.
const parseErrorClose = (code, isClient) => {
  switch (code) {
    case PARSE_ERR_CODES.MESSAGE_TOO_BIG:
      return Frame.errorClose('MESSAGE_TOO_BIG', isClient);
    case PARSE_ERR_CODES.INVALID_PAYLOAD:
      return Frame.errorClose('INVALID_PAYLOAD', isClient);
    case PARSE_ERR_CODES.PROTOCOL_ERROR_RSV:
      return Frame.protocolErrorClose('RSV', isClient);
    case PARSE_ERR_CODES.PROTOCOL_ERROR_CTRL_TOO_LONG:
      return Frame.protocolErrorClose('CTRL_TOO_LONG', isClient);
    default:
      return Frame.protocolErrorClose('COMMON', isClient);
  }
};

const isMark = (mark) => mark === undefined || (Number.isInteger(mark) && mark > 0);

// `turnBudget`: false (or absent) for none, true for TURN_BUDGET, or
// `{ messages, bytes }` — each a positive integer, either left out to keep
// its default.
const turnBudgetOf = (value) => {
  if (value === undefined || value === null || value === false) return null;
  if (value === true) return TURN_BUDGET;
  if (typeof value !== 'object' || !isMark(value.messages) || !isMark(value.bytes)) {
    throw new ConfigError('Connection: turnBudget must be a boolean or { messages, bytes } of positive integers');
  }
  return { messages: value.messages ?? TURN_BUDGET.messages, bytes: value.bytes ?? TURN_BUDGET.bytes };
};

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
  // Receive-side backpressure of the INFLATE queue: while this many
  // messages (or maxPayload of compressed bytes) are inflating off the
  // loop, the socket is paused — apart from the public pause(), which the
  // application owns — and the frame loop stops, until enough landed. A
  // peer that sent thousands of compressed frames in one segment used to
  // have every one of them inflating at once, each holding its output.
  #inboxBytes = 0;
  #inboxHeld = false;
  // The turn budget (TURN_BUDGET): what this connection has handed over
  // since the last reset, and whether the frame loop is held for it. A hold
  // of its own beside #inboxHeld — the two are lifted by different things
  // (the next turn; a drained inflate queue), and either keeps the socket
  // paused. The counts are reset only where a hold is lifted, so a
  // connection under its budget pays an increment a message and one spare
  // turn per budget, never a timer per turn.
  #turnBudget = null;
  #turnCount = 0;
  #turnBytes = 0;
  #turnHeld = false;
  #nextTurn = null;
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
  //
  // The outbox is a ring, not an array: how long it gets is the bytes
  // `maxBackpressure` admits over the size of a frame, and it is read from
  // its head — over an array a burst of 256,000 small frames waiting behind
  // deflates cost 150 µs each to send, against 30 (bench/ws-outbox.js). It
  // exists only on a connection that can queue at all (context takeover for
  // what we send, or `async`): null elsewhere, where nothing ever waits.
  #outbox = null;
  #outboxBytes = 0;
  #draining = false;
  // The Close frame of a graceful close() that found writes still waiting
  // behind a compress: it is the outbox's tail, and the pump sends it when
  // it gets there (see sendClose).
  #pendingClose = null;
  #inbox = [];
  #coalesce;
  #corked = false;
  // Write coalescing: the first write of an event-loop turn corks the socket
  // and this uncorks it on the next tick, so every frame produced in that
  // turn — the N callbacks of a batch, a burst of events — leaves in ONE
  // writev instead of N syscalls. The idiom node:http uses. The `send()`
  // boolean stays honest: a corked write still reports writableLength
  // against the high-water mark, and 'drain' is unchanged.
  #uncork = null;

  constructor(socket, head, options = {}) {
    super();
    this.#nextTurn = () => this.#startTurn();
    this.#uncork = () => this.#uncorkSocket();
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
      // Off for a bare Connection too — its owner drives it, and a message
      // is delivered inside the 'data' event that carried it; on for the
      // connections WebsocketServer creates, where one peer must not hold
      // the loop every other peer waits on.
      turnBudget = false,
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
    this.#turnBudget = turnBudgetOf(turnBudget);
    this.#deflate = deflate;
    if (deflate && (deflate.serverTakeover || deflate.async)) this.#outbox = new Deque();
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
    // Bytes the peer sent right behind its handshake. Parsed here, a frame
    // among them that failed emitted 'error' from inside this constructor —
    // before anyone could listen, so it was thrown out of the server's
    // 'upgrade' listener, which is the process. Handed back to the socket
    // instead, they are read first through 'data', once the owner of this
    // connection is listening. A socket without `unshift` (a test double)
    // gets them on the next tick — queued before the 'data' listener below
    // resumes the socket, so nothing the socket holds overtakes them.
    if (head && head.length > 0) {
      if (typeof this.#socket.unshift === 'function') this.#socket.unshift(head);
      else process.nextTick(() => this.#receive(head));
    }
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
  }

  // Receive-side flow control: stops/restarts socket reads so a slow
  // consumer propagates pressure down to the peer through TCP. While
  // paused, inbound pongs are not read either — liveness checks must not
  // treat a paused connection as dead (see the heartbeat's isPaused skip).
  get isPaused() {
    return this.#paused || this.#inboxHeld || this.#turnHeld;
  }

  pause() {
    if (this.#socket.destroyed) return;
    this.#paused = true;
    this.#socket.pause();
  }

  resume() {
    if (this.#socket.destroyed) return;
    this.#paused = false;
    // Held for the inflate queue or for the turn: the socket stays paused
    // until that hold is lifted.
    if (!this.#inboxHeld && !this.#turnHeld) this.#socket.resume();
  }

  #holdInbox() {
    if (this.#inboxHeld) return;
    this.#inboxHeld = true;
    if (!this.#socket.destroyed) this.#socket.pause();
  }

  // Enough inflates landed: the socket reads again (unless the application
  // paused it), and the frames already buffered are taken up — no 'data'
  // event will come for those.
  #releaseInbox() {
    this.#inboxHeld = false;
    if (this.#socket.destroyed) return;
    if (!this.#paused && !this.#turnHeld) this.#socket.resume();
    this.#processFrames();
  }

  // Hands a message to the listeners, and counts it against the turn: past
  // the budget the frame loop stops after this message and the socket reads
  // no more until the next turn of the loop takes both up again (#nextTurn).
  // Every delivery goes through here — the frame loop's and the inflate
  // queue's — but only the frame loop is held: what #deliver hands over has
  // been counted against its own marks already.
  #announce(data, isBinary) {
    const budget = this.#turnBudget;
    if (budget !== null && !this.#turnHeld) {
      this.#turnCount++;
      this.#turnBytes += data.length;
      if (this.#turnCount >= budget.messages || this.#turnBytes >= budget.bytes) {
        this.#turnHeld = true;
        if (!this.#socket.destroyed) this.#socket.pause();
        setImmediate(this.#nextTurn);
      }
    }
    this.emit('message', data, isBinary);
  }

  #startTurn() {
    this.#turnHeld = false;
    this.#turnCount = 0;
    this.#turnBytes = 0;
    if (this.#socket.destroyed) return;
    if (!this.#paused && !this.#inboxHeld) this.#socket.resume();
    // Unconditionally: no 'data' event comes for what is already read, and
    // a close meanwhile left the peer's own Close among it.
    this.#processFrames();
  }

  #receive(data) {
    if (this.#closeReceived && this.#closing) return;

    this.#queue.push(data);

    if (this.#queue.length > this.#maxBuffer) {
      const error = new ParseError('Buffer overflow, closing connection', { code: PARSE_ERR_CODES.MESSAGE_TOO_BIG });
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
      // The inflate queue is full, or this connection has had its turn: what
      // is left waits in the segment queue (under maxBuffer) until the queue
      // drains or the next turn comes — see #releaseInbox and #nextTurn.
      if (this.#inboxHeld || this.#turnHeld) break;
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
    this.#fault('ws.frame', error, { close: error.code });
    this.#fail(parseErrorClose(error.code, this.#isClient));
  }

  #trackMessageSize(size) {
    const tooBig = size > this.#maxBuffer;
    if (tooBig) {
      const error = new ParseError('Message too big', { code: PARSE_ERR_CODES.MESSAGE_TOO_BIG });
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
        const error = new ParseError('Protocol error: Unexpected CONTINUATION without start', {
          code: PARSE_ERR_CODES.PROTOCOL_ERROR_COMMON,
        });
        this.#fault('ws.protocol', error);
        const closeFrame = Frame.protocolErrorClose('COMMON', this.#isClient);
        return void this.#fail(closeFrame);
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
        const error = new ParseError('Invalid UTF-8 in text frame', { code: PARSE_ERR_CODES.INVALID_PAYLOAD });
        this.#fault('ws.invalid-utf8', error);
        const closeFrame = Frame.errorClose('INVALID_PAYLOAD', this.#isClient);
        return void this.#fail(closeFrame);
      }
      this.#emitMessage(fullPayload, isBinary);
    } else {
      const error = new ParseError('Protocol error: Unexpected data frame during fragments', {
        code: PARSE_ERR_CODES.PROTOCOL_ERROR_COMMON,
      });
      this.#fault('ws.protocol', error);
      const closeFrame = Frame.protocolErrorClose('COMMON', this.#isClient);
      return void this.#fail(closeFrame);
    }
  }

  // A message decoded synchronously while others are still inflating off
  // the loop waits behind them: the inbox is what keeps arrival order.
  //
  // It waits COUNTED, and under the same marks as an inflating one. The hold
  // used to be looked at only where a message starts to inflate, so a peer
  // that sent one compressed frame and any number of plain ones behind it —
  // 300,000 of two bytes in a segment of 2.5 MiB — had every one of them
  // queued here: 70 MiB held with the socket never paused, and then taken
  // from the head of an array one shift() at a time, 44 seconds of one
  // synchronous loop. Held at the mark, the frame loop stops and what is
  // left waits in the segment queue, under maxBuffer; the inbox stays at
  // INBOX_HIGH + 1 messages, which is what keeps its shift() cheap.
  #emitMessage(data, isBinary) {
    if (this.#inbox.length > 0) {
      const size = data.length;
      this.#inbox.push({ done: true, error: null, data, isBinary, isText: false, size });
      this.#inboxBytes += size;
      if (this.#inbox.length >= INBOX_HIGH || this.#inboxBytes >= this.#maxPayload) this.#holdInbox();
      return;
    }
    this.#announce(data, isBinary);
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
      const size = payload.length;
      const item = { done: false, error: null, data: null, isBinary, isText, size };
      this.#inbox.push(item);
      this.#inboxBytes += size;
      // Never called inside decompress(): zlib's callback API and the live
      // stream both settle off the loop, which is what lets #deliver take
      // the socket up again without a re-entrancy guard here.
      const settle = (error, inflated) => {
        item.done = true;
        item.error = error ?? null;
        item.data = inflated ?? null;
        this.#deliver();
      };
      if (viaContext) this.#context.decompress(payload, limit, settle);
      else permessageDeflate.decompressAsync(payload, limit, settle);
      // The context is one stream, so its inflates are serial anyway and
      // the bound is on what waits; the threadpool path runs them in
      // parallel, and libuv's pool is four threads wide.
      const high = viaContext ? INBOX_HIGH : INBOX_ASYNC_HIGH;
      if (this.#inbox.length >= high || this.#inboxBytes >= this.#maxPayload) this.#holdInbox();
      return;
    }
    let inflated = null;
    try {
      inflated = permessageDeflate.decompress(payload, limit);
    } catch (error) {
      return void this.#failInflate(error);
    }
    if (isText && !isValidUTF8(inflated)) {
      const error = new ParseError('Invalid UTF-8 in text frame', { code: PARSE_ERR_CODES.INVALID_PAYLOAD });
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
      this.#inboxBytes -= item.size;
      if (this.#closing) continue;
      if (item.error !== null) return void this.#failInflate(item.error);
      if (item.isText && !isValidUTF8(item.data)) {
        const error = new ParseError('Invalid UTF-8 in text frame', { code: PARSE_ERR_CODES.INVALID_PAYLOAD });
        this.#fault('ws.invalid-utf8', error);
        return void this.#fail(Frame.errorClose('INVALID_PAYLOAD', this.#isClient));
      }
      this.#announce(item.data, item.isBinary);
    }
    if (this.#inboxHeld && inbox.length <= INBOX_LOW && this.#inboxBytes <= this.#maxPayload / 2) {
      this.#releaseInbox();
    }
  }

  // `options.compress === false` sends this one message uncompressed even
  // when deflate was negotiated and the payload clears the threshold.
  send(data, options = null) {
    if (typeof data === 'string') return this.sendText(data, options);
    if (Buffer.isBuffer(data)) return this.sendBinary(data, options);
    throw new ArgumentError('send() accepts only string or Buffer');
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
      return this.#write(frames.deflated(deflate.windowBits, deflate.zlibOptions ?? null));
    }
    return this.#write(frames.plain());
  }

  #write(buffer) {
    // Behind an in-flight compress every write waits its turn (the pump
    // writes with #draining set, so its own frames pass through).
    const outbox = this.#outbox;
    if (outbox !== null && outbox.length > 0 && !this.#draining) {
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

  #uncorkSocket() {
    this.#corked = false;
    if (!this.#socket.destroyed) this.#socket.uncork();
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
    const error = new TransportError('Backpressure limit exceeded, terminating connection', {
      code: 'ALX_ERR_BACKPRESSURE',
    });
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
      payload = permessageDeflate.compress(payload, deflate.windowBits, deflate.zlibOptions ?? null);
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
      const head = outbox.peek();
      if (head.buffer !== null) {
        outbox.shift();
        this.#outboxBytes -= head.buffer.length;
        // The Close a graceful close() queued behind what it had accepted:
        // everything ahead of it is on the socket now.
        if (head.buffer === this.#pendingClose) return void this.#flushClose();
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
      if (this.#outbox.peek() !== job) return;
      if (error) {
        // The mirror of ws.inflate: a deflate that failed on the way out is
        // a line, not only an 'error' nobody may be listening to.
        this.#fault('ws.deflate', error);
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
    const deflate = this.#deflate;
    const zlibOptions = deflate.zlibOptions ?? null;
    if (job.shared !== null) {
      job.shared.deflatedAsync(deflate.windowBits, zlibOptions, (error, frame) => finish(error, null, frame));
    } else if (this.#context !== null && deflate.serverTakeover === true) {
      this.#context.compress(job.payload, (error, compressed) => finish(error, compressed, null));
    } else {
      permessageDeflate.compressAsync(
        job.payload,
        deflate.windowBits,
        (error, compressed) => finish(error, compressed, null),
        zlibOptions,
      );
    }
  }

  #dropInbound() {
    const held = this.#inboxHeld;
    this.#inbox.length = 0;
    this.#inboxBytes = 0;
    this.#inboxHeld = false;
    // The socket the hold paused reads again (unless the application paused
    // it): the peer's Close, answering ours, is on it — left unread, the
    // close waited out closeTimeout and ended as 1006.
    if (!held || this.#socket.destroyed) return;
    if (!this.#paused && !this.#turnHeld) this.#socket.resume();
    // And what the hold left in the segment queue is taken up: no 'data'
    // event comes for bytes already read, and the peer's Close may be among
    // them. On the next tick — this runs from inside the frame loop when a
    // frame is what ended the connection.
    process.nextTick(() => {
      if (!this.#socket.destroyed) this.#processFrames();
    });
  }

  // Both directions, and the takeover context with them: every way out but
  // a graceful close whose accepted writes are still on their way.
  #dropQueues() {
    if (this.#outbox !== null) this.#outbox.clear();
    this.#outboxBytes = 0;
    this.#pendingClose = null;
    this.#dropInbound();
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
    // Behind a queued Close it would leave AFTER the Close frame.
    if (this.#pendingClose !== null) return false;
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
      // The same field owns the grace after end(): terminate() and the
      // socket's 'close' clear it, where an anonymous timer outlived both.
      this.#closeTimer = setTimeout(() => this.#socket.destroy(), 200);
    }, this.#closeTimeout);
  }

  // The queued Close of a graceful close(), at the moment it leaves: by the
  // pump once the outbox ahead of it is written, or by the close timer when
  // that took longer than `closeTimeout`.
  #flushClose() {
    const frame = this.#pendingClose;
    this.#dropQueues();
    this.#closeSent = true;
    this.#socket.write(frame);
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
    // Our own Close may still be queued behind accepted writes (closing,
    // not yet sent): the peer closing first ends that — it reads nothing
    // more — so the queue is dropped and the peer's Close answered.
    if (this.#closing && this.#pendingClose === null) return;
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

  // A graceful close delivers what send() already accepted. With a compress
  // in flight (context takeover, or `async` past its threshold) accepted
  // writes wait in the outbox, and closing used to drop them — so whether
  // `send(x); close()` delivered x depended on a performance option. The
  // Close frame now takes its place at the outbox's TAIL; nothing new is
  // accepted meanwhile, and `closeTimeout` stays the upper bound on the
  // whole close: when it expires with the queue still waiting, the queue is
  // dropped — and said (`ws.close.dropped`) — and the Close goes out.
  sendClose(code = 1000, reason = '') {
    const frame = Frame.close(code, reason);
    if (this.#isClient) frame.maskPayload();
    const buffer = frame.toBuffer();
    if (this.#closing || this.#outbox === null || this.#outbox.length === 0) return void this.#close(buffer);
    this.#closing = true;
    this.#fragments = null;
    this.#dropInbound();
    this.#pendingClose = buffer;
    this.#enqueue({ buffer, payload: null, shared: null, opcode: 0, started: false });
    this.#closeTimer = setTimeout(() => {
      if (this.#pendingClose !== null) {
        // A line, not an 'error': nothing is wrong with the connection but
        // the time its last writes took.
        const frames = this.#outbox.length - 1;
        const bytes = this.#outboxBytes - this.#pendingClose.length;
        this.#log.warn({ event: 'ws.close.dropped', frames, bytes });
        this.#flushClose();
      }
      this.#socket.end();
      // The same field owns the grace after end(): terminate() and the
      // socket's 'close' clear it, where an anonymous timer outlived both.
      this.#closeTimer = setTimeout(() => this.#socket.destroy(), 200);
    }, this.#closeTimeout);
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

module.exports = { MAX_PAYLOAD, CLOSE_TIMEOUT, TURN_BUDGET, turnBudgetOf, Connection };
