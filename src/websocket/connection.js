'use strict';

const { EventEmitter } = require('node:events');

const { OPCODES, CLOSE_CODES, RSV1 } = require('./constants.js');
const { Frame, EMPTY_PING, EMPTY_PONG } = require('./frame.js');
const { FrameParser, isValidUTF8 } = require('./frameParser.js');
const { SegmentQueue } = require('./segments.js');
const permessageDeflate = require('./permessageDeflate.js');

const MAX_BUFFER = 1024 * 1024 * 100;
const CLOSE_TIMEOUT = 1000;
// How long the answering side waits for the peer's FIN before destroying.
const CLOSE_GRACE = 200;
const MAX_HEADER_SIZE = 14; // 2 base + 8 extended length + 4 mask key

class Connection extends EventEmitter {
  #socket;
  #isClient;
  #queue = new SegmentQueue();
  #pendingHeader = null;
  #maxBuffer;
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

  constructor(socket, head, options = {}) {
    super();
    this.#socket = socket;

    const {
      isClient = false,
      maxBuffer = MAX_BUFFER,
      closeTimeout = CLOSE_TIMEOUT,
      maxBackpressure = 0,
      fragmentThreshold = 0,
      protocol = '',
      deflate = null,
    } = options;
    this.#isClient = isClient;
    this.#maxBuffer = maxBuffer;
    this.#closeTimeout = closeTimeout;
    this.#maxBackpressure = maxBackpressure;
    this.#fragmentThreshold = fragmentThreshold;
    this.#deflate = deflate;
    if (deflate) this.#allowedRsv = RSV1;
    this.protocol = protocol;
    this.#init(head);
  }

  get bufferedAmount() {
    return this.#socket.writableLength ?? 0;
  }

  get remoteAddress() {
    return this.#socket.remoteAddress;
  }

  #init(head) {
    this.#socket.on('data', (data) => this.#receive(data));
    this.#socket.on('drain', () => {
      if (!this.#needsDrain) return;
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
      this.emit('error', error);
      if (this.#isClient) {
        return void this.sendClose(CLOSE_CODES.MESSAGE_TOO_BIG, 'Message too big');
      }
      const frame = Frame.errorClose('MESSAGE_TOO_BIG');
      return void this.#close(frame);
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
          return void this.#close(closeFrame);
        }
        if (this.#isClient && value.masked) {
          const closeFrame = Frame.protocolErrorClose('MASKED', this.#isClient);
          return void this.#close(closeFrame);
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
    this.emit('error', error);
    const frame =
      type === 'PROTOCOL_ERROR'
        ? Frame.protocolErrorClose(subtype, this.#isClient)
        : Frame.errorClose(type, this.#isClient);
    this.#close(frame);
  }

  #trackMessageSize(size) {
    const tooBig = size > this.#maxBuffer;
    if (tooBig) {
      const error = new Error('Message too big');
      this.emit('error', error);
      if (this.#isClient) {
        this.sendClose(CLOSE_CODES.MESSAGE_TOO_BIG, 'Message too big');
      } else {
        const frame = Frame.errorClose('MESSAGE_TOO_BIG');
        this.#close(frame);
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
        this.emit('error', error);
        const frame = Frame.protocolErrorClose('COMMON', this.#isClient);
        return void this.#close(frame);
      }
      if (frame.fin) {
        // single frame
        if (compressed) return void this.#emitInflated(opcode, payload);
        const isBinary = opcode === OPCODES.BINARY;
        this.emit('message', frame.payload, isBinary);
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
        this.emit('error', error);
        const frame = Frame.errorClose('INVALID_PAYLOAD', this.#isClient);
        return void this.#close(frame);
      }
      this.emit('message', fullPayload, isBinary);
    } else {
      const error = new Error('Protocol error: Unexpected data frame during fragments');
      this.emit('error', error);
      const frame = Frame.protocolErrorClose('COMMON', this.#isClient);
      return void this.#close(frame);
    }
  }

  #emitInflated(opcode, payload) {
    let inflated = null;
    try {
      inflated = permessageDeflate.decompress(payload, this.#maxBuffer);
    } catch (error) {
      this.emit('error', error);
      const type = error.code === 'ERR_BUFFER_TOO_LARGE' ? 'MESSAGE_TOO_BIG' : 'INVALID_PAYLOAD';
      return void this.#close(Frame.errorClose(type, this.#isClient));
    }
    const isText = opcode === OPCODES.TEXT;
    if (isText && !isValidUTF8(inflated)) {
      const error = new Error('Invalid UTF-8 in text frame');
      this.emit('error', error);
      return void this.#close(Frame.errorClose('INVALID_PAYLOAD', this.#isClient));
    }
    this.emit('message', inflated, opcode === OPCODES.BINARY);
  }

  send(data) {
    if (typeof data === 'string') return this.sendText(data);
    if (Buffer.isBuffer(data)) return this.sendBinary(data);
    throw new TypeError('send() accepts only string or Buffer');
  }

  #write(buffer) {
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
    this.emit('error', error);
    this.terminate();
    return true;
  }

  #sendData(opcode, payload) {
    let rsv = 0;
    if (this.#deflate && payload.length >= this.#deflate.threshold) {
      payload = permessageDeflate.compress(payload, this.#deflate.windowBits);
      rsv = RSV1;
    }
    const threshold = this.#fragmentThreshold;
    if (!threshold || payload.length <= threshold) {
      return this.#writeFrame(new Frame(true, opcode, false, payload, null, rsv));
    }
    let ok = true;
    for (let offset = 0; offset < payload.length; offset += threshold) {
      const end = Math.min(offset + threshold, payload.length);
      const fin = end === payload.length;
      const op = offset === 0 ? opcode : OPCODES.CONTINUATION;
      const frameRsv = offset === 0 ? rsv : 0;
      ok = this.#writeFrame(new Frame(fin, op, false, payload.subarray(offset, end), null, frameRsv));
    }
    return ok;
  }

  // Data send methods return false when the socket did not take the bytes
  // without exceeding its high-water mark — wait for 'drain' before more.
  sendText(message) {
    if (this.#closing) return false;
    if (this.#exceedsBackpressure()) return false;
    return this.#sendData(OPCODES.TEXT, Buffer.from(message, 'utf8'));
  }

  sendBinary(buffer) {
    if (this.#closing) return false;
    if (this.#exceedsBackpressure()) return false;
    if (!Buffer.isBuffer(buffer)) buffer = Buffer.from(buffer);
    return this.#sendData(OPCODES.BINARY, buffer);
  }

  sendPing(payload) {
    if (this.#closing) return false;
    if (payload) return this.#writeFrame(Frame.ping(payload));
    return this.#fastPing();
  }

  // No #closing guard: RFC 6455 5.5.3 requires a pong unless a Close frame
  // was received, and #processFrames stops draining input once
  // #closeReceived is set — including frames already queued behind the Close.
  sendPong(payload) {
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

  #close(frameBuffer) {
    if (this.#closing) return;
    this.#closing = true;
    this.#closeSent = true;
    this.#fragments = null;

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
    if (!this.#socket.destroyed) this.#socket.destroy();
  }
}

module.exports = { Connection, CLOSE_TIMEOUT };
