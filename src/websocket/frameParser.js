'use strict';

const { CONTROL_OPCODES, DATA_OPCODES, OPCODES, CLOSE_CODES, RSV1 } = require('./constants.js');
const { Frame } = require('./frame.js');
const { Result } = require('./result.js');

const FIN_MASK = 0x80;
const RSV_MASK = 0x70;
const OPCODE_MASK = 0x0f;
const MASK_MASK = 0x80;
const PAYLOAD_LEN_MASK = 0x7f;
const LEN_16_BIT = 126;
const LEN_64_BIT = 127;
const MAX_SAFE_HIGH_MASK = 0xffe00000;
const TWO_32 = 4294967296;
const VALID_USER_CLOSE_CODES = { MIN: 3000, MAX: 4999 };
const VALID_CLOSE_CODES = new Set([
  CLOSE_CODES.NORMAL_CLOSE,
  CLOSE_CODES.GOING_AWAY,
  CLOSE_CODES.PROTOCOL_ERROR,
  CLOSE_CODES.UNSUPPORTED_DATA,
  CLOSE_CODES.INVALID_PAYLOAD,
  CLOSE_CODES.POLICY_VIOLATED,
  CLOSE_CODES.MESSAGE_TOO_BIG,
  CLOSE_CODES.MANDATORY_EXTENSION,
  CLOSE_CODES.INTERNAL_SERVER_ERROR,
]);

const { isUtf8: nativeIsUtf8 } = require('node:buffer');

// The scalar JS loop below stays for SHORT buffers (a native call has fixed
// overhead that dominates under ~24 bytes — the same threshold ws uses) and
// as the fallback where node:buffer.isUtf8 is missing. Everything else goes
// to the native SIMD path: 20-70x faster on kilobyte-and-up frames, and the
// single biggest receive-path gap benchmarks showed against ws.
const isValidUTF8 = (buf) => {
  if (nativeIsUtf8 && buf.length >= 24) return nativeIsUtf8(buf);
  return isValidUTF8Scalar(buf);
};

const isValidUTF8Scalar = (buf) => {
  let i = 0;
  const len = buf.length;

  while (i < len) {
    const byte1 = buf[i];

    if (byte1 <= 0x7f) {
      // 1-byte sequence: 0xxxxxxx
      i += 1;
    } else if ((byte1 & 0xe0) === 0xc0) {
      // 2-byte sequence: 110xxxxx 10xxxxxx
      if (i + 1 >= len) return false;
      const byte2 = buf[i + 1];
      if ((byte2 & 0xc0) !== 0x80) return false;

      const codePoint = ((byte1 & 0x1f) << 6) | (byte2 & 0x3f);
      // Overlong encoding check: minimum code point for 2-byte is 0x80
      if (codePoint < 0x80) return false;

      i += 2;
    } else if ((byte1 & 0xf0) === 0xe0) {
      // 3-byte sequence: 1110xxxx 10xxxxxx 10xxxxxx
      if (i + 2 >= len) return false;
      const byte2 = buf[i + 1];
      const byte3 = buf[i + 2];
      if ((byte2 & 0xc0) !== 0x80 || (byte3 & 0xc0) !== 0x80) return false;

      const codePoint = ((byte1 & 0x0f) << 12) | ((byte2 & 0x3f) << 6) | (byte3 & 0x3f);
      // Overlong encoding check: minimum code point for 3-byte is 0x800
      if (codePoint < 0x800) return false;
      // Check for surrogate halves (U+D800 to U+DFFF)
      if (codePoint >= 0xd800 && codePoint <= 0xdfff) return false;

      i += 3;
    } else if ((byte1 & 0xf8) === 0xf0) {
      // 4-byte sequence: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
      if (i + 3 >= len) return false;
      const byte2 = buf[i + 1];
      const byte3 = buf[i + 2];
      const byte4 = buf[i + 3];
      if ((byte2 & 0xc0) !== 0x80 || (byte3 & 0xc0) !== 0x80 || (byte4 & 0xc0) !== 0x80) {
        return false;
      }

      const codePoint = ((byte1 & 0x07) << 18) | ((byte2 & 0x3f) << 12) | ((byte3 & 0x3f) << 6) | (byte4 & 0x3f);
      // Overlong encoding check: minimum code point for 4-byte is 0x10000
      if (codePoint < 0x10000) return false;
      // Maximum valid code point is 0x10FFFF
      if (codePoint > 0x10ffff) return false;

      i += 4;
    } else {
      // Invalid leading byte
      return false;
    }
  }

  return true;
};

const isValidCloseCode = (code) => {
  if (VALID_CLOSE_CODES.has(code)) return true;
  const isValidUserCode = code >= VALID_USER_CLOSE_CODES.MIN && code <= VALID_USER_CLOSE_CODES.MAX;
  if (isValidUserCode) return true;
  return false;
};

class ParseError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'ParseError';
    this.code = code;
  }
}

const PARSE_ERR_CODES = {
  MESSAGE_TOO_BIG: 'MESSAGE_TOO_BIG',
  PROTOCOL_ERROR_COMMON: 'PROTOCOL_ERROR-COMMON',
  PROTOCOL_ERROR_RSV: 'PROTOCOL_ERROR-RSV',
  PROTOCOL_ERROR_CTRL_TOO_LONG: 'PROTOCOL_ERROR-CTRL_TOO_LONG',
  INVALID_PAYLOAD: 'INVALID_PAYLOAD',
};

class FrameParser {
  // Parses only the frame header (through the mask key, when present).
  // Returns { fin, rsv, opcode, masked, mask, length, headerSize } or an
  // empty Result when more bytes are needed. `allowedRsv` is a bitmask of
  // RSV bits negotiated via extensions (RFC 6455 5.2) — any other set RSV
  // bit is a protocol error.
  static parseHeader(buffer, options = {}) {
    if (buffer.length < 2) return Result.empty();

    const fin = (buffer[0] & FIN_MASK) !== 0;
    const rsv = buffer[0] & RSV_MASK;
    const opcode = buffer[0] & OPCODE_MASK;
    const masked = (buffer[1] & MASK_MASK) !== 0;
    let length = buffer[1] & PAYLOAD_LEN_MASK;
    let offset = 2;

    const allowedRsv = options.allowedRsv ?? 0;
    if ((rsv & ~allowedRsv) !== 0) {
      return Result.from(new ParseError(PARSE_ERR_CODES.PROTOCOL_ERROR_RSV, 'RSV bits must be 0'));
    }

    if (length === LEN_16_BIT) {
      if (buffer.length < offset + 2) return Result.empty();
      length = buffer.readUInt16BE(offset);
      offset += 2;
    } else if (length === LEN_64_BIT) {
      if (buffer.length < offset + 8) return Result.empty();
      const high = buffer.readUInt32BE(offset);
      const low = buffer.readUInt32BE(offset + 4);
      offset += 8;
      const isSafeHigh = (high & MAX_SAFE_HIGH_MASK) === 0;
      if (!isSafeHigh) {
        return Result.from(new ParseError(PARSE_ERR_CODES.MESSAGE_TOO_BIG, 'Payload length exceeds MAX_SAFE_INTEGER'));
      }
      length = high * TWO_32 + low;
    }

    let mask = null;
    if (masked) {
      if (buffer.length < offset + 4) return Result.empty();
      // A view, not a copy: the segment bytes are immutable and the mask is
      // consumed immediately by unmaskPayload — one less allocation on the
      // hottest path in the parser.
      mask = buffer.subarray(offset, offset + 4);
      offset += 4;
    }

    return Result.from({ fin, rsv, opcode, masked, mask, length, headerSize: offset });
  }

  static parse(buffer, options = {}) {
    const headerResult = FrameParser.parseHeader(buffer, options);
    const { value, error } = headerResult;
    if (error || !value) return headerResult;

    const { fin, rsv, opcode, masked, mask, length, headerSize } = value;
    if (buffer.length < headerSize + length) return Result.empty();
    const payload = buffer.subarray(headerSize, headerSize + length);
    const frame = new Frame(fin, opcode, masked, payload, mask, rsv);
    return Result.from({ frame, bytesUsed: headerSize + length });
  }

  static checkControlFrame(frame) {
    const { fin, rsv, opcode, payload } = frame;
    if (!CONTROL_OPCODES.has(opcode) || !fin || rsv !== 0) {
      return Result.from(new ParseError(PARSE_ERR_CODES.PROTOCOL_ERROR_COMMON, 'Protocol error'));
    }
    if (payload.length > 125) {
      return Result.from(new ParseError(PARSE_ERR_CODES.PROTOCOL_ERROR_CTRL_TOO_LONG, 'Control frame too long'));
    }
    if (opcode === OPCODES.CLOSE) {
      if (payload.length === 0) return Result.from(true);
      if (payload.length === 1) {
        return Result.from(new ParseError(PARSE_ERR_CODES.PROTOCOL_ERROR_COMMON, 'Protocol error'));
      }
      const code = payload.readUInt16BE(0);
      const reason = payload.subarray(2);
      if (!isValidCloseCode(code)) {
        return Result.from(new ParseError(PARSE_ERR_CODES.PROTOCOL_ERROR_COMMON, `Invalid close code: ${code}`));
      }
      if (!isValidUTF8(reason)) {
        return Result.from(new ParseError(PARSE_ERR_CODES.INVALID_PAYLOAD, 'Invalid UTF-8 in close reason'));
      }
    }
    return Result.from(true);
  }

  static checkDataFrame(frame) {
    const { fin, rsv, opcode, payload } = frame;
    if (!DATA_OPCODES.has(opcode)) {
      return Result.from(new ParseError(PARSE_ERR_CODES.PROTOCOL_ERROR_COMMON, 'Protocol error'));
    }
    // RSV1 (compression) may only start a message, never continue one
    if (opcode === OPCODES.CONTINUATION && rsv !== 0) {
      return Result.from(new ParseError(PARSE_ERR_CODES.PROTOCOL_ERROR_COMMON, 'Protocol error'));
    }
    // Compressed text is validated after inflation, not on the wire bytes
    const isCompressed = (rsv & RSV1) !== 0;
    const isText = opcode === OPCODES.TEXT;
    if (isText && fin && !isCompressed && !isValidUTF8(payload)) {
      return Result.from(new ParseError(PARSE_ERR_CODES.INVALID_PAYLOAD, 'Invalid UTF-8 in text frame'));
    }
    return Result.from(true);
  }
}

module.exports = {
  FrameParser,
  ParseError,
  PARSE_ERR_CODES,
  isValidUTF8,
};
