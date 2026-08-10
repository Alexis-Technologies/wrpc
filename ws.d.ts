import { EventEmitter } from 'node:events';
import { IncomingMessage, Server as HttpServer } from 'node:http';
import { Server as HttpsServer } from 'node:https';
import { Socket } from 'node:net';
import type { Duplex } from 'node:stream';

export declare const MAGIC: string;
export declare const CLOSE_TIMEOUT: number;
export declare const RSV1: number;

export declare const OPCODES: {
  readonly CONTINUATION: 0x00;
  readonly TEXT: 0x01;
  readonly BINARY: 0x02;
  readonly CLOSE: 0x08;
  readonly PING: 0x09;
  readonly PONG: 0x0a;
};

export declare const CLOSE_CODES: {
  readonly NORMAL_CLOSE: 1000;
  readonly GOING_AWAY: 1001;
  readonly PROTOCOL_ERROR: 1002;
  readonly UNSUPPORTED_DATA: 1003;
  readonly RESERVED: 1004;
  readonly NO_CODE_RECEIVED: 1005;
  readonly CONNECTION_CLOSED_ABNORMALLY: 1006;
  readonly INVALID_PAYLOAD: 1007;
  readonly POLICY_VIOLATED: 1008;
  readonly MESSAGE_TOO_BIG: 1009;
  readonly MANDATORY_EXTENSION: 1010;
  readonly INTERNAL_SERVER_ERROR: 1011;
  readonly TLS_HANDSHAKE: 1015;
};

export interface VerifyClientInfo {
  req: IncomingMessage;
  socket: Socket;
  head: Buffer;
}

export interface DeflateParams {
  response: string;
  threshold: number;
  windowBits: number;
}

export interface PerMessageDeflateOptions {
  threshold?: number;
}

export interface WebsocketServerOptions {
  /**
   * Binds to this server's 'upgrade' event. Omit it to drive handshakes by
   * hand through handleUpgrade() — how middleware adapters (express) attach
   * to a listener they do not own.
   */
  server?: HttpServer | HttpsServer;
  pingInterval?: number;
  maxBuffer?: number;
  closeTimeout?: number;
  maxBackpressure?: number;
  fragmentThreshold?: number;
  path?: string;
  verifyClient?: (info: VerifyClientInfo) => boolean;
  protocols?: Array<string>;
  handleProtocols?: (offered: Array<string>, req: IncomingMessage) => string | false;
  perMessageDeflate?: boolean | PerMessageDeflateOptions;
}

export declare class WebsocketServer extends EventEmitter {
  constructor(options?: WebsocketServerOptions);

  /** Snapshot of the live connections (mutations do not affect the server). */
  readonly connections: Set<Connection>;

  /**
   * Performs one handshake on a raw socket. Use it from your own 'upgrade'
   * listener when the server was constructed without `options.server`.
   */
  handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void;

  close(options?: { code?: number; reason?: string }): void;

  on(
    event: 'connection',
    listener: (ws: Connection, req: IncomingMessage) => void,
  ): this;

  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'close', listener: () => void): this;
  on(event: string | symbol, listener: (...args: unknown[]) => void): this;
}

export interface ConnectionOptions {
  isClient?: boolean;
  maxBuffer?: number;
  closeTimeout?: number;
  maxBackpressure?: number;
  fragmentThreshold?: number;
  protocol?: string;
  deflate?: DeflateParams | null;
}

export declare class Connection extends EventEmitter {
  constructor(socket: Socket, head: Buffer, options?: ConnectionOptions);

  /** Subprotocol selected during the handshake, '' when none. */
  protocol: string;
  /** Bytes queued in the socket's writable buffer, not yet flushed. */
  readonly bufferedAmount: number;
  /** True while socket reads are paused via pause(). */
  readonly isPaused: boolean;
  readonly remoteAddress: string | undefined;

  send(data: string | Buffer): boolean;
  sendText(message: string): boolean;
  sendBinary(buffer: Buffer): boolean;
  sendPing(payload?: Buffer | string): boolean;
  sendPong(payload?: Buffer | string): boolean;
  sendClose(code?: number, reason?: string): void;
  /** WrpcSocket engine-contract alias for sendClose. */
  close(code?: number, reason?: string): void;
  terminate(): void;
  pause(): void;
  resume(): void;

  /**
   * Received payloads may share memory with the socket receive buffer;
   * copy them when retaining beyond the listener call.
   */
  on(
    event: 'message',
    listener: (data: Buffer, isBinary: boolean) => void,
  ): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'close', listener: (code: number, reason: string) => void): this;
  on(event: 'ping', listener: (payload: Buffer) => void): this;
  on(event: 'pong', listener: (payload: Buffer) => void): this;
  on(event: 'drain', listener: () => void): this;
}

export declare class Frame {
  fin: boolean;
  rsv: number;
  opcode: number;
  masked: boolean;
  payload: Buffer;
  mask: Buffer | null;
  constructor(
    fin: boolean,
    opcode: number,
    masked: boolean,
    payload: Buffer,
    mask: Buffer | null,
    rsv?: number,
  );
  static text(message: string, fin?: boolean, masked?: boolean): Frame;
  static binary(buffer: Buffer, fin?: boolean, masked?: boolean): Frame;
  static ping(payload?: Buffer | string): Frame;
  static pong(payload?: Buffer | string): Frame;
  static close(code?: number | null, reason?: string): Frame;
  unmaskPayload(): void;
  maskPayload(): void;
  toString(): string | null;
  toBuffer(): Buffer;
  readonly header: Buffer;
  readonly isControlFrame: boolean;
}

export declare class ParseError extends Error {
  code: string;
  constructor(code: string, message: string);
}

export declare const PARSE_ERR_CODES: {
  readonly MESSAGE_TOO_BIG: 'MESSAGE_TOO_BIG';
  readonly PROTOCOL_ERROR_COMMON: 'PROTOCOL_ERROR-COMMON';
  readonly PROTOCOL_ERROR_RSV: 'PROTOCOL_ERROR-RSV';
  readonly PROTOCOL_ERROR_CTRL_TOO_LONG: 'PROTOCOL_ERROR-CTRL_TOO_LONG';
  readonly INVALID_PAYLOAD: 'INVALID_PAYLOAD';
};

export interface FrameHeader {
  fin: boolean;
  rsv: number;
  opcode: number;
  masked: boolean;
  mask: Buffer | null;
  length: number;
  headerSize: number;
}

export interface FrameParserOptions {
  /** Bitmask of RSV bits negotiated via extensions (RFC 6455 5.2). */
  allowedRsv?: number;
}

export declare class FrameParser {
  static parseHeader(
    buffer: Buffer,
    options?: FrameParserOptions,
  ): {
    value: FrameHeader | null;
    error: ParseError | null;
  };
  static parse(
    buffer: Buffer,
    options?: FrameParserOptions,
  ): {
    value: { frame: Frame; bytesUsed: number } | null;
    error: ParseError | null;
  };
  static checkControlFrame(frame: Frame): {
    value: boolean | null;
    error: ParseError | null;
  };
  static checkDataFrame(frame: Frame): {
    value: boolean | null;
    error: ParseError | null;
  };
}
