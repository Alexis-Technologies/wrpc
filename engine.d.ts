import { EventEmitter } from 'node:events';
import { IncomingMessage, Server as HttpServer } from 'node:http';
import { Server as HttpsServer } from 'node:https';
import type { VerifyClientInfo, PerMessageDeflateOptions } from './ws.js';

/**
 * The server-side socket contract every engine adapter yields for each
 * peer. The built-in engine's Connection satisfies it natively; adapters
 * (e.g. uWebSockets.js) normalize their sockets to this shape.
 *
 * Events: 'message'(data, isBinary), 'drain', 'ping'(payload),
 * 'pong'(payload), 'close'(code, reason), 'error'(error).
 */
export interface WrpcSocket extends EventEmitter {
  /** Returns false when the socket buffer is above its high-water mark. */
  send(data: string | Buffer): boolean;
  readonly bufferedAmount: number;
  readonly remoteAddress?: string;
  protocol?: string;
  close(code?: number, reason?: string): void;
  terminate(): void;
  /** Optional receive-side flow control (capability `pause`). */
  pause?(): void;
  resume?(): void;
}

export interface EngineCapabilities {
  backpressure: boolean;
  /** True when the engine can send protocol pings / owns peer liveness. */
  ping: boolean;
  deflate: boolean;
  cork: boolean;
  pause: boolean;
}

export interface EngineAttachOptions {
  server: HttpServer | HttpsServer;
  path?: string;
  verifyClient?: (info: VerifyClientInfo) => boolean;
  protocols?: Array<string>;
  handleProtocols?: (offered: Array<string>, req: IncomingMessage) => string | false;
  perMessageDeflate?: boolean | PerMessageDeflateOptions;
  pingInterval?: number;
  maxBuffer?: number;
  maxBackpressure?: number;
  fragmentThreshold?: number;
  closeTimeout?: number;
}

export interface EngineConnectionSource extends EventEmitter {
  on(
    event: 'connection',
    listener: (socket: WrpcSocket, req: IncomingMessage) => void,
  ): this;
  on(event: string | symbol, listener: (...args: unknown[]) => void): this;
}

/**
 * The replaceable server-side engine contract: attach() binds to an http
 * server's upgrade flow and emits 'connection'(WrpcSocket, req).
 */
export interface Engine {
  name: string;
  capabilities: EngineCapabilities;
  attach(options: EngineAttachOptions): EngineConnectionSource;
  close(options?: { code?: number; reason?: string }): void;
}

export declare function createNodeEngine(
  engineOptions?: Omit<EngineAttachOptions, 'server'>,
): Engine;

export declare function isEngine(engine: unknown): engine is Engine;
