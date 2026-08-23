import { EventEmitter } from 'node:events';
import { IncomingMessage, Server as HttpServer } from 'node:http';
import { Server as HttpsServer } from 'node:https';
import type { Duplex } from 'node:stream';
import type { PerMessageDeflateOptions } from './ws.js';
import type { HttpCall } from './index.js';

/**
 * The upgrade request an engine hands to `verifyClient`, `handleProtocols`
 * and the 'connection' listener.
 *
 * The built-in engine passes a real node `IncomingMessage`; a standalone
 * engine (uWebSockets.js) never sees one and synthesizes a look-alike, so
 * the port promises only what both provide. Narrow with a cast when an
 * engine's concrete request type is known.
 */
export interface EngineRequest {
  method?: string;
  url?: string;
  headers: Record<string, string | Array<string> | undefined>;
  socket: { remoteAddress?: string };
}

export interface EngineVerifyClientInfo {
  req: EngineRequest;
  /** Null for engines that own their network stack instead of node:http. */
  socket: Duplex | null;
  head: Buffer | null;
}

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
  /**
   * Required for hosted engines, absent for standalone ones and for the
   * manual-upgrade mode middleware adapters use.
   */
  server?: HttpServer | HttpsServer;
  path?: string;
  verifyClient?: (info: EngineVerifyClientInfo) => boolean;
  protocols?: Array<string>;
  handleProtocols?: (offered: Array<string>, req: EngineRequest) => string | false;
  perMessageDeflate?: boolean | PerMessageDeflateOptions;
  pingInterval?: number;
  maxBuffer?: number;
  /** Inflated-size cap for permessage-deflate messages. Default 16 MiB. */
  maxPayload?: number;
  /** Outbound-buffer cap; defaults to maxBuffer, 0 = unbounded. */
  maxBackpressure?: number;
  fragmentThreshold?: number;
  closeTimeout?: number;
  /**
   * Standalone engines only: the core's HTTP entry point, invoked with the
   * same abstract call description RpcServer.handleHttpCall consumes.
   */
  onHttpCall?: (call: HttpCall) => unknown;
}

export interface EngineConnectionSource extends EventEmitter {
  on(
    event: 'connection',
    listener: (socket: WrpcSocket, req: EngineRequest) => void,
  ): this;
  on(event: string | symbol, listener: (...args: unknown[]) => void): this;
  /**
   * Present when the engine can be driven from the app's own 'upgrade'
   * listener instead of binding to a server (the express adapter's mode).
   * Only node-hosted engines offer it, so the request is a real one.
   */
  handleUpgrade?(req: IncomingMessage, socket: Duplex, head: Buffer): void;
}

/**
 * The replaceable server-side engine contract.
 *
 * A *hosted* engine (the default) attaches to a node http server's upgrade
 * flow: `attach({ server })`. The Server shell owns the listener and the
 * HTTP request path.
 *
 * A *standalone* engine (`standalone: true`, e.g. uWebSockets.js) owns the
 * whole network stack instead: it is attached without a server, receives the
 * core's HTTP entry point as `onHttpCall`, and must implement `listen()`.
 */
export interface Engine {
  name: string;
  standalone?: boolean;
  /** @experimental Beyond the documented WrpcSocket contract; may change in a minor. */
  capabilities: EngineCapabilities;
  attach(options: EngineAttachOptions): EngineConnectionSource;
  listen?(options: { host?: string; port?: number }): Promise<unknown>;
  /**
   * Optional listener-only phase of a graceful shutdown (standalone
   * engines): refuse NEW connections while the accepted ones keep working
   * — the intake-first ordering a hosted boot gets from httpServer.close().
   * Feature-detected by the Server shell before drain().
   */
  stopListening?(): void;
  close(options?: { code?: number; reason?: string }): void;
}

export declare function createNodeEngine(
  engineOptions?: Omit<EngineAttachOptions, 'server'>,
): Engine;

export declare function isEngine(engine: unknown): engine is Engine;
