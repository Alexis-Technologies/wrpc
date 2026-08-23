import { EventEmitter } from 'node:events';
import type { Engine, EngineCapabilities, WrpcSocket } from './engine.js';
import type { HttpCall } from './index.js';

/**
 * The slice of the uWebSockets.js module this adapter uses. Typed
 * structurally on purpose: `uWebSockets.js` is injected by the caller and is
 * never a dependency of @alexify/wrpc, so its own types may be absent.
 */
export interface UwsModule {
  App(options?: Record<string, unknown>): UwsApp;
  SSLApp(options: Record<string, unknown>): UwsApp;
  us_listen_socket_close?(token: unknown): void;
  us_socket_local_port?(token: unknown): number;
  [key: string]: unknown;
}

export interface UwsApp {
  ws(pattern: string, behavior: Record<string, unknown>): UwsApp;
  any(pattern: string, handler: (res: unknown, req: unknown) => void): UwsApp;
  listen(host: string, port: number, cb: (token: unknown) => void): UwsApp;
  close?(): UwsApp;
  [key: string]: unknown;
}

/** A WrpcSocket backed by a uws WebSocket handle. */
export declare class UwsSocket extends EventEmitter implements WrpcSocket {
  readonly closed: boolean;
  readonly bufferedAmount: number;
  remoteAddress: string;
  protocol: string;
  constructor(ws: unknown, meta?: { remoteAddress?: string; protocol?: string });
  send(data: string | Buffer): boolean;
  close(code?: number, reason?: string): void;
  terminate(): void;
  /** Called by the engine's close handler; the uws handle is poison after it. */
  markClosed(code: number, reason: string): void;
}

export interface UwsEngineOptions {
  /** `require('uWebSockets.js')` — injected, never a dependency. */
  uws?: UwsModule;
  /** An existing app to attach to (fastify-uws) instead of creating one. */
  app?: UwsApp;
  /** Passed to SSLApp() instead of App() when present. */
  ssl?: Record<string, unknown> | null;
  /** Seconds without traffic before uws closes the peer. Default 120. */
  idleTimeout?: number;
  maxPayloadLength?: number;
  maxBackpressure?: number;
  closeOnBackpressureLimit?: boolean;
  maxLifetime?: number;
  /** A uws compressor constant; null (default) disables permessage-deflate. */
  compression?: number | null;
  sendPingsAutomatically?: boolean;
  maxBodySize?: number;
}

export interface UwsAttachOptions {
  /** uws route pattern for upgrades. Default '/*'. */
  path?: string;
  verifyClient?: (info: { req: UpgradeRequest; socket: null; head: null }) => boolean;
  protocols?: Array<string>;
  handleProtocols?: (offered: Array<string>, req: UpgradeRequest) => string | false;
  /** When present, the engine also owns HTTP and routes calls here. */
  onHttpCall?: (call: HttpCall) => unknown;
}

/** A node IncomingMessage look-alike synthesized from the uws upgrade. */
export interface UpgradeRequest {
  method: string;
  url: string;
  headers: Record<string, string>;
  socket: { remoteAddress: string };
}

export interface UwsEngineConnectionSource extends EventEmitter {
  on(event: 'connection', listener: (socket: UwsSocket, req: UpgradeRequest) => void): this;
  on(event: string | symbol, listener: (...args: unknown[]) => void): this;
}

export interface UwsEngine extends Engine {
  name: 'uws';
  standalone: true;
  capabilities: EngineCapabilities;
  app: UwsApp;
  attach(options?: UwsAttachOptions): UwsEngineConnectionSource;
  listen(options?: { host?: string; port?: number }): Promise<{
    address: string;
    family: string;
    port: number;
  }>;
  close(options?: { code?: number; reason?: string }): void;
}

/**
 * Standalone engine over uWebSockets.js: uws owns the whole network stack,
 * so `Server` creates no node:http server and delegates listening.
 */
export declare function createUwsEngine(options?: UwsEngineOptions): UwsEngine;

/** uws send() status codes, re-exported for adapters and tests. */
export declare const BACKPRESSURE: 0;
export declare const SUCCESS: 1;
export declare const DROPPED: 2;
