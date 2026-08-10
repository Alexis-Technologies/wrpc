import {
  IncomingMessage,
  Server as HttpServer,
  ServerResponse,
} from 'node:http';
import { Writable } from 'node:stream';
import type { Connection } from './ws.js';
import type { Engine, EngineConnectionSource, WrpcSocket, EngineAttachOptions } from './engine.js';

export declare class Emitter {
  constructor(options?: { maxListeners?: number });
  emit(eventName: PropertyKey, value?: unknown): Promise<void>;
  on(eventName: PropertyKey, listener: (value: any) => void): void;
  once(eventName: PropertyKey, listener: (value: any) => void): void;
  off(eventName: PropertyKey, listener?: (value: any) => void): void;
  clear(eventName?: PropertyKey): void;
  listeners(eventName: PropertyKey): Array<(value: any) => void>;
  listenerCount(eventName: PropertyKey): number;
  eventNames(): Array<PropertyKey>;
}

export class WrpcError extends Error {
  code: number;
  constructor(options: { message: string; code: number });
}

export interface ReadableOptions {
  highWaterMark?: number;
}

export class WrpcReadable extends Emitter {
  id: string;
  name: string;
  size: number;
  queue: Array<ArrayBufferView>;
  streaming: boolean;
  status: string;
  bytesRead: number;
  highWaterMark: number;
  constructor(
    id: string,
    name: string,
    size: number,
    options?: ReadableOptions,
  );
  push(data: ArrayBufferView): Promise<ArrayBufferView>;
  finalize(writable: Writable): Promise<void>;
  pipe(writable: Writable): Writable;
  toBlob(type?: string): Promise<Blob>;
  close(): Promise<void>;
  terminate(): Promise<void>;
  stop(force?: boolean): Promise<void>;
  read(): Promise<ArrayBufferView | null>;
  pull(): ArrayBufferView | undefined;
  checkStreamLimits(): void;
  waitEvent(event: PropertyKey): Promise<unknown>;
  [Symbol.asyncIterator](): AsyncIterableIterator<ArrayBufferView>;
}

export interface Transport {
  send(obj: object): void;
  write(data: string | ArrayBufferView): void;
}

export class WrpcWritable extends Emitter {
  id: string;
  name: string;
  size: number;
  transport: Transport;
  /** True once the transport closed: write() reports false, no 'drain' follows. */
  readonly closed: boolean;
  constructor(id: string, name: string, size: number, transport: Transport);
  init(): void;
  write(data: ArrayBufferView): boolean;
  end(): void;
  terminate(): void;
}

export interface BlobUploader {
  id: string;
  upload(): Promise<void>;
}

declare class ClientTransport extends Emitter {
  url: string;
  active: boolean;
  constructor(url: string);
  open(options?: WrpcClientOptions): Promise<void>;
  close(): void;
  send(obj: object): void;
  write(data: string | ArrayBufferView): void;
  online(): void;
  offline(): void;
}
export type { ClientTransport };

export class WrpcClient extends Emitter {
  static connections: Set<WrpcClient>;
  static isOnline: boolean;
  static online(): void;
  static offline(): void;
  static initialize(): void;
  static connect(
    url: string,
    options?: WrpcClientOptions,
  ): Promise<WrpcClient>;
  static transport: {
    ws: new (url: string) => ClientTransport;
    http: new (url: string) => ClientTransport;
    event: {
      getInstance(url: string): ClientTransport;
    };
  };

  url: string;
  api: Record<string, Emitter>;
  readonly active: boolean;

  constructor(
    url: string,
    transport: ClientTransport,
    options?: WrpcClientOptions,
  );
  open(): Promise<void>;
  close(): void;
  load(...units: Array<string>): Promise<void>;
  getStream(id: string): WrpcReadable | WrpcWritable;
  createStream(name: string, size: number): WrpcWritable;
  createBlobUploader(blob: Blob): BlobUploader;
  send(obj: object): void;
  write(data: string | ArrayBufferView): void;
}

export interface WrpcClientOptions {
  callTimeout?: number;
  reconnectTimeout?: number;
  worker?: ServiceWorker;
  proxy?: (data: string) => void;
}

export class WrpcClientProxy extends Emitter {
  constructor(options?: WrpcClientOptions);
  open(): Promise<void>;
  close(): void;
}

// ---------------------------------------------------------------------------
// Router / procedures

export type EventName = PropertyKey;

export interface State {
  [key: string]: unknown;
}

/**
 * A validator is a plain `(value) => value | throws` function (returning
 * undefined keeps the original value) or a Standard Schema object.
 */
export type Validator<T = unknown> =
  | ((value: T) => T | undefined | Promise<T | undefined>)
  | { '~standard': { validate(value: unknown): unknown } };

export type ProcedureHandler = (
  context: Context,
  args: any,
) => unknown | Promise<unknown>;

export interface QueueOptions {
  concurrency: number;
  size?: number;
  timeout?: number;
}

export interface ProcedureOptions {
  handler: ProcedureHandler;
  access?: 'public' | 'session' | string;
  input?: Validator;
  output?: Validator;
  /** Milliseconds; the call fails with code 408 when exceeded. */
  timeout?: number;
  /** Concurrency limit; overflow/starvation fails with code 503. */
  queue?: QueueOptions;
  meta?: Record<string, unknown>;
  /** Flat descriptor consumed by the type-codegen CLI. */
  signature?: Record<string, unknown>;
}

export declare class Procedure {
  handler: ProcedureHandler;
  access: string;
  input: Validator | null;
  output: Validator | null;
  timeout: number;
  meta: Record<string, unknown>;
  signature: Record<string, unknown> | null;
  constructor(options: ProcedureOptions);
  invoke(context: Context, args: unknown): Promise<unknown>;
}

export declare function procedure(
  options: ProcedureOptions | ProcedureHandler,
): Procedure;

/**
 * Unit keys are 'unit' or 'unit.version'; method values are procedures,
 * bare handler functions, or procedure option objects.
 */
export type RouterDefinition = Record<
  string,
  Record<string, Procedure | ProcedureHandler | ProcedureOptions>
>;

export interface MethodInfo {
  access: string;
  meta?: Record<string, unknown>;
  signature?: Record<string, unknown>;
}

export declare class Router {
  constructor(definition?: RouterDefinition);
  getProcedure(
    unit: string,
    version: string | undefined,
    method: string,
  ): Procedure | null;
  introspect(units?: Array<string> | null): Record<string, Record<string, MethodInfo>>;
  /** Returns a NEW router; on collision the other router's procedure wins. */
  merge(other: Router): Router;
}

export declare function defineRouter(definition: RouterDefinition): Router;

// ---------------------------------------------------------------------------
// Sessions

/** Structural store contract — anything with this shape plugs in. */
export interface SessionStore {
  get(token: string): Promise<State | null>;
  set(token: string, data: State): Promise<void>;
  delete(token: string): Promise<void>;
}

export interface MemorySessionStoreOptions {
  /** LRU cap; 0 disables it. Default 10000. */
  maxSessions?: number;
  /** Entry lifetime in ms; 0 disables expiry. Default 24h. */
  ttl?: number;
  now?: () => number;
}

/**
 * Bounded in-memory store (LRU + TTL). Sessions outlive their connection,
 * so production deployments should inject a real store instead.
 */
export declare class MemorySessionStore implements SessionStore {
  constructor(options?: MemorySessionStoreOptions);
  readonly size: number;
  get(token: string): Promise<State | null>;
  set(token: string, data: State): Promise<void>;
  delete(token: string): Promise<void>;
}

export declare class Session {
  token: string;
  state: State;
  constructor(token: string, data: State, save?: (data: State) => void);
}

export interface CookieOptions {
  name?: string;
  path?: string;
  httpOnly?: boolean;
  secure?: boolean;
  sameSite?: 'Strict' | 'Lax' | 'None';
  maxAge?: number | null;
}

export interface SessionsOptions {
  store?: SessionStore;
  cookie?: CookieOptions;
  generateToken?: () => string;
}

declare class SessionManager {
  store: SessionStore;
  generateToken: () => string;
  cookie: CookieOptions & { name: string; path: string };
  create(token?: string, data?: State): Session;
  restore(token: string): Promise<Session | null>;
  destroy(token: string): Promise<void>;
  cookieHeader(token: string): string;
  cookieDeleteHeader(): string;
  readToken(cookies: Record<string, string>): string | null;
}
export type { SessionManager };

export function createProxy<T extends object>(
  data: T,
  save?: (data: T) => void,
): T;

// ---------------------------------------------------------------------------
// Server core

export interface CorsOptions {
  origins?: Array<string> | ((origin: string) => boolean);
  credentials?: boolean;
  headers?: string;
  methods?: string;
}

export interface ErrorOptions {
  id?: string;
  error?: Error;
}

export declare class Context {
  client: Client;
  uuid: string;
  state: Record<string, unknown>;
  readonly session: Session | null;
  constructor(client: Client);
}

export class Client extends Emitter {
  source: string;
  session: Session | null;
  /** True for transports that stay open (WebSocket, worker port). */
  readonly persistent: boolean;
  /** Settles once the cookie-based session restore (if any) finished. */
  sessionReady: Promise<unknown>;
  streams: Map<string, WrpcReadable | WrpcWritable>;
  error(code: number, options?: ErrorOptions): void;
  send(obj: object, options?: { code?: number; method?: string }): void;
  createContext(): Context;
  emit(name: EventName, data?: unknown): Promise<void>;
  sendEvent(name: string, data?: unknown): void;
  getStream(id: string): WrpcReadable | WrpcWritable;
  createStream(name: string, size: number): WrpcWritable;
  initializeSession(token?: string, data?: State): boolean;
  finalizeSession(): Promise<boolean>;
  startSession(token?: string, data?: State): boolean;
  restoreSession(token: string): Promise<boolean>;
  close(): void;
  destroy(): void;
}

/** Abstract HTTP call description consumed by RpcServer.handleHttpCall. */
export interface HttpCall {
  method: string;
  url: string;
  headers: Record<string, string | undefined>;
  body?: string | Buffer | null;
  remoteAddress?: string;
  respond(response: {
    status: number;
    headers: Record<string, string | number | Array<string>>;
    body?: Buffer;
  }): void;
  /**
   * Registers a listener for a request that ends without a response, so
   * the core can evict its client. Adapters should wire it to their
   * request/response close signal.
   */
  onAbort?(listener: () => void): void;
}

export interface RpcServerOptions {
  router: Router;
  sessions?: SessionsOptions;
  cors?: CorsOptions | null;
  /** Default '/api'; '' serves from the root. */
  basePath?: string;
  console?: Console;
}

export declare class RpcServer extends Emitter {
  readonly router: Router;
  readonly sessions: SessionManager;
  readonly basePath: string;
  readonly clients: Set<Client>;
  constructor(options: RpcServerOptions);
  attachSocket(
    socket: WrpcSocket | Connection,
    meta?: { headers?: Record<string, string | undefined>; remoteAddress?: string },
  ): Client;
  attachPort(port: MessagePort): Client;
  handleHttpCall(call: HttpCall): Promise<void>;
  matchPath(pathname: string): { mode: 'packet' | 'rest'; rest?: string } | null;
  close(): Promise<void>;
}

export interface ServerOptions extends RpcServerOptions {
  host?: string;
  port?: number;
  protocol?: string;
  nagle?: boolean;
  key?: string;
  cert?: string;
  SNICallback?: Function;
  timeouts?: { bind?: number };
  retry?: number;
  engine?: Engine;
  /** Options forwarded to the engine's attach() (path, protocols, ...). */
  ws?: Omit<EngineAttachOptions, 'server'>;
}

export class Server extends Emitter {
  /** Null with a standalone engine, which owns the network stack itself. */
  httpServer: HttpServer | null;
  wsServer: EngineConnectionSource | null;
  rpc: RpcServer;
  constructor(options: ServerOptions);
  /** The bound address, whichever side owns the listener. */
  address(): { address: string; family: string; port: number } | string | null;
  listen(): Promise<Server>;
  close(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Transports

export interface TransportOptions {
  headers?: Record<string, string>;
}

export class ServerTransport extends Emitter {
  static transport: {
    http: typeof ServerHttpTransport;
    ws: typeof ServerWsTransport;
    event: typeof ServerEventTransport;
  };
  source: string;
  constructor(source: string);
  error(code?: number, options?: ErrorOptions): void;
  send(obj: object, code?: number): void;
}

declare class ServerHttpTransport extends ServerTransport {
  call: HttpCall;
  headers: Record<string, string>;
  readonly responded: boolean;
  constructor(call: HttpCall, options?: TransportOptions);
  write(data: string | Buffer, httpCode?: number): void;
  getCookies(): Record<string, string>;
  sendSessionCookie(cookieHeader: string): void;
  close(): void;
}
export type { ServerHttpTransport };

declare class ServerWsTransport extends ServerTransport {
  connection: WrpcSocket | Connection;
  constructor(
    connection: WrpcSocket | Connection,
    meta?: { remoteAddress?: string },
  );
  write(data: string | Buffer): boolean;
  close(): void;
}
export type { ServerWsTransport };

declare class ServerEventTransport extends ServerTransport {
  port: MessagePort;
  constructor(port: MessagePort);
  write(data: string | Buffer): void;
  close(): void;
}
export type { ServerEventTransport };

/** Per-request response headers: security defaults + CORS for `origin`. */
export function buildHeaders(
  cors?: CorsOptions | null,
  origin?: string,
): Record<string, string>;

export interface CallPacket {
  type: 'call';
  id: string;
  method: string;
  args: object;
}

export interface StreamPacket {
  type: 'stream';
  id: string;
  name?: string;
  size?: number;
  status?: 'end' | 'terminate';
}

export function chunkEncode(id: string, payload: Uint8Array): Uint8Array;
export function chunkDecode(chunk: Uint8Array): {
  id: string;
  payload: Uint8Array;
};
