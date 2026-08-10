import {
  IncomingMessage,
  Server as HttpServer,
  ServerResponse,
} from 'node:http';
import { Writable } from 'node:stream';
import type { Connection, WebsocketServer } from './ws.js';

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

export interface ApplicationContext {
  console: Console;
  auth: Auth;
  getMethod(unit: string, version: string, methodName: string): object | null;
}

export interface Options {
  host: string;
  port: number;
  protocol?: string;
  cors?: { origin: string };
  nagle?: boolean;
  key?: string;
  cert?: string;
  SNICallback?: Function;
  timeouts?: { bind?: number };
  retry?: number;
  websocketPath?: string;
}

export interface ErrorOptions {
  id?: string;
  error?: Error;
}

export interface Auth {
  generateToken(): string;
  saveSession(token: string, data: object): Promise<void>;
  createSession(token: string, data: object, fields?: object): Promise<void>;
  readSession(token: string): Promise<object | null>;
  deleteSession(token: string): Promise<void>;
  registerUser(login: string, password: string): Promise<object>;
  getUser(login: string): Promise<object>;
}

export type EventName = PropertyKey;

export class Client extends Emitter {
  source: string;
  session: Session | null;
  streams: Map<string, WrpcReadable | WrpcWritable>;
  error(code: number, options?: ErrorOptions): void;
  send(obj: object, options?: { code?: number; method?: string }): void;
  createContext(): Context;
  emit(name: EventName, data?: unknown): Promise<void>;
  sendEvent(name: string, data?: unknown): void;
  getStream(id: string): WrpcReadable | WrpcWritable;
  createStream(name: string, size: number): WrpcWritable;
  initializeSession(token: string, data?: object): boolean;
  finalizeSession(): boolean;
  startSession(token: string, data?: object): boolean;
  restoreSession(token: string): boolean;
  close(): void;
  destroy(): void;
}

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
  req: IncomingMessage;
  res: ServerResponse;
  headers: Record<string, string>;
  constructor(
    req: IncomingMessage,
    res: ServerResponse,
    options?: TransportOptions,
  );
  write(data: string | Buffer, httpCode?: number): void;
  options(): void;
  getCookies(): Record<string, string>;
  sendSessionCookie(token: string): void;
  removeSessionCookie(): void;
  close(): void;
}
export type { ServerHttpTransport };

declare class ServerWsTransport extends ServerTransport {
  connection: Connection;
  constructor(req: IncomingMessage, connection: Connection);
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

export function buildHeaders(cors?: { origin: string }): Record<string, string>;

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

export class Server extends Emitter {
  httpServer: HttpServer;
  wsServer: WebsocketServer | null;
  constructor(context: ApplicationContext, options: Options);
  listen(): Promise<Server>;
  close(): Promise<void>;
}

export interface State {
  [key: string]: unknown;
}

export declare class Session {
  token: string;
  state: State;
  constructor(token: string, data: State, context: ApplicationContext);
}

export declare class Context {
  client: Client;
  uuid: string;
  state: Record<string, unknown>;
  session: Session | null;
  constructor(client: Client);
}

export function createProxy<T extends object>(
  data: T,
  save?: (data: T) => void,
): T;

export function chunkEncode(id: string, payload: Uint8Array): Uint8Array;
export function chunkDecode(chunk: Uint8Array): {
  id: string;
  payload: Uint8Array;
};
