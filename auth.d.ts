// Hand-maintained types for the @alexify/wrpc/auth subpath: token stores
// (client), token transports (server), and the bearerAuth() composition.
// Structural throughout — nothing here imports the other declaration files.

import type { WrpcClient, RefreshHook, WrpcClientOptions } from './client.js';

/**
 * Where the client keeps tokens. Sync or async — IndexedDB is only
 * reachable through async, so every caller awaits either way. A `Map`
 * already satisfies the contract.
 */
export interface TokenStore {
  get(key: string): unknown | Promise<unknown>;
  set(key: string, value: unknown): unknown | Promise<unknown>;
  delete(key: string): unknown | Promise<unknown>;
}

export declare function isTokenStore(value: unknown): value is TokenStore;

/** In-memory, per-tab: a Map IS the contract. */
export declare function memoryStore(): Map<string, unknown>;

/** Structural view of localStorage/sessionStorage. */
export interface WebStorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

/** localStorage/sessionStorage-backed store; values JSON-encoded. */
export declare function webStorage(storage: WebStorageLike, options?: { prefix?: string }): TokenStore;

/**
 * A JS-readable cookie as the store (document.cookie) — NOT the HttpOnly
 * session cookie, which belongs to the server's cookie token transport.
 */
export declare function cookieStorage(
  doc: { cookie: string },
  options?: { prefix?: string; maxAge?: number; path?: string },
): TokenStore;

export interface BearerAuthOptions {
  store: TokenStore;
  /** Returns the token pair to store (`{ access, ... }`) or falsy for none. */
  signIn: (client: WrpcClient<any>, info: { reconnected: boolean; attempts: number }) => unknown;
  /** Receives the stored tokens; what it returns replaces them (falsy clears). */
  refresh?: (client: WrpcClient<any>, tokens: unknown, error: unknown) => unknown;
  /** The store key; default 'tokens'. */
  key?: string;
  /** Refresh trigger codes; default [401, 403]. */
  on?: Array<number>;
}

/**
 * The client-side composition: `headers` (Authorization on every open),
 * `authenticate` (signs in when the store is empty, awaited before the
 * reconnect restore) and single-flight `refresh` — ready to spread into
 * `connect(url, bearerAuth({ ... }))`.
 */
export declare function bearerAuth(options: BearerAuthOptions): Pick<
  WrpcClientOptions,
  'headers' | 'authenticate'
> & { refresh?: { on: Array<number>; handler: RefreshHook } };

/**
 * The server half (`sessions.transport`): token in
 * `Authorization: <scheme> <token>` — the real header where the transport
 * can send one, the `wrpc_h` connect-URL parameter on browser ws.
 * Non-ambient: exempt from the safe-method CSRF rule, `write()` stamps
 * nothing.
 */
export declare function bearerTransport(options?: { scheme?: string }): {
  ambient: false;
  read(request: { headers?: Record<string, string | Array<string> | undefined>; url?: string }): string | null;
  write(token: string): null;
};

/**
 * The server half for token-in-metadata: a field of the client's declared
 * `meta` option (`meta: { token }` / the x-wrpc-meta header).
 */
export declare function payloadTransport(options?: { field?: string }): {
  ambient: false;
  read(request: { headers?: Record<string, string | Array<string> | undefined>; url?: string }): string | null;
  write(token: string): null;
};
