import type { Engine, EngineAttachOptions } from '../engine/index.js';
import type { Context, RpcServer, RpcServerOptions, WrpcLogger } from '../../index.js';
import type { UwsApp } from './uws.js';

/**
 * Minimal structural view of the fastify instance the plugin needs. Typed
 * this way so `fastify` stays a devDependency-only type: the plugin never
 * imports it at runtime.
 */
export interface FastifyLike {
  server: unknown;
  /**
   * fastify's logger, used as the core's logger when `logger` is not passed.
   * fastify's default is a pino, which the core detects as structured and
   * calls natively — no adaptation in between.
   */
  log?: WrpcLogger;
  /**
   * Loosely typed on purpose: fastify's own RouteOptions is generic over the
   * server/request/reply/schema quintet, so a concrete parameter type here
   * would stop real FastifyInstance values from matching this view.
   */
  route(options: any): unknown;
  decorate(name: string, value: unknown): unknown;
  addHook(name: string, hook: (...args: any[]) => unknown): unknown;
  /** Used by the mirror feature to dispatch through the route pipeline. */
  inject?(options: any): Promise<any>;
}

export interface WrpcFastifyOptions extends Partial<RpcServerOptions> {
  /** Reuse an existing core instead of constructing one from `router`. */
  rpc?: RpcServer;
  /** Skips backend detection. */
  engine?: Engine;
  /**
   * Milliseconds calls in flight get to finish in when fastify closes (new
   * ones answer 503 meanwhile), before every open socket is told 1001 and
   * the clients are evicted — `Server#close({ drain })`. Default 0.
   */
  drain?: number;
  /** Forwarded to the engine's attach() (path, protocols, verifyClient, ...). */
  ws?: Omit<EngineAttachOptions, 'server'>;
  /**
   * Per-route `bodyLimit` for the RPC routes. Unlike the express and uws
   * adapters this plugin never reads the request stream — fastify parses the
   * body, so its own `bodyLimit` (1 MiB by default) already guards these
   * routes and answers `413 FST_ERR_CTP_BODY_TOO_LARGE`. Set this only to
   * narrow that limit; leaving it unset keeps the app's own.
   */
  maxBodySize?: number;
  /** Mirror the app's own routes as wrpc procedures; `true` takes defaults. */
  mirror?: MirrorOptions | boolean;
  /**
   * Error format on delegated REST routes: 'wrpc' (default) answers the wire
   * error object `{ message, code, details? }`; 'app' leaves errors to the
   * app's own fastify error handling.
   */
  restErrors?: 'wrpc' | 'app';
}

/**
 * Fastify plugin. Registers the RPC HTTP routes under `basePath` and
 * attaches a WebSocket engine chosen from what fastify runs on: a real
 * node http.Server gets the built-in engine, a fastify-uws server gets the
 * uWebSockets.js engine over the same app.
 *
 * Decorates the instance with `wrpc` (the RpcServer) and closes it on
 * fastify's `preClose` hook.
 */
export declare function wrpcFastify(fastify: FastifyLike, options?: WrpcFastifyOptions): Promise<void>;

/** A route as the mirror's filters and naming callbacks see it. */
export interface MirroredRoute {
  method: string;
  url: string;
  schema: object | null;
  config: Record<string, unknown>;
}

/**
 * Reverse engineering: existing fastify routes become wrpc procedures,
 * dispatched through `fastify.inject()` so the route's whole pipeline
 * (hooks, auth, validation, serialization) keeps running. Routes registered
 * AFTER the plugin are collected; per-route `config.wrpc` refines
 * (`{ unit, name }`) or opts out (`false`).
 */
export interface MirrorOptions {
  /** Extra filter on top of the defaults (own routes, HEAD/OPTIONS, wildcards). */
  include?(route: MirroredRoute): boolean;
  /** Overrides the default unit (the last static segment before the first param). */
  unit?(route: MirroredRoute): string | undefined;
  /** Overrides the reverse-REST default name (create/findAll/findById/...). */
  name?(route: MirroredRoute): string | undefined;
  /**
   * Maps the wrpc context into headers for the injected request (auth).
   * These always win over a header the caller sent under the same name.
   */
  headers?(context: Context): Record<string, string> | Promise<Record<string, string>>;
  /**
   * The header names a caller may pass to a mirrored procedure in
   * `args.headers` (`['x-request-id', 'accept-language']`). None by default:
   * the injected request arrives from inside the process, past whatever
   * strips client-supplied headers at the edge. Names the handshake never
   * lets a client declare (`cookie`, `host`, `x-forwarded-*`, …) are refused
   * here as well, listed or not.
   */
  allowHeaders?: Array<string>;
  /** Access level of the generated procedures. Default 'session'. */
  access?: 'public' | 'session';
}

/** Digs the uWebSockets.js app out of a fastify-uws server; null otherwise. */
export declare function findUwsApp(server: unknown): UwsApp | null;
