import type { Engine, EngineAttachOptions } from './engine.js';
import type { RpcServer, RpcServerOptions } from './index.js';
import type { UwsApp } from './uws.js';

/**
 * Minimal structural view of the fastify instance the plugin needs. Typed
 * this way so `fastify` stays a devDependency-only type: the plugin never
 * imports it at runtime.
 */
export interface FastifyLike {
  server: unknown;
  /**
   * fastify's logger, reused as the core's console when `console` is not
   * passed. Typed as a partial Console because fastify's default is pino,
   * which shares only part of the Console surface.
   */
  log?: Partial<Console>;
  /**
   * Loosely typed on purpose: fastify's own RouteOptions is generic over the
   * server/request/reply/schema quintet, so a concrete parameter type here
   * would stop real FastifyInstance values from matching this view.
   */
  route(options: any): unknown;
  decorate(name: string, value: unknown): unknown;
  addHook(name: string, hook: (...args: any[]) => unknown): unknown;
}

export interface WrpcFastifyOptions extends Partial<RpcServerOptions> {
  /** Reuse an existing core instead of constructing one from `router`. */
  rpc?: RpcServer;
  /** Skips backend detection. */
  engine?: Engine;
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
export declare function wrpcFastify(
  fastify: FastifyLike,
  options?: WrpcFastifyOptions,
): Promise<void>;

/** Digs the uWebSockets.js app out of a fastify-uws server; null otherwise. */
export declare function findUwsApp(server: unknown): UwsApp | null;
