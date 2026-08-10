import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Duplex } from 'node:stream';
import type { Engine, EngineAttachOptions, EngineConnectionSource } from './engine.js';
import type { RpcServer, RpcServerOptions } from './index.js';

export interface CreateWrpcOptions extends Partial<RpcServerOptions> {
  /** Reuse an existing core instead of constructing one from `router`. */
  rpc?: RpcServer;
  /** Must be a hosted engine — standalone engines own their own listener. */
  engine?: Engine;
  /** Forwarded to the engine's attach() (path, protocols, verifyClient, ...). */
  ws?: Omit<EngineAttachOptions, 'server'>;
  /** Cap for bodies this adapter reads itself. Default 10 MiB. */
  maxBodySize?: number;
}

export interface WrpcMiddleware {
  rpc: RpcServer;
  engine: Engine;
  wsServer: EngineConnectionSource;
  /**
   * express/connect middleware. Requests outside `basePath` fall through to
   * next() instead of 404 — wrpc composes with the app's other routes.
   */
  handler(req: IncomingMessage, res: ServerResponse, next: () => void): void;
  /** Wire to `httpServer.on('upgrade', wrpc.upgrade)`. */
  upgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void;
  close(): Promise<void>;
}

/**
 * Builds an express/connect (or bare node:http) integration: nothing here
 * owns the listener, the app does.
 *
 *   const wrpc = createWrpc({ router });
 *   app.use(wrpc.handler);
 *   app.listen(8000).on('upgrade', wrpc.upgrade);
 */
export declare function createWrpc(options?: CreateWrpcOptions): WrpcMiddleware;
