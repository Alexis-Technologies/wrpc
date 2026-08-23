import type { Client, HttpCall, WrpcLogger } from './index.js';

/**
 * `@alexify/wrpc/sse` — Server-Sent Events as a wrpc transport.
 *
 * SSE is one-way, so a channel is two halves that find each other by id:
 *
 *   GET  {basePath}/events                        opens a NEW channel
 *   GET  {basePath}/events  x-wrpc-channel: <id>  re-attaches to it
 *   POST {basePath}         x-wrpc-channel: <id>  client -> server
 *
 * The id is SERVER-minted and handed out once, in the `ready` frame; the
 * channel is bound to the cookie identity of the GET that created it, and
 * every re-attach and POST must present the same one (403 otherwise; 409
 * when the id is unknown). Both halves belong to ONE server-side `Client`,
 * which is what lets a subscription opened by a POST deliver its values
 * down the stream. A POST answers `202` with no body — every reply,
 * callbacks included, travels on the stream.
 *
 * Requiring this module registers the client transport:
 *
 * ```js
 * require('@alexify/wrpc/sse');
 * const client = await WrpcClient.connect('https://host/api', { transport: 'sse' });
 * ```
 *
 * The server half is built into `RpcServer` and configured through its `sse`
 * option; what is exported here is the machinery behind it. In a browser
 * bundle this subpath resolves to the client half alone (sse.browser.d.ts).
 *
 * **Text only.** SSE frames are text, so wrpc's binary streams are refused
 * on this transport rather than silently corrupted — use a WebSocket for
 * those.
 */

// The client half (transport, parser, header) is shared with the browser
// types condition verbatim.
export * from './sse.browser.js';

/** Response headers every event stream carries. */
export declare const SSE_HEADERS: Record<string, string>;

export interface SseWriter {
  write(chunk: string): boolean;
  end(): void;
  onClose?(listener: () => void): void;
  onDrain?(listener: () => void): void;
}

export interface SseOptions {
  /**
   * How long a channel outlives its stream, in ms. A dropped stream is
   * usually a blip, not a goodbye: within this window a reconnect
   * re-attaches and replays instead of starting over. Default 30000.
   */
  retention?: number;
  /** Outbound frames kept for Last-Event-ID replay. Default 100. */
  replay?: number;
  /** Comment-frame interval in ms; 0 disables. Default 15000. */
  heartbeat?: number;
  /** The `retry:` value handed to the peer, in ms. Default 2000. */
  retry?: number;
  /** Byte budget for the replay buffer; evicts oldest first. Default 1 MiB. */
  replayBytes?: number;
  /** Live channels per server; past it a new GET is 503. Default 10000. */
  maxChannels?: number;
  /**
   * Live channels per remote address; past it 429. Default 100. Behind a
   * proxy the default address is the PROXY — see `clientAddress`.
   */
  maxChannelsPerAddress?: number;
  /**
   * What the per-address cap counts by; defaults to the TCP peer address.
   * Behind a load balancer inject a reader for your proxy's client header
   * (trust it only when the proxy is yours).
   */
  clientAddress?: (call: { headers?: Record<string, unknown>; remoteAddress?: string }) => string;
}

/** The server-side transport behind one event stream. Text-only. */
export declare class ServerSseTransport {
  readonly channelId: string;
  readonly binary: false;
  readonly attached: boolean;
  write(data: string): boolean;
  close(): void;
}

export declare class SseChannel {
  readonly id: string;
  /**
   * The identity the channel was created under: the session token read from
   * the opening GET's cookie, or '' for an anonymous peer. Every re-attach
   * and POST must present it again — the id alone is never enough.
   */
  readonly key: string;
  readonly client: Client;
  readonly transport: ServerSseTransport;
  /** Replays every buffered frame newer than `lastEventId`. */
  resume(lastEventId: string | number): void;
}

/**
 * The channel registry. `RpcServer` owns one and exposes it as `.sse`;
 * a host driving SSE itself can use it directly.
 */
export declare class SseChannels {
  constructor(
    options: SseOptions & {
      /**
       * Builds the one `Client` both halves of a channel share. `headers` are
       * the request headers of the GET that opened the stream — a channel's
       * only handshake, and where its session cookie comes from.
       */
      addClient(transport: ServerSseTransport, headers: Record<string, string>): Client;
      /**
       * Extracts the identity a request presents (the session token from its
       * cookie). Injected by RpcServer, which owns the SessionManager.
       */
      channelKey?(headers: Record<string, string | undefined>): string;
      /** Internal: the channel registry's writer, built by RpcServer. */
      log?: WrpcLogger | boolean;
    },
  );
  readonly size: number;
  get(channelId: string): SseChannel | null;
  /** True when `headers` present the identity the channel was created under. */
  authorized(channel: SseChannel, requestHeaders?: Record<string, string | undefined>): boolean;
  /**
   * Opens or re-attaches the server -> client half. The call must provide
   * `stream`; a host that cannot keep a response open gets a 501. Ids are
   * server-minted: an unknown `channelId` answers 409, a known one with the
   * wrong cookie identity 403, and creation past the caps 503/429.
   *
   * `headers` are RESPONSE headers (CORS and the rest); the request headers a
   * new channel's client is built from come off `call` itself.
   */
  open(
    call: HttpCall,
    options?: { channelId?: string | null; lastEventId?: string | null; headers?: Record<string, string> },
  ): SseChannel | undefined;
  close(): void;
}
