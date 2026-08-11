import type { Client, HttpCall, WrpcLogger } from './index.js';

/**
 * `@alexify/wrpc/sse` — Server-Sent Events as a wrpc transport.
 *
 * SSE is one-way, so a channel is two halves that find each other by id:
 *
 *   GET  {basePath}/events?channel=<id>      server -> client stream
 *   POST {basePath} + x-wrpc-channel: <id>   client -> server
 *
 * Both halves belong to ONE server-side `Client`, which is what lets a
 * subscription opened by a POST deliver its values down the stream. A POST
 * answers `202` with no body — every reply, callbacks included, travels on
 * the stream.
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
 * bundle this subpath resolves to the client half alone.
 *
 * **Text only.** SSE frames are text, so wrpc's binary streams are refused
 * on this transport rather than silently corrupted — use a WebSocket for
 * those.
 */

/** One parsed event-stream event. */
export interface SseEvent {
  /** The last `id:` seen, or null if the stream has not sent one. */
  id: string | null;
  /** The `event:` name; 'message' when unnamed. */
  event: string;
  /** `data:` lines joined with newlines. */
  data: string;
}

/**
 * Incremental WHATWG event-stream parser. Feed it decoded chunks; it
 * returns the events each chunk completed. Handles fields split across
 * chunks, CRLF/CR/LF line endings, comments (the proxy heartbeat) and
 * multi-line data.
 */
export declare class SseParser {
  push(chunk: string): Array<SseEvent>;
}

declare class ClientTransportBase {
  url: string;
  active: boolean;
  heartbeat: boolean;
  open(options?: object): Promise<void>;
  close(): void;
  terminate(): void;
  send(obj: object): void;
  write(data: string): boolean;
}

/**
 * The client half: a `fetch`-based transport, registered as
 * `WrpcClient.transport.sse` on require. Deliberately not `EventSource`,
 * which cannot set headers, cannot be aborted cleanly, and reconnects on its
 * own schedule instead of the client's.
 */
export declare class ClientSseTransport extends ClientTransportBase {
  constructor(url: string);
  /** Identifies this channel to the server; sent on the stream and on POSTs. */
  readonly channelId: string;
  readonly eventsUrl: string;
}

/** The header a POST uses to say which event stream it belongs to. */
export declare const CHANNEL_HEADER: string;

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
      /** Internal: the channel registry's writer, built by RpcServer. */
      log?: WrpcLogger | boolean;
    },
  );
  readonly size: number;
  get(channelId: string): SseChannel | null;
  /**
   * Opens or re-attaches the server -> client half. The call must provide
   * `stream`; a host that cannot keep a response open gets a 501.
   *
   * `headers` are RESPONSE headers (CORS and the rest); the request headers a
   * new channel's client is built from come off `call` itself.
   */
  open(
    call: HttpCall,
    options?: { channelId?: string; lastEventId?: string | null; headers?: Record<string, string> },
  ): SseChannel | undefined;
  close(): void;
}
