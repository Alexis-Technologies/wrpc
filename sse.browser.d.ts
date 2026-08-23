// Types for the browser half of `@alexify/wrpc/sse` (`sse.browser.js`): the
// fetch-based client transport and the event-stream parser. The channel
// registry is server machinery and lives in sse.d.ts, which re-exports this
// file — so the server types are a superset, exactly like the runtime.
import { ClientTransport } from './client.js';

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

/**
 * The client half: a `fetch`-based transport, registered as
 * `WrpcClient.transport.sse` on require. Deliberately not `EventSource`,
 * which cannot set headers, cannot be aborted cleanly, and reconnects on its
 * own schedule instead of the client's.
 *
 * The channel id is SERVER-minted: the first GET carries none, the server
 * answers with a `ready` frame naming the channel, and only then can this
 * transport POST. A 409 means the channel is gone — the transport starts
 * over with a fresh one.
 */
export declare class ClientSseTransport extends ClientTransport {
  constructor(url: string);
  readonly eventsUrl: string;
}

/** The header a POST (or a re-attaching GET) names its channel with. */
export declare const CHANNEL_HEADER: string;
