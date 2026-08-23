import { expectAssignable, expectError, expectType } from 'tsd';
import * as sse from '../sse.js';
import type { ClientSseTransport, SseChannels, SseEvent, SseOptions, SseParser, SseWriter } from '../sse.js';
import type { HttpCall, RpcServerOptions, Router } from '../index.js';

expectType<typeof SseParser>(sse.SseParser);
expectType<typeof ClientSseTransport>(sse.ClientSseTransport);
expectType<typeof SseChannels>(sse.SseChannels);
expectType<string>(sse.CHANNEL_HEADER);
expectType<Record<string, string>>(sse.SSE_HEADERS);

// The parser is incremental: a chunk yields the events it completed
const parser = new sse.SseParser();
expectType<Array<SseEvent>>(parser.push('data: hi\n\n'));
const [event] = parser.push('');
expectType<string | null>(event.id);
expectType<string>(event.event);
expectType<string>(event.data);

// The client transport is registered on require, and named at connect time.
// No channelId property: the id is server-minted and learned from the
// `ready` frame, never generated (or exposed) client-side.
const transport = new sse.ClientSseTransport('https://host/api');
expectType<string>(transport.eventsUrl);
expectType<boolean>(transport.active);
expectAssignable<{ transport?: string }>({ transport: 'sse' });
expectError(new sse.ClientSseTransport());

// SSE is configured through the core's `sse` option, or turned off
declare const router: Router;
expectAssignable<RpcServerOptions>({ router, sse: { retention: 60_000, replay: 200, heartbeat: 0 } });
expectAssignable<RpcServerOptions>({ router, sse: false });
expectError<RpcServerOptions>({ router, sse: { retention: 'soon' } });
expectAssignable<SseOptions>({ retry: 1000 });

// A call that can stream is what the events endpoint needs
expectAssignable<HttpCall>({
  method: 'GET',
  url: '/api/events',
  headers: {},
  respond: () => {},
  stream: () => null,
});
declare const writer: SseWriter;
expectType<boolean>(writer.write('data: x\n\n'));
expectType<void>(writer.end());
