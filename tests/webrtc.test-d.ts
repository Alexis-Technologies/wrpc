import { expectAssignable, expectError, expectType } from 'tsd';
import * as webrtc from '../webrtc.js';
import type {
  ChannelsOptions,
  ClientRtcTransport,
  Mesh,
  PeerHost,
  PeerLink,
  RosterSignaler,
  RtcAdapter,
  RtcLink,
  RtcLinkState,
  SignalMessage,
  Signaler,
  WrpcPeer,
  WrpcSignaler,
} from '../webrtc.js';
import type { AskResult, Client, ClientHost, Router, RouterDefinition, WrpcClient } from '../index.js';
import { connect, defineRouter, procedure } from '../index.js';

// The barrel: peer surface plus the Node-only signaling unit.
expectType<typeof WrpcPeer>(webrtc.WrpcPeer);
expectType<typeof PeerLink>(webrtc.PeerLink);
expectType<typeof Mesh>(webrtc.Mesh);
expectType<typeof PeerHost>(webrtc.PeerHost);
expectType<typeof RtcLink>(webrtc.RtcLink);
expectType<typeof ClientRtcTransport>(webrtc.ClientRtcTransport);
expectType<typeof WrpcSignaler>(webrtc.WrpcSignaler);
expectType<0>(webrtc.KIND_TEXT);
expectType<1>(webrtc.KIND_BINARY);
// A browser peer defines its router through the same functions the main barrel has.
expectType<typeof defineRouter>(webrtc.defineRouter);
expectType<typeof procedure>(webrtc.procedure);

// The signaling unit is a router fragment, spread into defineRouter.
expectType<RouterDefinition>(webrtc.createSignalingUnit());
const router = defineRouter(
  {
    ...webrtc.createSignalingUnit({
      access: 'public',
      relay: 'any',
      authorize: async (context, info) => {
        expectType<Client>(context.client);
        if (info.action === 'join') expectType<string>(info.room);
        else expectType<string>(info.to);
        return true;
      },
    }),
  },
  { hooks: webrtc.createSignalingHooks({ prefix: 'rtc:' }) },
);
expectType<Router>(router);
expectError(webrtc.createSignalingUnit({ access: 'admin' }));
expectError(webrtc.createSignalingUnit({ relay: 'everyone' }));

// The port is structural: any W3C-shaped constructor makes an adapter.
declare const RTCPeerConnection: unknown;
expectType<RtcAdapter>(webrtc.createW3cAdapter({ RTCPeerConnection }));
expectType<RtcAdapter>(webrtc.createW3cAdapter());
declare const maybeAdapter: unknown;
if (webrtc.isRtcAdapter(maybeAdapter)) expectType<RtcAdapter>(maybeAdapter);

// The signaler contract, and the built-in one over any WrpcClient.
declare const client: WrpcClient;
const signaler = webrtc.wrpcSignaler(client, { unit: 'signaling' });
expectAssignable<Signaler>(signaler);
expectAssignable<RosterSignaler>(signaler);
expectType<string | null>(signaler.id);
expectType<Promise<string>>(signaler.ready());
expectType<Promise<Array<{ id: string; data: unknown }>>>(signaler.join('lobby', { name: 'ada' }));
signaler.send('peer', { type: 'close' }, { room: 'lobby' });
expectError(signaler.send('peer', { type: 'offer' }));
expectAssignable<SignalMessage>({ type: 'connect' });
expectAssignable<SignalMessage>({ type: 'description', description: { type: 'offer', sdp: 'v=0' } });
// A hand-rolled signaler needs only the shape.
expectAssignable<Signaler>({
  id: null,
  ready: async () => 'me',
  send() {},
  on() {},
  off() {},
});

// The peer: a router, a signaler, an adapter; channels must match on both sides.
interface Api {
  chat: { hello: () => Promise<string> };
}
const peer = new webrtc.WrpcPeer({
  router,
  signaler,
  rtc: webrtc.createW3cAdapter(),
  iceServers: [{ urls: 'stun:stun.example' }],
  channels: { initiator: 0, responder: 1 },
  client: { heartbeat: false },
  host: { trust: 'none', highWaterMark: 65536 },
  redial: { retries: 3 },
  telemetry: { includeIdentity: false },
  accept: (from, room) => {
    expectType<string>(from);
    expectType<string | null>(room);
    return true;
  },
});
expectType<string | null>(peer.id);
expectType<PeerHost | null>(peer.host);
expectType<Map<string, PeerLink>>(peer.links);
expectAssignable<Required<ChannelsOptions>>(peer.channels);
expectError(new webrtc.WrpcPeer({ signaler, host: { trust: 'always' } }));
expectError(new webrtc.WrpcPeer({}));

async function usage() {
  const link = await peer.connect<Api>('other', { room: 'lobby' });
  expectType<PeerLink<Api>>(link);
  expectType<string>(await link.api.chat.hello());
  expectType<string>(link.id);
  expectType<boolean>(link.initiator);
  expectType<Client | null>(link.client);
  expectType<RtcLink>(link.link);
  expectType<RtcLinkState>(link.link.state);
  expectType<boolean>(link.link.initiator);
  expectType<Promise<PeerLink<Api>>>(link.ready());
  link.respond('poll', async (data) => data);
  expectType<Promise<unknown>>(link.ask('poll', {}, { timeout: 1000 }));
  link.join('team');
  link.close();

  const mesh = peer.join('lobby', { data: { name: 'ada' } });
  expectType<Mesh>(mesh);
  expectType<Set<string>>(mesh.peers);
  expectType<number>(mesh.broadcast('chat/note', { n: 1 }));
  expectType<AskResult>(await mesh.ask('poll', {}));
  mesh.respond('poll', () => 42);
  await mesh.leave();

  // The transport can also be named on an ordinary connect, given a link.
  const direct = await connect<Api>('webrtc:other', { transport: 'webrtc', link: link.link });
  expectType<WrpcClient<Api>>(direct);
}
void usage;

// A PeerHost is a ClientHost like an RpcServer: handlers reach rooms on both.
declare const host: PeerHost;
expectAssignable<ClientHost>(host);
expectType<number>(host.to('mesh:lobby').emit('x/y', 1));
expectType<Set<Client>>(host.clients);
expectType<boolean>(host.otel.enabled);
