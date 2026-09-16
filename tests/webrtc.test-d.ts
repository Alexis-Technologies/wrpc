import { expectAssignable, expectError, expectType } from 'tsd';
import * as webrtc from '../webrtc.js';
import type {
  AssertionClaims,
  AssertionVerifier,
  ChannelsOptions,
  ClientRtcTransport,
  LeaveReason,
  RtcDataChannelLike,
  Mesh,
  PeerHost,
  PeerLink,
  RosterMember,
  RosterSignaler,
  RtcAdapter,
  RtcLink,
  RtcLinkState,
  SignalMessage,
  Signaler,
  WrpcPeer,
  WrpcSignaler,
} from '../webrtc.js';
import type { AskResult, Client, ClientHost, Router, RouterDefinition, RpcServer, WrpcClient } from '../index.js';
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
const signaler = webrtc.wrpcSignaler(client, { unit: 'signaling', identity: 'alice', generateId: () => 'tab-1' });
webrtc.wrpcSignaler(client, { identity: async () => 'alice' });
expectError(webrtc.wrpcSignaler(client, { identity: 42 }));
expectAssignable<Signaler>(signaler);
expectAssignable<RosterSignaler>(signaler);
expectType<string | null>(signaler.id);
expectType<string>(signaler.instance);
expectType<boolean>(signaler.replaced);
expectType<string | null>(signaler.addressOf('peer'));
expectType<Promise<string>>(signaler.ready());
expectType<Promise<Array<RosterMember>>>(signaler.join('lobby', { name: 'ada' }));
signaler.send('peer', { type: 'close' }, { room: 'lobby', address: 'node.1' });
expectError(signaler.send('peer', { type: 'offer' }));
signaler.on('leave', (event) => {
  expectType<string>(event.id);
  expectType<LeaveReason | undefined>(event.reason);
});
signaler.on('replaced', (event) => expectType<string>(event.id));
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
  accept: (from, room, about) => {
    expectType<string>(from);
    expectType<string | null>(room);
    expectType<string | null>(about.instance);
    expectType<AssertionClaims | null>(about.claims);
    return true;
  },
});
expectType<string | null>(peer.id);
expectType<PeerHost | null>(peer.host);
expectType<boolean>(peer.assertions);
// Trust assertions: verified on the peer, issued by the unit.
new webrtc.WrpcPeer({ router, signaler, host: { trust: 'assertion' }, assertions: {} });
new webrtc.WrpcPeer({ router, signaler, assertions: { keys: async () => [], issuer: 'sig' } });
expectError(new webrtc.WrpcPeer({ router, signaler, assertions: { keys: 'jwk' } }));
const verifier: AssertionVerifier = webrtc.createAssertionVerifier({ keys: [], issuer: null });
expectType<Promise<AssertionClaims>>(verifier.verify('a.b.c', { from: 'x', sdp: 'v=0', now: 1 }));
expectType<string | null>(webrtc.sdpFingerprint('v=0'));
expectType<boolean>(webrtc.hasAssertions(signaler));
expectType<Promise<{ assertion: string; iat?: number; exp?: number }>>(signaler.assert({ fingerprint: 'sha-256 AA' }));
expectType<Promise<Array<JsonWebKey>>>(signaler.keys());
async function issuing() {
  const keys = await webrtc.generateAssertionKeys({ kid: 'k1' });
  expectType<JsonWebKey>(keys.privateKey);
  const issuer = webrtc.createAssertionIssuer({ key: keys.privateKey, ttl: 60 });
  expectType<Promise<Array<JsonWebKey>>>(issuer.publicKeys());
  const issued = await issuer.sign({ sub: 'alice', fp: 'sha-256 AA', role: 'host' });
  expectType<string>(issued.assertion);
  webrtc.createSignalingUnit({ assertions: { key: keys.privateKey, claims: () => ({ role: 'x' }) } });
  expectError(webrtc.createSignalingUnit({ assertions: { key: 'nope' } }));
}
void issuing;
peer.on('replaced', (event: { id: string }) => event.id);
expectType<Map<string, PeerLink>>(peer.links);
expectAssignable<Required<ChannelsOptions>>(peer.channels);
expectError(new webrtc.WrpcPeer({ signaler, host: { trust: 'always' } }));
expectError(new webrtc.WrpcPeer({}));

async function usage() {
  const link = await peer.connect<Api>('other', { room: 'lobby' });
  expectType<PeerLink<Api>>(link);
  expectType<string>(await link.api.chat.hello());
  expectType<string>(link.id);
  expectType<string | null>(link.instance);
  expectType<AssertionClaims | null>(link.claims);
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

  // Or a raw data channel the application negotiated — static, or a factory
  // the reconnect cycle asks for the next one.
  const raw = await connect<Api>('webrtc:server', { transport: 'webrtc', channel: dataChannel, maxMessageSize: 65536 });
  expectType<WrpcClient<Api>>(raw);
  await connect<Api>('webrtc:server', { transport: 'webrtc', channel: async () => dataChannel });
  const clientHalf = new webrtc.ClientRtcTransport('webrtc:server', { channel: () => dataChannel });
  expectType<RtcDataChannelLike | null>(clientHalf.channel);
  expectType<RtcLink | null>(clientHalf.link);
  const hostHalf = new webrtc.RtcPeerTransport(dataChannel);
  expectType<RtcLink | null>(hostHalf.link);
  expectType<RtcDataChannelLike>(hostHalf.channel);
  new webrtc.RtcPeerTransport(dataChannel, { peer: 'browser', maxMessageSize: 65536 });
  new webrtc.RtcPeerTransport(link.link, { peer: 'other' });
  expectError(new webrtc.RtcPeerTransport(link.link));
}
void usage;
declare const dataChannel: RtcDataChannelLike;

// The attachPort of WebRTC: a raw channel on an RpcServer, through the Node barrel.
declare const rpc: RpcServer;
expectType<Client>(webrtc.attachChannel(rpc, dataChannel));
expectType<Client>(
  webrtc.attachChannel(rpc, dataChannel, {
    peer: 'browser',
    headers: { 'x-a': '1' },
    data: { u: 1 },
    maxMessageSize: 65536,
  }),
);
expectError(webrtc.attachChannel(rpc, dataChannel, { peer: 42 }));
expectError(webrtc.attachChannel({}, dataChannel));

// A PeerHost is a ClientHost like an RpcServer: handlers reach rooms on both.
declare const host: PeerHost;
expectAssignable<ClientHost>(host);
expectType<number>(host.to('mesh:lobby').emit('x/y', 1));
expectType<Set<Client>>(host.clients);
expectType<boolean>(host.otel.enabled);
