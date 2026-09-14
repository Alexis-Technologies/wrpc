// Types for the browser half of `@alexify/wrpc/webrtc` (`webrtc.browser.js`):
// everything a page needs to be a peer. The server-side signaling unit is
// Node machinery and lives in webrtc.d.ts, which re-exports this file — so
// the Node types are a superset, exactly like the runtime. Node-free: the
// router and Client types come from rpc.d.ts, the client's from client.d.ts.
import { ClientTransport, Emitter, WrpcClient, WrpcClientOptions, WrpcCodec, WrpcLogger, WrpcWritable } from './client.js';
import { AskOptions, AskResult, Broadcast, Client, ClientHost, RoomRegistry, Router } from './rpc.js';

// A browser peer defines its router with these, which the main browser
// entry deliberately leaves out (its byte budget); they are the same
// functions the Node barrel exports.
export { defineRouter, procedure, tracked, createEventLog } from './rpc.js';

// ---------------------------------------------------------------------------
// The RTC port: wrpc's own W3C-shaped structural contract. A browser
// satisfies it natively; Node injects an implementation (node-datachannel's
// polyfill, a werift wrapper) — wrpc binds to none.

export interface RtcDescriptionLike {
  type: string;
  sdp?: string;
}

export interface RtcSctpLike {
  maxMessageSize: number;
}

export interface RtcDataChannelLike {
  readonly id: number | null;
  readonly label: string;
  readonly readyState: string;
  readonly bufferedAmount: number;
  bufferedAmountLowThreshold: number;
  binaryType: string;
  send(data: string | ArrayBuffer | ArrayBufferView): void;
  close(): void;
  addEventListener(type: string, listener: (event: any) => void): void;
  removeEventListener(type: string, listener: (event: any) => void): void;
}

export interface RtcPeerConnectionLike {
  readonly localDescription: RtcDescriptionLike | null;
  readonly signalingState: string;
  readonly connectionState: string;
  readonly iceConnectionState: string;
  readonly sctp: RtcSctpLike | null;
  createDataChannel(label: string, options?: { negotiated?: boolean; id?: number; ordered?: boolean }): RtcDataChannelLike;
  createOffer(options?: { iceRestart?: boolean }): Promise<RtcDescriptionLike>;
  createAnswer(): Promise<RtcDescriptionLike>;
  setLocalDescription(description?: RtcDescriptionLike): Promise<void>;
  setRemoteDescription(description: RtcDescriptionLike): Promise<void>;
  addIceCandidate(candidate: unknown): Promise<void>;
  restartIce?(): void;
  close(): void;
  addEventListener(type: string, listener: (event: any) => void): void;
  removeEventListener(type: string, listener: (event: any) => void): void;
}

export interface RtcAdapter {
  name?: string;
  createPeerConnection(configuration?: object): RtcPeerConnectionLike;
}

export declare function isRtcAdapter(value: unknown): value is RtcAdapter;
export declare function isRtcPeerConnection(value: unknown): value is RtcPeerConnectionLike;
export declare function isRtcDataChannel(value: unknown): value is RtcDataChannelLike;
/**
 * The adapter over any W3C-shaped `RTCPeerConnection` constructor: the
 * global one in a browser (the default), an injected one in Node —
 * `createW3cAdapter(require('node-datachannel/polyfill'))`. Throws a
 * TypeError when there is none.
 */
export declare function createW3cAdapter(source?: { RTCPeerConnection?: unknown }): RtcAdapter;

// ---------------------------------------------------------------------------
// Framing: one ordered channel carries text packets and binary chunks,
// each split into fragments of at most the negotiated message size behind
// a one-byte header (see docs/reference/protocol.md#webrtc-framing).

export declare const KIND_TEXT: 0;
export declare const KIND_BINARY: 1;

export declare class FramingError extends Error {
  name: 'FramingError';
  /** 'empty' | 'reserved' | 'kind' | 'too-large' | 'utf8' */
  code: string;
  constructor(message: string, code: string);
}

/** Fragment sink: the frame is valid only for the duration of the call. */
export type FrameSink = (frame: Uint8Array) => void;

export declare class FrameEncoder {
  constructor(maxMessageSize: number);
  encode(kind: 0 | 1, bytes: Uint8Array, sink: FrameSink): void;
  encodeText(text: string, sink: FrameSink): void;
}

export interface FramingOptions {
  /** Bytes a single message may reassemble to before the peer is cut off. Default 16 MiB. */
  maxReassembly?: number;
}

export declare class FrameDecoder {
  constructor(options?: FramingOptions);
  /** Bytes of the message being reassembled, 0 between messages. */
  readonly pending: number;
  /** One message per FIN fragment; null while a message is still incomplete. */
  push(input: ArrayBuffer | ArrayBufferView): { kind: 0; data: string } | { kind: 1; data: Uint8Array } | null;
  reset(): void;
}

/** The usable message size from what SCTP advertises: a 16 KiB floor, `ceiling` (default 256 KiB) on top. */
export declare function negotiateMessageSize(sctp: RtcSctpLike | null | undefined, ceiling?: number): number;

// ---------------------------------------------------------------------------
// The link: one peer connection, perfect negotiation, two negotiated data
// channels — one per client→host direction.

export interface ChannelsOptions {
  /** The data-channel id the initiator's client speaks on. Default 0. */
  initiator?: number;
  /** The data-channel id the responder's client speaks on. Default 1. */
  responder?: number;
  /** Both channels' label. Default 'wrpc'. */
  label?: string;
}

/** Negotiated channels are never described in SDP: BOTH peers must configure the same ids. */
export declare const DEFAULT_CHANNELS: Readonly<Required<ChannelsOptions>>;
export declare const MAX_CHANNEL_ID: number;
export declare function normalizeChannels(channels?: ChannelsOptions): Required<ChannelsOptions>;

export type SignalMessage =
  | { type: 'description'; description: RtcDescriptionLike }
  | { type: 'candidate'; candidate: unknown }
  | { type: 'close' }
  /** A knock from the non-initiator: "dial me". */
  | { type: 'connect' };

export type RtcLinkState = 'new' | 'connecting' | 'connected' | 'reconnecting' | 'failed' | 'closed';

export interface RtcLinkOptions {
  localId: string;
  remoteId: string;
  adapter: RtcAdapter;
  /** Delivers one signal to the remote peer; may return a promise. */
  signal: (message: SignalMessage) => void | Promise<void>;
  /** The RTCPeerConnection configuration (iceServers, ...). */
  configuration?: object;
  channels?: ChannelsOptions;
  /** ms for a dial to reach both channels open. Default 30000. */
  connectTimeout?: number;
  /** ms an ICE restart may take before the link fails. Default 15000. */
  restartTimeout?: number;
  log?: WrpcLogger | false;
}

/**
 * Events: 'state' (RtcLinkState), 'open' (both channels open), 'close',
 * 'error', 'channel-close' ({ which: 'client' | 'host' }).
 */
export declare class RtcLink extends Emitter {
  constructor(options: RtcLinkOptions);
  readonly localId: string;
  readonly remoteId: string;
  /** True when localId < remoteId: this side offers, restarts ICE and redials. */
  readonly initiator: boolean;
  /** The perfect-negotiation role; the initiator is impolite. */
  readonly polite: boolean;
  readonly state: RtcLinkState;
  readonly channels: Required<ChannelsOptions>;
  readonly pc: RtcPeerConnectionLike | null;
  /** The channel this side's WrpcClient writes on. */
  readonly clientChannel: RtcDataChannelLike | null;
  /** The channel the remote peer's WrpcClient writes on — this side's host reads it. */
  readonly hostChannel: RtcDataChannelLike | null;
  /** The negotiated message size once connected. */
  readonly maxMessageSize: number;
  readonly open: boolean;
  start(): void;
  /** A fresh connection after 'failed'; false when the link is in any other state. */
  redial(): boolean;
  /** Resolves when both channels are open; rejects on failed/closed/not started. */
  waitOpen(): Promise<void>;
  /** An ICE restart on the live connection. */
  restart(): void;
  receive(message: SignalMessage): Promise<void>;
  /** Tells the peer, closes the connection; idempotent. */
  close(): void;
}

// ---------------------------------------------------------------------------
// The two transports.

export interface RtcTransportOptions {
  framing?: FramingOptions;
  /** bufferedAmount above which write() answers false. Default 1 MiB. */
  highWaterMark?: number;
  /** bufferedAmountLowThreshold, where 'drain' fires. Default 256 KiB. */
  lowWaterMark?: number;
}

/**
 * The client half over a link's clientChannel, registered as
 * `WrpcClient.transport.webrtc`: `connect('webrtc:<peer>', { transport:
 * 'webrtc', link })`. close() ends the LINK (goodbye, no redial);
 * terminate() is local, and the client's reconnect cycle re-opens on the
 * link once it is connected again.
 */
export declare class ClientRtcTransport extends ClientTransport {
  constructor(url: string, options?: RtcTransportOptions & { link?: RtcLink | null });
  readonly link: RtcLink | null;
}

/** The host half over a link's hostChannel; what PeerHost.attach() takes. */
export declare class RtcPeerTransport extends Emitter {
  constructor(link: RtcLink, options: RtcTransportOptions & { peer: string; onError?: (error: Error) => void });
  readonly kind: 'webrtc';
  readonly source: string;
  readonly link: RtcLink;
  readonly channel: RtcDataChannelLike;
  connection: unknown;
  write(data: string | Uint8Array): boolean;
  close(): void;
}

// ---------------------------------------------------------------------------
// PeerHost: the server half of a peer, browser-safe.

export interface PeerHostOptions {
  router: Router;
  codec?: WrpcCodec | null;
  logger?: WrpcLogger | boolean;
  generateId?: (() => string) | null;
  /** Merge `system/introspect` in (default true); false leaves the router as is. */
  introspection?: boolean;
  maxBatch?: number;
  maxSubscriptions?: number;
  maxCalls?: number;
  metaMaxBytes?: number;
  /**
   * 'link' (default): every attached Client gets a frozen pseudo-session
   * `{ token: peerId, data: { peer, room, ...data } }`, so procedures with
   * the default `access: 'session'` run — the link only exists because the
   * signaling server admitted the peer. 'none' leaves `session` null.
   */
  trust?: 'link' | 'none';
  instanceId?: string | null;
}

/**
 * A router, a dispatcher and one Client per attached peer — the pieces of
 * RpcServer a peer needs, with no sessions, cluster or HTTP. Satisfies
 * `ClientHost`, so a handler's `context.server.to(room).emit(...)` works
 * on a peer as on a server. Events: 'attach' (Client), 'detach' (Client).
 */
export declare class PeerHost extends Emitter implements ClientHost {
  constructor(options: PeerHostOptions);
  readonly router: Router;
  readonly rooms: RoomRegistry;
  readonly clients: Set<Client>;
  readonly instanceId: string;
  readonly trust: 'link' | 'none';
  getClient(id: string): Client | undefined;
  attach(transport: RtcPeerTransport, options: { peer: string; room?: string | null; data?: object | null }): Client;
  to(...rooms: Array<string>): Broadcast;
  except(...clients: Array<Client>): Broadcast;
  broadcast(name: string, data?: unknown): number;
  close(): void;
}

// ---------------------------------------------------------------------------
// Signaling: the contract, and the built-in client half.

export interface SignalEvent {
  from: string;
  room: string | null;
  message: SignalMessage;
}

export interface RosterMember {
  id: string;
  data: unknown;
}

/** What WrpcPeer needs: an identity and a relay. Structural — anything with the shape qualifies. */
export interface Signaler {
  readonly id: string | null;
  ready(): Promise<string>;
  send(to: string, message: SignalMessage, options?: { room?: string }): void | Promise<void>;
  on(event: 'signal', handler: (event: SignalEvent) => void): unknown;
  on(event: string, handler: (...args: any[]) => void): unknown;
  off(event: string, handler: (...args: any[]) => void): unknown;
  close?(): void;
}

/** What Mesh needs on top: rooms with a roster. */
export interface RosterSignaler extends Signaler {
  join(room: string, data?: unknown): Promise<Array<RosterMember>>;
  leave(room: string): Promise<void>;
  on(event: 'signal', handler: (event: SignalEvent) => void): unknown;
  on(event: 'join', handler: (event: { room: string; id: string; data: unknown }) => void): unknown;
  on(event: 'leave', handler: (event: { room: string; id: string }) => void): unknown;
  on(event: 'reset', handler: (event: SignalerReset) => void): unknown;
  on(event: string, handler: (...args: any[]) => void): unknown;
}

/** The signaling connection came back under a new id: rooms re-joined, rosters fresh. */
export interface SignalerReset {
  id: string;
  previous: string | null;
  rooms: Array<{ room: string; members: Array<RosterMember> }>;
}

export declare const SIGNAL_MESSAGE_TYPES: ReadonlyArray<SignalMessage['type']>;
export declare function isSignalMessage(value: unknown): value is SignalMessage;
export declare function isSignaler(value: unknown): value is Signaler;
export declare function hasRoster(value: unknown): value is RosterSignaler;

/**
 * The client half of `createSignalingUnit`, over any WrpcClient (ws, sse,
 * ...). Events: 'signal', 'join', 'leave', 'reset', 'error'.
 */
export declare class WrpcSignaler extends Emitter implements RosterSignaler {
  constructor(client: WrpcClient<any>, options?: { unit?: string });
  readonly id: string | null;
  readonly client: WrpcClient<any>;
  readonly unit: string;
  /** The rooms joined through this signaler (a copy). */
  readonly rooms: Set<string>;
  ready(): Promise<string>;
  send(to: string, message: SignalMessage, options?: { room?: string }): void;
  join(room: string, data?: unknown): Promise<Array<RosterMember>>;
  leave(room: string): Promise<void>;
  members(room: string): Promise<Array<RosterMember>>;
  /** Detaches from the client; the client itself stays open. */
  close(): void;
}

export declare function wrpcSignaler(client: WrpcClient<any>, options?: { unit?: string }): WrpcSignaler;

// ---------------------------------------------------------------------------
// WrpcPeer, PeerLink, Mesh.

export interface RedialOptions {
  /** Redials (initiator) or knocks (responder) after a failure before the link closes. Default 5. */
  retries?: number;
  minDelay?: number;
  maxDelay?: number;
  factor?: number;
  jitter?: boolean;
}

export interface WrpcPeerOptions {
  /** What other peers can call; null makes a client-only peer. */
  router?: Router | null;
  signaler: Signaler;
  /** The RTC implementation; defaults to `createW3cAdapter()` over the global RTCPeerConnection. */
  rtc?: RtcAdapter | null;
  /** RTCPeerConnection configuration. */
  configuration?: object;
  /** Shorthand for `configuration.iceServers`. */
  iceServers?: Array<object>;
  /** Must match on both peers. */
  channels?: ChannelsOptions;
  /** Options of every link's remote WrpcClient (heartbeat, reconnect, codec, ...). */
  client?: WrpcClientOptions;
  /** PeerHost options (trust, codec, limits) plus the host transport's water marks. */
  host?: Omit<PeerHostOptions, 'router'> & Pick<RtcTransportOptions, 'highWaterMark' | 'lowWaterMark'>;
  framing?: FramingOptions;
  connectTimeout?: number;
  restartTimeout?: number;
  redial?: RedialOptions | false;
  /** Gates incoming links; return false (or throw) to refuse. */
  accept?: ((from: string, room: string | null) => boolean | Promise<boolean>) | null;
  logger?: WrpcLogger | boolean;
}

export type PeerLinkState = 'connecting' | 'open' | 'reconnecting' | 'closed';

/**
 * One connected peer, both directions. Events: 'open' (both directions
 * up, once), 'reconnect', 'close', 'state', 'attach' (the host-side Client
 * was (re)created), 'error'.
 */
export declare class PeerLink<Api = Record<string, Record<string, any>>> extends Emitter {
  readonly id: string;
  readonly room: string | null;
  /** The remote peer's roster data, when known. */
  readonly data: unknown;
  readonly initiator: boolean;
  readonly state: PeerLinkState;
  readonly open: boolean;
  readonly link: RtcLink;
  /** The WrpcClient that calls the remote peer's router. */
  readonly remote: WrpcClient<Api>;
  /** The remote's scaffolded api, after load(). */
  readonly api: WrpcClient<Api>['api'];
  /** The host-side Client the remote peer is on this router; null while down. */
  readonly client: Client | null;
  /** The host-side rooms this link is kept in across redials (a copy). */
  readonly rooms: Set<string>;
  /** Resolves once both directions are up (and stays settled); rejects when the link closes first. */
  ready(): Promise<this>;
  load(...units: Array<string>): Promise<WrpcClient<Api>['api']>;
  call(method: string, args?: unknown, options?: { timeout?: number; signal?: AbortSignal }): Promise<unknown>;
  /** Answers the remote peer's asks; replaces an earlier responder of that name. */
  respond(name: string, handler: (data: unknown) => unknown | Promise<unknown>): void;
  unrespond(name: string): boolean;
  /** An event to the remote peer, through this router's Client for it. */
  send(name: string, data?: unknown): void;
  /** Asks the remote peer; answered by its `remote.respond()`. */
  ask(name: string, data?: unknown, options?: AskOptions): Promise<unknown>;
  /** A binary stream to the remote peer. */
  createStream(name: string, size: number): WrpcWritable;
  /** Keeps this link's host Client in `room`, now and after every redial. */
  join(room: string): void;
  leave(room: string): void;
  /** Goodbye: the peer is told, both directions end, no redial. */
  close(): void;
}

/**
 * A wrpc peer: a router others call, a signaler to find them through, an
 * RTC adapter to reach them with. Events: 'link' (PeerLink, incoming or
 * outgoing), 'reset', 'close', 'error' (error, source).
 */
export declare class WrpcPeer extends Emitter {
  constructor(options: WrpcPeerOptions);
  /** This peer's id: the signaler's, once start() resolved. */
  readonly id: string | null;
  readonly signaler: Signaler;
  readonly host: PeerHost | null;
  readonly router: Router | null;
  readonly channels: Required<ChannelsOptions>;
  /** Every link, keyed by remote id (a copy). */
  readonly links: Map<string, PeerLink>;
  link(id: string): PeerLink | undefined;
  /** Identifies through the signaler; runs on its own on the first signal. */
  start(): Promise<string>;
  /** A link to `remoteId`, dialled from either side; idempotent while one exists. */
  connect<Api = Record<string, Record<string, any>>>(
    remoteId: string,
    options?: { room?: string | null; data?: unknown },
  ): Promise<PeerLink<Api>>;
  /** Joins a signaling room and links with everyone in it. Needs a RosterSignaler. */
  join(room: string, options?: { data?: unknown }): Mesh;
  mesh(room: string): Mesh | undefined;
  /** Closes every link and mesh; the signaler is the owner's to close. */
  close(): void;
}

/**
 * Everyone in a signaling room, linked to everyone. Events: 'join' ({ id,
 * data }, once the link is open), 'leave' ({ id }), 'link' (PeerLink),
 * 'left'.
 */
export declare class Mesh extends Emitter {
  readonly room: string;
  /** The host-side room every member link's Client is kept in: `mesh:<room>`. */
  readonly hostRoom: string;
  /** Ids of the members this peer has an open link with (a copy). */
  readonly peers: Set<string>;
  /** Every member link, open or still connecting (a copy). */
  readonly links: Map<string, PeerLink>;
  link(id: string): PeerLink | undefined;
  /** Resolves once the roster was fetched and every member link is dialling. */
  ready(): Promise<void>;
  /** One event to every open member; how many received it. */
  broadcast(name: string, data?: unknown): number;
  /** A question to every open member. */
  ask(name: string, data?: unknown, options?: AskOptions): Promise<AskResult>;
  /** Answers asks from every member, current and future. */
  respond(name: string, handler: (data: unknown) => unknown | Promise<unknown>): void;
  unrespond(name: string): boolean;
  /** Leaves the room; links no other mesh holds are closed. */
  leave(): Promise<void>;
}
