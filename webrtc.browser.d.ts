// Types for the browser half of `@alexify/wrpc/webrtc` (`webrtc.browser.js`):
// everything a page needs to be a peer. The server-side signaling unit is
// Node machinery and lives in webrtc.d.ts, which re-exports this file — so
// the Node types are a superset, exactly like the runtime. Node-free: the
// router and Client types come from rpc.d.ts, the client's from client.d.ts.
import {
  ClientTransport,
  Emitter,
  WrpcClient,
  WrpcClientOptions,
  WrpcCodec,
  WrpcLogger,
  WrpcTelemetryOptions,
  WrpcWritable,
} from './client.js';
import { AskOptions, AskResult, Broadcast, Client, ClientHost, RoomRegistry, Router } from './rpc.js';

// A browser peer defines its router with these, which the main browser
// entry deliberately leaves out (its byte budget); they are the same
// functions the Node barrel exports.
export { defineRouter, procedure, tracked, createEventLog, buildDictionary } from './rpc.js';

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
  /**
   * Announced in every description this side sends (`caps`) — the
   * negotiation the channels have no handshake of their own for; what the
   * peer announced is `peerCaps`. WrpcPeer sets `{ deflate: id }` from its
   * `compression` option.
   */
  caps?: Record<string, unknown> | null;
}

/**
 * Events: 'state' (RtcLinkState), 'open' (both channels open), 'close',
 * 'error', 'channel-close' ({ which: 'client' | 'host' }), 'restart'
 * ({ outcome: 'requested' | 'recovered' | 'failed' }).
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
  /** What the peer's last description announced, or null. */
  readonly peerCaps: Record<string, unknown> | null;
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
  /**
   * Per-message compression on the channel (`src/compression`), off by
   * default. Over a link: announced in the description signal and applied
   * only once the peer named the same codec. Over a raw channel there is
   * no handshake — both applications turn it on, or neither; a plain peer
   * closes the channel on the first flagged frame, as on any reserved bit.
   */
  compression?: boolean | import('./client.js').CompressionOptions;
  /** bufferedAmount above which write() answers false. Default 1 MiB. */
  highWaterMark?: number;
  /** bufferedAmountLowThreshold, where 'drain' fires. Default 256 KiB. */
  lowWaterMark?: number;
  /**
   * Raw-channel mode only (a link negotiates its own): the message size to
   * fragment at. Default 16 KiB, the interop floor; `negotiateMessageSize(pc.sctp)`
   * for what the connection actually allows.
   */
  maxMessageSize?: number;
}

/**
 * A data channel the application owns, or a factory the transport asks for
 * one on every open() — how an application on the raw-channel level plugs
 * its own recovery into the client's reconnect cycle.
 */
export type ChannelSource = RtcDataChannelLike | (() => RtcDataChannelLike | Promise<RtcDataChannelLike>);

/**
 * The client half over a link's clientChannel — or over a raw data channel
 * — registered as `WrpcClient.transport.webrtc`: `connect('webrtc:<peer>',
 * { transport: 'webrtc', link })` or `{ transport: 'webrtc', channel }`.
 * Over a link, close() ends the LINK (goodbye, no redial) and terminate()
 * is local — the client's reconnect cycle re-opens on the link once it is
 * connected again. Over a raw channel both close the channel; a factory
 * hands the reconnect cycle its successor, a static channel is refused
 * once closed.
 */
export declare class ClientRtcTransport extends ClientTransport {
  /** The compression codec id in effect on the channel — both ends named it — or null. */
  readonly compression: string | null;
  constructor(url: string, options?: RtcTransportOptions & { link?: RtcLink | null; channel?: ChannelSource | null });
  readonly link: RtcLink | null;
  /** The channel spoken on; null before open() and after close. */
  readonly channel: RtcDataChannelLike | null;
}

/**
 * The host half over a link's hostChannel — what PeerHost.attach() takes —
 * or over a raw data channel — what RpcServer.attachChannel() builds.
 */
export declare class RtcPeerTransport extends Emitter {
  /** The compression codec id in effect on the channel — both ends named it — or null. */
  readonly compression: string | null;
  constructor(link: RtcLink, options: RtcTransportOptions & { peer: string; onError?: (error: Error) => void });
  /** `peer` defaults to the channel's label. */
  constructor(
    channel: RtcDataChannelLike,
    options?: RtcTransportOptions & { peer?: string; onError?: (error: Error) => void },
  );
  readonly kind: 'webrtc';
  readonly source: string;
  /** null over a raw channel. */
  readonly link: RtcLink | null;
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
  /** Binary attachments in packets (see RpcServerOptions.attachments). Default true; off under a codec. */
  attachments?: boolean;
  logger?: WrpcLogger | boolean;
  /**
   * Every id this host mints: its `instanceId` when one is not given, the
   * per-peer client ids, context uuids and stream ids. uuid v4 by default.
   * Validated once at construction — a function answering a non-empty
   * string of at most 255 characters.
   *
   * @deprecated-behaviour A non-function is reported through the logger and
   * replaced with the default; 2.0 makes it a TypeError.
   */
  generateId?: (() => string) | null;
  /** Merge `system/introspect` in (default true); false leaves the router as is. */
  introspection?: boolean;
  maxBatch?: number;
  maxSubscriptions?: number;
  maxCalls?: number;
  metaMaxBytes?: number;
  /**
   * 'link' (default): every attached Client gets a frozen pseudo-session
   * `{ token: peerId, data: { peer, room, ...data, claims? } }`, so
   * procedures with the default `access: 'session'` run — the link only
   * exists because the signaling server admitted the peer. 'assertion':
   * the same session, but `attach()` requires the peer's verified
   * assertion claims (`session.data.claims`). 'none' leaves `session` null.
   */
  trust?: PeerTrust;
  instanceId?: string | null;
  /** The same injection RpcServer takes: spans for answered calls, the connection gauge, the rtc instruments. */
  telemetry?: WrpcTelemetryOptions | null;
}

/**
 * A router, a dispatcher and one Client per attached peer — the pieces of
 * RpcServer a peer needs, with no sessions, cluster or HTTP. Satisfies
 * `ClientHost`, so a handler's `context.server.to(room).emit(...)` works
 * on a peer as on a server. Events: 'attach' (Client), 'detach' (Client).
 */
export type PeerTrust = 'link' | 'assertion' | 'none';

export declare class PeerHost extends Emitter implements ClientHost {
  constructor(options: PeerHostOptions);
  readonly router: Router;
  readonly rooms: RoomRegistry;
  readonly clients: Set<Client>;
  readonly instanceId: string;
  readonly trust: PeerTrust;
  /** The telemetry writer; `enabled` is false when nothing was injected. */
  readonly otel: { readonly enabled: boolean };
  getClient(id: string): Client | undefined;
  attach(
    transport: RtcPeerTransport,
    options: { peer: string; room?: string | null; data?: object | null; claims?: AssertionClaims | null },
  ): Client;
  to(...rooms: Array<string>): Broadcast;
  except(...clients: Array<Client>): Broadcast;
  broadcast(name: string, data?: unknown): number;
  close(): void;
}

// ---------------------------------------------------------------------------
// Signaling: the contract, and the built-in client half.

export interface SignalEvent {
  /** The sender's peer id. */
  from: string;
  /** The sender's incarnation, when the signaler carries one. */
  instance?: string | null;
  /** The sender's routable address (its signaling client id), when known. */
  address?: string | null;
  room: string | null;
  message: SignalMessage;
}

export interface RosterMember {
  id: string;
  instance?: string | null;
  address?: string | null;
  data: unknown;
}

export interface JoinEvent extends RosterMember {
  room: string;
}

/** Why a member left: it said so, its signaling connection dropped, or a newer connection took its id. */
export type LeaveReason = 'left' | 'disconnect' | 'replaced';

export interface LeaveEvent {
  room: string;
  id: string;
  instance?: string | null;
  address?: string | null;
  reason?: LeaveReason;
}

/** What WrpcPeer needs: an identity and a relay. Structural — anything with the shape qualifies. */
export interface Signaler {
  readonly id: string | null;
  /** This incarnation of the id: two signalers under one id are told apart by it. Optional. */
  readonly instance?: string | null;
  ready(): Promise<string>;
  send(to: string, message: SignalMessage, options?: { room?: string; address?: string }): void | Promise<void>;
  on(event: 'signal', handler: (event: SignalEvent) => void): unknown;
  on(event: string, handler: (...args: any[]) => void): unknown;
  off(event: string, handler: (...args: any[]) => void): unknown;
  close?(): void;
  /** Trust assertions: a signed token binding this peer's id to one of its certificates. */
  assert?(claims: { fingerprint: string }): Promise<AssertionIssued>;
  /** The server's public assertion keys. */
  keys?(): Promise<Array<JsonWebKey>>;
}

/** A Signaler whose server issues trust assertions (`hasAssertions`). */
export interface AssertingSignaler extends Signaler {
  assert(claims: { fingerprint: string }): Promise<AssertionIssued>;
  keys?(): Promise<Array<JsonWebKey>>;
}

/** What Mesh needs on top: rooms with a roster. */
export interface RosterSignaler extends Signaler {
  join(room: string, data?: unknown): Promise<Array<RosterMember>>;
  leave(room: string): Promise<void>;
  on(event: 'signal', handler: (event: SignalEvent) => void): unknown;
  on(event: 'join', handler: (event: JoinEvent) => void): unknown;
  on(event: 'leave', handler: (event: LeaveEvent) => void): unknown;
  on(event: 'reset', handler: (event: SignalerReset) => void): unknown;
  on(event: 'replaced', handler: (event: { id: string }) => void): unknown;
  on(event: string, handler: (...args: any[]) => void): unknown;
}

/**
 * The signaling connection came back and re-identified: rooms re-joined,
 * rosters fresh. Under a stable identity `id === previous` and the peer's
 * links stay; under a new id they are closed.
 */
export interface SignalerReset {
  id: string;
  previous: string | null;
  rooms: Array<{ room: string; members: Array<RosterMember> }>;
}

export declare const SIGNAL_MESSAGE_TYPES: ReadonlyArray<SignalMessage['type']>;
export declare function isSignalMessage(value: unknown): value is SignalMessage;
export declare function isSignaler(value: unknown): value is Signaler;
export declare function hasRoster(value: unknown): value is RosterSignaler;
export declare function hasAssertions(value: unknown): value is AssertingSignaler;

// ---------------------------------------------------------------------------
// Trust assertions: a JWS (compact, ES256) binding a peer id to the DTLS
// certificate fingerprint of the connection it dials with.

/** What an issuer answers: the token, and its `iat`/`exp` (seconds). */
export interface AssertionIssued {
  assertion: string;
  iat?: number;
  exp?: number;
}

/** The verified payload of an assertion: the reserved claims plus whatever the server added. */
export interface AssertionClaims {
  /** The peer id. */
  sub: string;
  iat: number;
  exp: number;
  /** The certificate fingerprint, `'sha-256 AB:CD:...'`. */
  fp: string;
  iss?: string;
  [claim: string]: unknown;
}

export type AssertionRefusal =
  | 'malformed'
  | 'typ'
  | 'alg'
  | 'kid'
  | 'signature'
  | 'subject'
  | 'fingerprint'
  | 'expired'
  | 'issuer'
  | 'missing';

/** Why an assertion was refused; `code` names the check that failed. */
export declare class AssertionError extends Error {
  readonly code: AssertionRefusal;
}

export interface AssertionVerifierOptions {
  /** Public EC P-256 JWKs — one, several (matched by `kid`), or a function answering them (asked again for an unknown kid). */
  keys: JsonWebKey | Array<JsonWebKey> | (() => Promise<Array<JsonWebKey>> | Array<JsonWebKey>);
  /** The `iss` every assertion must carry, when the issuer sets one. */
  issuer?: string | null;
  /** WebCrypto; defaults to `globalThis.crypto.subtle`. */
  subtle?: SubtleCrypto;
}

export interface AssertionVerifier {
  /** Verifies and binds: `sub` is `from`, `fp` is the fingerprint in `sdp`, `exp` is after `now`. */
  verify(token: string, context: { from: string; sdp: string; now?: number }): Promise<AssertionClaims>;
}

export declare function createAssertionVerifier(options: AssertionVerifierOptions): AssertionVerifier;
/** The `a=fingerprint:` an SDP declares for `algorithm` (default sha-256), normalized, or null. */
export declare function sdpFingerprint(sdp: string, algorithm?: string): string | null;
/** `'sha-256 ab:cd'` -> `'sha-256 AB:CD'`; null when the value is not a fingerprint. */
export declare function normalizeFingerprint(value: unknown): string | null;
/** True for anything shaped like a compact JWS of a sane size. */
export declare function isAssertion(value: unknown): value is string;

export interface WrpcSignalerOptions {
  /** The unit name on the server (default 'signaling'). */
  unit?: string;
  /**
   * The peer id this side proposes in `whoami`; the server's `identity`
   * strategy decides (the default strategy ignores it). A function is asked
   * on every identification.
   */
  identity?: string | (() => string | Promise<string>) | null;
  /** Generates the one `instance` of this signaler (default: a uuid). */
  generateId?: () => string;
}

/**
 * The client half of `createSignalingUnit`, over any WrpcClient (ws, sse,
 * ...). Events: 'signal', 'join', 'leave', 'reset', 'replaced', 'error'.
 */
export declare class WrpcSignaler extends Emitter implements RosterSignaler {
  constructor(client: WrpcClient<any>, options?: WrpcSignalerOptions);
  /** The peer id the server agreed to, or null before ready(). */
  readonly id: string | null;
  /** This incarnation of the id: generated once, sent with every whoami. */
  readonly instance: string;
  readonly client: WrpcClient<any>;
  readonly unit: string;
  /** The rooms joined through this signaler (a copy). */
  readonly rooms: Set<string>;
  /** True once a newer connection took this peer id; the signaler is over. */
  readonly replaced: boolean;
  /** The routable address last learned for a peer, or null. */
  addressOf(id: string): string | null;
  ready(): Promise<string>;
  send(to: string, message: SignalMessage, options?: { room?: string; address?: string }): void;
  join(room: string, data?: unknown): Promise<Array<RosterMember>>;
  leave(room: string): Promise<void>;
  members(room: string): Promise<Array<RosterMember>>;
  /** `<unit>/assert`: a trust assertion for one of this peer's certificates (the unit must issue them). */
  assert(claims: { fingerprint: string }): Promise<AssertionIssued>;
  /** `<unit>/keys`: the server's public assertion keys. */
  keys(): Promise<Array<JsonWebKey>>;
  on(event: 'signal', handler: (event: SignalEvent) => void): this;
  on(event: 'join', handler: (event: JoinEvent) => void): this;
  on(event: 'leave', handler: (event: LeaveEvent) => void): this;
  on(event: 'reset', handler: (event: SignalerReset) => void): this;
  on(event: 'replaced', handler: (event: { id: string }) => void): this;
  on(event: 'error', handler: (error: Error) => void): this;
  on(event: string, handler: (...args: any[]) => void): this;
  /** Detaches from the client; the client itself stays open. */
  close(): void;
}

export declare function wrpcSignaler(client: WrpcClient<any>, options?: WrpcSignalerOptions): WrpcSignaler;

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
  /**
   * Per-message compression on every link, both directions, off by default:
   * announced in each description this peer sends, applied on a link whose
   * peer named the same codec — a peer without it is served plain.
   */
  compression?: boolean | import('./client.js').CompressionOptions;
  connectTimeout?: number;
  restartTimeout?: number;
  redial?: RedialOptions | false;
  /**
   * Gates incoming links; return false (or throw) to refuse. `about` carries
   * the peer's incarnation and — with assertions, for an offer — its verified
   * claims (a knock carries none: they arrive with the answer).
   */
  accept?:
    | ((
        from: string,
        room: string | null,
        about: { instance: string | null; claims: AssertionClaims | null },
      ) => boolean | Promise<boolean>)
    | null;
  /**
   * Trust assertions: verify every peer's server-signed token against the
   * DTLS certificate of the link it arrives on, and get this peer's own
   * from `signaler.assert()` for every dial. `keys` defaults to
   * `signaler.keys()`. Required for `host.trust: 'assertion'`.
   */
  assertions?: { keys?: AssertionVerifierOptions['keys']; issuer?: string | null } | null;
  logger?: WrpcLogger | boolean;
  /**
   * Telemetry for the peer's server half and its links (the host's spans and
   * connection gauge, `wrpc.rtc.links` / `wrpc.rtc.redials` /
   * `wrpc.rtc.ice_restarts`). The client half of each link takes its own
   * through `client.telemetry`.
   */
  telemetry?: WrpcTelemetryOptions | null;
}

export type PeerLinkState = 'connecting' | 'open' | 'reconnecting' | 'closed';

/**
 * One connected peer, both directions. Events: 'open' (both directions
 * up, once), 'reconnect', 'close', 'state', 'attach' (the host-side Client
 * was (re)created), 'error'.
 */
export declare class PeerLink<Api = Record<string, Record<string, any>>> extends Emitter {
  readonly id: string;
  /** The remote peer's incarnation once known, else null; another one under the same id is another endpoint. */
  readonly instance: string | null;
  /** The remote peer's verified assertion claims, or null (no assertions, or none seen yet on the dialling side). */
  readonly claims: AssertionClaims | null;
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
 * outgoing), 'reset', 'replaced' (a newer connection took this peer id;
 * the peer closes), 'close', 'error' (error, source).
 */
export declare class WrpcPeer extends Emitter {
  constructor(options: WrpcPeerOptions);
  /** This peer's id: the signaler's, once start() resolved. */
  readonly id: string | null;
  readonly signaler: Signaler;
  readonly host: PeerHost | null;
  readonly router: Router | null;
  readonly channels: Required<ChannelsOptions>;
  /** True when this peer issues and verifies trust assertions. */
  readonly assertions: boolean;
  /** Every link, keyed by remote id (a copy). */
  readonly links: Map<string, PeerLink>;
  link(id: string): PeerLink | undefined;
  /** Identifies through the signaler; runs on its own on the first signal. */
  start(): Promise<string>;
  /**
   * A link to `remoteId`, dialled from either side; idempotent while one
   * exists, unless `instance` names another incarnation of the id — then
   * the stale link is abandoned and the new endpoint dialled.
   */
  connect<Api = Record<string, Record<string, any>>>(
    remoteId: string,
    options?: { room?: string | null; data?: unknown; instance?: string | null },
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
  /** Members whose signaling connection dropped while their link stayed up (a copy). */
  readonly away: Set<string>;
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
