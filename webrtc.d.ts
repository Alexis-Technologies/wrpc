import type { Client, ClientMeta, Context, RouterDefinition, ConnectionHook } from './rpc.js';
import type { RtcDataChannelLike, RtcPeerTransport, RtcTransportOptions } from './webrtc.browser.js';

/**
 * `@alexify/wrpc/webrtc` — peer-to-peer wrpc over WebRTC data channels.
 *
 * Two browsers (or a browser and a Node process with an injected RTC
 * implementation) each serve a router and call the other's, over ONE
 * RTCPeerConnection carrying two negotiated data channels — one per
 * client→host direction — so the ordinary WrpcClient and dispatcher speak
 * across the link unchanged: calls, events, ask/respond, subscriptions with
 * resume, binary streams, heartbeat and reconnect.
 *
 * ```js
 * // the signaling server (any wrpc server)
 * const router = defineRouter({ ...createSignalingUnit() }, { hooks: createSignalingHooks() });
 *
 * // a peer (browser or Node)
 * const client = await connect('wss://host/api');
 * const peer = new WrpcPeer({ router: myRouter, signaler: wrpcSignaler(client) });
 * const mesh = peer.join('lobby');
 * mesh.on('join', ({ id }) => mesh.link(id).load('chat'));
 * ```
 *
 * In a browser bundle this subpath resolves to the peer half alone
 * (webrtc.browser.d.ts); the server-side signaling unit below is Node-only.
 */

export * from './webrtc.browser.js';

export interface SignalingUnitOptions {
  /** The unit name (default 'signaling'); the client helper must agree. */
  name?: string;
  /** Applied to every method and the signal event. Default 'session'. */
  access?: 'session' | 'public';
  /**
   * The peer id of a connection — a user id from the session, say — with
   * the id the client proposed in `whoami` as one input. Default: the
   * connection's client id, the proposal ignored.
   */
  identity?: ((context: Context, info: { proposed: string | null }) => string | Promise<string>) | null;
  /**
   * A second connection on this instance identifying as an id already held:
   * 'replace' (default) hands the id over and tells the first `replaced`;
   * 'refuse' answers it 409. Node-local — a cluster-wide claim belongs in
   * the identity strategy.
   */
  duplicate?: 'replace' | 'refuse';
  /**
   * Runs before a join and before every relayed signal. Return false to
   * refuse with 403, or throw a coded error of your own.
   */
  authorize?:
    | ((
        context: Context,
        info:
          | { action: 'join'; room: string; data: unknown }
          | { action: 'signal'; room: string | null; to: string; message: unknown },
      ) => boolean | void | Promise<boolean | void>)
    | null;
  /** 'room' (default): a signal reaches `to` only while both share the room; 'any': any connected id. */
  relay?: 'room' | 'any';
  /** The room-registry namespace signaling rooms live under. Default 'rtc:'. */
  prefix?: string;
  /**
   * Issue trust assertions: adds `assert({ fingerprint })` and the public
   * `keys()` to the unit. `key` is a private EC P-256 JWK
   * (`generateAssertionKeys().privateKey`) or a CryptoKey pair; `claims`
   * adds claims of your own (roles, say) to every token.
   */
  assertions?: AssertionIssuerOptions & {
    claims?: (context: Context) => object | null | Promise<object | null>;
  };
}

export interface AssertionIssuerOptions {
  key: JsonWebKey | { privateKey: CryptoKey; publicKey: CryptoKey };
  /** The `kid` put in every header (default: the JWK's). */
  kid?: string;
  /** Seconds an assertion is valid for. Default 300. */
  ttl?: number;
  /** The `iss` claim, when verifiers expect one. */
  issuer?: string | null;
  subtle?: SubtleCrypto;
}

export interface AssertionIssuer {
  readonly kid: string | null;
  readonly ttl: number;
  sign(claims: { sub: string; fp: string; [claim: string]: unknown }): Promise<{ assertion: string; iat: number; exp: number }>;
  publicKeys(): Promise<Array<JsonWebKey>>;
}

/** The issuing half of trust assertions (see `createAssertionVerifier` for the other). */
export declare function createAssertionIssuer(options: AssertionIssuerOptions): AssertionIssuer;

/** A fresh ES256 key pair as JWKs, `kid` on both. Keep the private one private. */
export declare function generateAssertionKeys(options?: {
  kid?: string;
  subtle?: SubtleCrypto;
}): Promise<{ kid: string; privateKey: JsonWebKey; publicKey: JsonWebKey }>;

/**
 * The built-in signaling unit as a router definition fragment — spread it
 * into `defineRouter`: `whoami`, `join`, `leave`, `members`, and the
 * inbound `signal` event relayed through `RpcServer.sendTo`. A peer's id is
 * what `identity` says — the connection's client id by default; rosters
 * and signals carry each peer's routable `address` and `instance`.
 */
export declare function createSignalingUnit(options?: SignalingUnitOptions): RouterDefinition;

/**
 * The router-level `onDisconnect` hook that announces a dropped signaling
 * connection's leave (`reason: 'disconnect'`) to the rooms it was in —
 * unless a newer connection took its id meanwhile. `name`/`prefix` must
 * match the unit's.
 */
export declare function createSignalingHooks(options?: { name?: string; prefix?: string }): {
  onDisconnect: ConnectionHook;
};

export interface AttachChannelOptions
  extends Pick<RtcTransportOptions, 'maxMessageSize' | 'framing' | 'highWaterMark' | 'lowWaterMark' | 'compression'> {
  /** The client's `source`; defaults to the channel's label. */
  peer?: string;
  /** Observed about the connection by the application; lands in `context.meta`. */
  headers?: Record<string, string>;
  data?: Record<string, unknown>;
  remoteAddress?: string;
}

/** What attachChannel needs of a server: RpcServer's `attach`. */
export interface AttachingServer {
  attach(transport: RtcPeerTransport, options?: { meta?: ClientMeta | null }): Client;
}

/**
 * The attachPort of WebRTC: a raw data channel the application negotiated
 * itself, attached to an ordinary RpcServer — sessions, rooms, cluster and
 * all — reachable from a browser with `connect(url, { transport: 'webrtc',
 * channel })`. Builds the host half of the transport over the channel and
 * hands it to `server.attach`. No session at attach (a channel carries no
 * request); no ICE restart or redial (the peer connection is the
 * application's).
 */
export declare function attachChannel(
  server: AttachingServer,
  channel: RtcDataChannelLike,
  options?: AttachChannelOptions,
): Client;
