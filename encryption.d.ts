// Types for the `@alexify/wrpc/encryption` subpath on Node: the primitives
// of the browser half (the AEADs synchronous over node:crypto here), plus
// what only a Node process does as it lands.

/**
 * `@alexify/wrpc/encryption` — application-level encryption, opt-in and
 * never in place of TLS.
 *
 * @experimental The whole subpath may change in a minor (see
 * docs/reference/stability.md): these types, the `encryption` options they
 * feed on the server, the client, rooms, the cluster, the broker bindings
 * and the session store, and the wire formats of docs/reference/protocol.md
 * `#session-encryption`, `#sealed-requests` and `#broker-sealing`.
 */
export * from './encryption.browser.js';

import type {
  Cipher,
  CipherAlgorithm,
  EncryptionInfo,
  KeyProvider,
  KeysOption,
  NoisePattern,
} from './encryption.browser.js';
import type { SessionStore } from './rpc.js';
import type { WrpcLogger } from './client.js';

/**
 * `encryption` on a Node↔Node carrier — `rooms.encryption`,
 * `cluster.encryption`: every envelope sealed under a shared keyring, so
 * the backplane and whoever operates it carry ciphertext. Off by default.
 *
 * A pub/sub backplane delivers at most once, so the switch is a rollout of
 * three deploys, never a flag day: `{ seal: false, acceptPlaintext: true }`
 * (every instance learns to open), `{ acceptPlaintext: true }` (every
 * instance seals), then neither (plaintext is refused).
 */
export interface EnvelopeEncryptionOptions {
  /** One key, a ring with its current kid, or a provider. The kid travels in the clear. */
  keys: KeysOption;
  /**
   * Default `'aes-256-gcm'`. Either built-in is opened whichever one this
   * instance seals with, so changing it is a config change, not a rollout;
   * an injected `Cipher` must answer synchronously and be the same on every
   * instance.
   */
  cipher?: CipherAlgorithm | Cipher;
  /** Default true. `false` opens sealed envelopes but still publishes plaintext. */
  seal?: boolean;
  /** Default false: an envelope that is not sealed is dropped and logged (`*.unsealed`). */
  acceptPlaintext?: boolean;
  /**
   * The anti-replay window kept per sender, in messages: a repeated or
   * too-old counter is dropped and logged. Default 1024; `false` disables
   * it. Kept in memory, so it starts empty with the process.
   */
  replayWindow?: number | false;
}

/** A provider for the sealed store must answer `kids()`: a rotation is walked through it. */
export type SealedStoreKeys = Exclude<KeysOption, KeyProvider> | (KeyProvider & { kids(): ReadonlyArray<string> });

export interface SealedStoreOptions {
  /** One key, a ring with its current kid, or a provider that answers `kids()` (a TypeError otherwise). */
  keys: SealedStoreKeys;
  /**
   * Whether writes are sealed. `false` is the first of the three deploys
   * over a store that already holds sessions: every instance reads sealed
   * rows already, none writes them yet, so a rollback finds every session
   * where it always was. Default true.
   */
  seal?: boolean;
  /** Default `'aes-256-gcm'`; an injected `Cipher` must answer synchronously. */
  cipher?: CipherAlgorithm | Cipher;
  /**
   * The first deploy over a store that already holds sessions: a row the
   * unwrapped store wrote is read once, sealed, and its plaintext deleted.
   * Default false. Turn it off again once the longest session TTL passed.
   */
  acceptPlaintext?: boolean;
  /** `session.unsealed`, `session.open`, `session.migrate` warnings. The token is never logged. */
  logger?: WrpcLogger | boolean;
}

/**
 * A session store that holds nothing readable — neither a session's state
 * nor its token. Rows are keyed by an HMAC of the token and hold the state
 * sealed, with the row's key as additional data: a row copied into another
 * session's slot does not open. A key rotation signs nobody out — a row
 * found under an older kid moves to the current one on its next read, so a
 * kid can be dropped once the longest session TTL passed since it stopped
 * being current. `touch` is forwarded when the wrapped store has one.
 *
 *   sessions: { store: sealedStore(createRedisSessionStore({ client }), { keys }) }
 */
export declare function sealedStore(store: SessionStore, options: SealedStoreOptions): SessionStore;

/**
 * `encryption` on a server: session encryption of its persistent
 * connections (WebSocket, WebTransport) — a Noise handshake, then every
 * frame sealed. A client opts in with `createEncryption()`; with `required`
 * nothing plaintext is accepted on any transport. Off by default, and never
 * a substitute for TLS: it is for the TLS terminator you do not trust, and
 * for `ws://` where no certificate can be had.
 *
 * It costs every broadcast its single shared frame: each recipient has its
 * own key, so an emit to N sealed clients is N seals (bench/encryption.js).
 */
export interface ServerEncryptionOptions {
  /**
   * One SECRET per key id — the static key pairs are derived from it. The
   * current kid is what `encryptionKey()` publishes; a client names the kid
   * it pinned, so an old pin works for as long as its key is on the ring.
   */
  keys: KeysOption;
  /** Refuse everything plaintext: sockets close 1008, HTTP answers 426, `attach()` must be told `encrypted`. Default false. */
  required?: boolean;
  /** Default `['NK', 'XX']`. `'NN'` (anonymous) and `'NNpsk0'` have to be listed to be accepted. */
  patterns?: ReadonlyArray<NoisePattern>;
  /** Default both. A protocol a client names that is not on these lists is refused — never negotiated down. */
  ciphers?: ReadonlyArray<CipherAlgorithm>;
  /** NNpsk0: the pre-shared key. */
  psk?: Bytes | string | null;
  /**
   * Runs once the handshake is done, before any call: answer `false` to
   * close the connection (1008). Under XX `peer.remoteStatic` is the
   * client's authenticated public key.
   */
  authorize?: ((peer: EncryptionInfo) => boolean | void | Promise<boolean | void>) | null;
  /** Default 10000 ms. */
  handshakeTimeout?: number;
  /** Default 2^20; both ends must agree. */
  rekeyAfter?: number;
  /**
   * The per-request binding (http, sse): how far a sender's clock may be
   * from this server's, in ms — a sealed request outside it is refused
   * (409), and one inside it is accepted ONCE. Default 300000.
   */
  maxSkew?: number;
  /**
   * The "accepted once" memory. In process by default — behind a balancer a
   * replay can land on another instance, so inject a shared one: `seen(id,
   * ttl)` answers true for an id it was shown within `ttl` ms (Redis: `SET
   * id 1 NX PX ttl`).
   */
  /**
   * The "seen once" memory of sealed requests: a shared one (`seen` — Redis
   * `SET id 1 NX PX ttl` behind more than one instance), or the built-in
   * cache's knobs. It must hold every request of the last 2·maxSkew, so
   * `max ≥ rps × 2·maxSkew/1000`; full of live entries it refuses (409,
   * `encryption.replay.overflow` once per ten seconds) unless `overflow:
   * 'evict'` trades "accepted once" for availability.
   */
  replay?:
    | { seen(id: string, ttl: number): boolean | Promise<boolean> }
    | { max?: number; overflow?: 'refuse' | 'evict' }
    | null;
  /** Serve the public key bundle at `GET <basePath>/encryption-key`. Default true; it is trust on first use. */
  discovery?: boolean;
}

/**
 * The default `replay` memory: bounded, in process. Full of live entries it
 * answers `true` (`overflow: 'refuse'`, the default) or forgets the oldest
 * (`'evict'`); `onOverflow` hears of each id that met the cap.
 */
export declare function createReplayCache(options?: {
  max?: number;
  now?: () => number;
  overflow?: 'refuse' | 'evict';
  onOverflow?: () => void;
}): {
  seen(id: string, ttl: number): boolean;
  readonly size: number;
};

type Bytes = Uint8Array;
