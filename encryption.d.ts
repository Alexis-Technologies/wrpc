// Types for the `@alexify/wrpc/encryption` subpath on Node: the primitives
// of the browser half (the AEADs synchronous over node:crypto here), plus
// what only a Node process does as it lands.

export * from './encryption.browser.js';

import type { Cipher, CipherAlgorithm, KeysOption } from './encryption.browser.js';
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

export interface SealedStoreOptions {
  /** One key, a ring with its current kid, or a provider — as everywhere. */
  keys: KeysOption;
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
