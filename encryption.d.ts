// Types for the `@alexify/wrpc/encryption` subpath on Node: the primitives
// of the browser half (the AEADs synchronous over node:crypto here), plus
// what only a Node process does as it lands.

export * from './encryption.browser.js';

import type { Cipher, CipherAlgorithm, KeysOption } from './encryption.browser.js';

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
