// Types for the browser half of `@alexify/wrpc/encryption`
// (`encryption.browser.js`): the primitives — AEAD, X25519, HKDF, the
// keyring — and the structural contracts an application injects its own
// through. Everything here runs on both platforms; encryption.d.ts
// re-exports this file and adds what only a Node process does.

/** Any byte view `crypto.subtle` and `node:crypto` both take. */
export type Bytes = Uint8Array;

/**
 * The one failure `open` has. A wrong key, a wrong nonce, a flipped bit and
 * a truncated message are indistinguishable on purpose.
 */
export declare class OpenError extends Error {
  readonly code: 'open';
}

/** One key of a `Cipher`, ready to use. Either method may answer a promise. */
export interface CipherKey {
  /** → ciphertext ‖ tag. A `(key, nonce)` pair must never seal two messages. */
  seal(nonce: Bytes, plaintext: Bytes, aad?: Bytes | null): Bytes | Promise<Bytes>;
  /** → plaintext; throws (or rejects with) `OpenError`. */
  open(nonce: Bytes, sealed: Bytes, aad?: Bytes | null): Bytes | Promise<Bytes>;
}

/**
 * An AEAD, structurally — the platform's (`aead()`), or an injected one:
 * XChaCha20-Poly1305, AES-GCM-SIV, AEGIS, a hardware module. `key()` is
 * where the expensive half happens once: a non-extractable `CryptoKey` in a
 * browser (hence the promise), a `KeyObject` on Node.
 */
export interface Cipher {
  /** What the two ends compare — never a level or an implementation name. */
  readonly id: string;
  readonly keyLength: number;
  readonly nonceLength: number;
  readonly tagLength: number;
  key(raw: Bytes): CipherKey | Promise<CipherKey>;
}

export declare function isCipher(value: unknown): value is Cipher;
export declare function isCipherKey(value: unknown): value is CipherKey;

/** The platform AEADs. `'chacha20-poly1305'` exists on Node only. */
export type CipherAlgorithm = 'aes-256-gcm' | 'chacha20-poly1305';

export declare const ALGORITHMS: ReadonlyArray<CipherAlgorithm>;

export interface AeadOptions {
  /** Default `'aes-256-gcm'` — the one AEAD every browser has. */
  algorithm?: CipherAlgorithm;
  /** Answer `null` instead of throwing when this platform lacks the algorithm. */
  optional?: boolean;
  /** WebCrypto; defaults to `globalThis.crypto.subtle`. Read by the browser half only. */
  subtle?: SubtleCrypto;
}

/**
 * A platform AEAD by name: synchronous over `node:crypto` on Node,
 * promise-answering over `crypto.subtle` in a browser.
 */
export declare function aead(options: AeadOptions & { optional: true }): Cipher | null;
export declare function aead(options?: AeadOptions): Cipher;

/** Opaque: a `CryptoKey` in a browser, a `KeyObject` on Node, or an injected implementation's own. */
export type PrivateKey = CryptoKey | object;

export interface KeyPair {
  /** The bytes that travel. */
  publicKey: Bytes;
  privateKey: PrivateKey;
}

/**
 * A Diffie-Hellman function in the shape Noise and HPKE's DHKEM consume —
 * the platform's X25519 (`x25519()`), or an injected one.
 */
export interface Dh {
  /** The name Noise gives the function: `'25519'`. */
  readonly id: string;
  readonly publicLength: number;
  generateKeyPair(): Promise<KeyPair>;
  /** A key pair from a configured private key — a server's static one. */
  keyPair(seed: Bytes): Promise<KeyPair>;
  /** Rejects for a public key that yields the all-zero secret. */
  dh(privateKey: PrivateKey, publicKey: Bytes): Promise<Bytes>;
}

export declare function isDh(value: unknown): value is Dh;

/**
 * X25519 (RFC 7748): `node:crypto` KeyObjects on Node, `crypto.subtle` in a
 * browser (Chrome 133, Firefox 130, Safari 17) — where a private key is a
 * non-extractable `CryptoKey`. `subtle` is read by the browser half only.
 */
export declare function x25519(options?: { subtle?: SubtleCrypto }): Dh;

/** SHA-256, HMAC and HKDF (RFC 5869), Extract and Expand apart. */
export interface Kdf {
  readonly id: 'SHA256';
  readonly hashLength: 32;
  hash(data: Bytes): Promise<Bytes>;
  hmac(key: Bytes, data: Bytes): Promise<Bytes>;
  extract(salt: Bytes, ikm: Bytes): Promise<Bytes>;
  expand(prk: Bytes, info: Bytes, length: number): Promise<Bytes>;
  /** `expand(extract(salt, ikm), info, length)`. */
  derive(ikm: Bytes, salt: Bytes, info: Bytes, length: number): Promise<Bytes>;
}

export declare function createKdf(options?: { subtle?: SubtleCrypto }): Kdf;

/** 32 bytes: a `Uint8Array`, 64 hex characters, or base64 / base64url. */
export type KeyMaterial = Bytes | string;

/**
 * The keyring's injected form — a KMS or Vault client that unwrapped its
 * data keys at boot. Both methods are SYNCHRONOUS: they are read where a
 * backplane envelope is opened, and that path cannot wait.
 */
export interface KeyProvider {
  current(): string;
  get(kid: string): Bytes | null;
  /** Every kid held, newest first — what a sealed store walks after a rotation. */
  kids?(): ReadonlyArray<string>;
}

export declare function isKeyProvider(value: unknown): value is KeyProvider;

/**
 * `keys` wherever wrpc seals under a shared key: one key (filed under kid
 * `'0'`), a ring with its current kid, or a provider. A kid is 1-32 of
 * `A-Z a-z 0-9 . _ -` and travels in the clear.
 */
export type KeysOption = KeyMaterial | { current: string; ring: Readonly<Record<string, KeyMaterial>> } | KeyProvider;

export interface Keyring {
  readonly current: string;
  readonly kids: ReadonlyArray<string>;
  get(kid: string): Bytes | null;
}

/** Strict: a TypeError at construction, never a message that silently does not open. */
export declare function normalizeKeys(value: KeysOption, name?: string): Keyring;

export declare function isKid(value: unknown): value is string;

/** A fresh 32-byte key from the platform's CSPRNG. */
export declare function generateKey(options?: { crypto?: Pick<Crypto, 'getRandomValues'> }): Bytes;

export declare function toBase64Url(bytes: Bytes): string;
/** base64 or base64url, padded or not; `null` when it is neither. */
export declare function fromBase64(text: string): Bytes | null;
/** Constant-time equality of two byte strings. */
export declare function equal(a: Bytes, b: Bytes): boolean;
