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
  /** From this call on `raw` is the cipher's: wrpc neither reuses nor wipes it, so keeping the reference is fine. */
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

/** The one-round-trip Noise patterns a session may run. */
export type NoisePattern = 'NN' | 'NK' | 'XX' | 'NNpsk0';

export declare const PATTERN_NAMES: ReadonlyArray<NoisePattern>;

/**
 * What both ends of an encrypted session know about it once the handshake
 * is done — `client.encryption` on a `WrpcClient`, and on the server's
 * `Client`. `handshakeHash` is unique to the handshake and identical on
 * both ends: bind a credential to it (send `HMAC(token, handshakeHash)`)
 * and a credential relayed onto another connection is worthless there.
 */
export interface EncryptionInfo {
  /** The Noise protocol name, e.g. `'Noise_NK_25519_AESGCM_SHA256'`. */
  readonly protocol: string;
  readonly pattern: NoisePattern;
  /** `'AESGCM'`, `'ChaChaPoly'`, or an injected cipher's id. */
  readonly cipher: string;
  /** The server key id the client pinned; `''` for NN and NNpsk0. */
  readonly kid: string;
  /** The peer's static public key: the pinned one (NK), the one that arrived (XX), null otherwise. */
  readonly remoteStatic: Bytes | null;
  readonly handshakeHash: Bytes;
}

/** A server's public key bundle — `"<kid>:<noise key>:<hpke key>"`, or the same as an object. */
export type ServerKey = string | { kid: string; noise: Bytes | string; hpke: Bytes | string };

export declare function parseBundle(value: ServerKey, name?: string): { kid: string; noise: Bytes; hpke: Bytes };

export interface CreateEncryptionOptions {
  /**
   * The server's key bundle, from `rpc.encryptionKey()` — what this client
   * PINS: only the holder of the matching private key can finish the
   * handshake. With it the pattern defaults to `'NK'`.
   */
  serverKey?: ServerKey | null;
  /**
   * `'NK'` (the default with a `serverKey`), `'XX'` (mutual — needs
   * `staticKey`), `'NN'` (anonymous: encrypted, nobody authenticated) or
   * `'NNpsk0'` (needs `psk`). `'NN'` is never a default: it has to be named.
   */
  pattern?: NoisePattern;
  /** XX: this client's long-lived private key, 32 bytes — its identity to the server's `authorize`. */
  staticKey?: Bytes | null;
  /** XX without a `serverKey`: decides whether the key that answered is trusted. Must answer `true`. */
  verifyServer?: ((publicKey: Bytes) => boolean | Promise<boolean>) | null;
  /** NNpsk0: the pre-shared key, 32 bytes. */
  psk?: Bytes | null;
  /** Default `'aes-256-gcm'` — the one cipher a browser has. */
  cipher?: CipherAlgorithm | Cipher;
  dh?: Dh;
  /** Messages per direction between deterministic rekeys. Default 2^20; both ends must agree; 0 disables. */
  rekeyAfter?: number;
  /** Default 10000 ms. */
  handshakeTimeout?: number;
}

/** The transport's side of the seam `Encryption.secure()` is handed. */
export interface EncryptionLink {
  /** `'ws'`, `'wt'` — bound into the handshake, so one cannot be replayed onto the other. */
  kind: string;
  write(bytes: Bytes): void;
  deliver(message: string | Bytes): void;
  fail(error: Error): void;
}

export interface EncryptedConnection {
  readonly ready: Promise<EncryptionInfo>;
  send(data: string | Bytes | ArrayBuffer): void;
  receive(data: string | Bytes | ArrayBuffer): void;
  /**
   * The transport is gone (a close, a terminate): nothing more is written
   * or delivered, and a pending `ready` rejects now rather than at the
   * handshake timeout. Does not call `link.fail`.
   */
  cancel(error?: Error): void;
}

/**
 * What a client is handed as `encryption`. A client that has one NEVER
 * speaks plaintext: a server that does not answer the handshake, a key that
 * is not the pinned one, a transport that cannot carry it — each is a
 * failed connection, never a fallback.
 */
export interface Encryption {
  /** The connect-URL parameter that announces the mode: `'wrpc_e'`. */
  readonly param: string;
  readonly protocol: string;
  readonly pattern: NoisePattern;
  secure(link: EncryptionLink): EncryptedConnection;
  /**
   * The per-request half, for a transport with no connection to hold a
   * session (http, sse): wraps a `fetch` so that every request is sealed to
   * the pinned server key (HPKE) and POSTed to `endpoint`, and answers a real
   * `Response`. Null without a `serverKey`, or under a cipher HPKE has no
   * registered id for.
   */
  readonly fetch: ((fetch: typeof globalThis.fetch, endpoint: string) => typeof globalThis.fetch) | null;
}

/**
 * The server's key bundle from its discovery endpoint, `GET
 * <url>/encryption-key`. TRUST ON FIRST USE: only as trustworthy as the
 * connection it came over — ship the bundle with the client where you can.
 */
export declare function fetchServerKey(url: string, options?: { fetch?: typeof globalThis.fetch }): Promise<string>;

/** A key-encapsulation mechanism, structurally — DHKEM over a `Dh`, or an injected one (ML-KEM, a hybrid). */
export interface Kem {
  /** The registered HPKE KEM id (`0x0020` for DHKEM(X25519, HKDF-SHA256)). */
  readonly id: number;
  readonly publicLength: number;
  readonly encLength: number;
  readonly secretLength: number;
  generateKeyPair(): Promise<KeyPair>;
  keyPair(seed: Bytes): Promise<KeyPair>;
  encap(
    recipientPublicKey: Bytes,
    ephemeral?: KeyPair | null,
    sender?: KeyPair | null,
  ): Promise<{ sharedSecret: Bytes; enc: Bytes }>;
  decap(enc: Bytes, recipient: KeyPair, senderPublicKey?: Bytes | null): Promise<Bytes>;
}

export declare function isKem(value: unknown): value is Kem;
export declare function dhKem(dh: Dh, kdf: Kdf, id?: number): Kem & { deriveKeyPair(ikm: Bytes): Promise<KeyPair> };

export interface HpkeOptions {
  /** What the message is FOR: one sealed for one purpose does not open for another. */
  info?: Bytes;
  /** Auth mode, on the sender: its static key pair — the recipient learns WHICH key sealed the message. */
  senderKey?: KeyPair | null;
  /** Auth mode, on the recipient: the sender it expects. A message anyone else sealed does not open. */
  senderPublicKey?: Bytes | null;
  /** psk mode: the recipient also learns the sender held this key. With `pskId`. */
  psk?: Bytes | null;
  pskId?: Bytes | null;
}

export interface HpkeContext {
  seal(aad: Bytes | null, plaintext: Bytes): Bytes | Promise<Bytes>;
  open(aad: Bytes | null, sealed: Bytes): Bytes | Promise<Bytes>;
  /** A secret both ends derive and nobody else: `length` bytes bound to `context`. */
  export(context: Bytes, length: number): Promise<Bytes>;
}

/** HPKE (RFC 9180): base, psk, auth and auth_psk modes, HKDF-SHA256. */
export declare function createHpke(options: { kem: Kem; kdf: Kdf; cipher: Cipher }): {
  readonly suite: Bytes;
  readonly aeadId: number;
  readonly encLength: number;
  setupSender(recipientPublicKey: Bytes, options?: HpkeOptions): Promise<{ enc: Bytes; context: HpkeContext }>;
  setupRecipient(enc: Bytes, recipient: KeyPair, options?: HpkeOptions): Promise<HpkeContext>;
};

export declare function createEncryption(options?: CreateEncryptionOptions): Encryption;
export declare function isEncryption(value: unknown): value is Encryption;

export interface NoiseHandshake {
  readonly writing: boolean;
  readonly done: boolean;
  readonly remoteStatic: Bytes | null;
  write(payload?: Bytes): Promise<Bytes>;
  read(message: Bytes): Promise<Bytes>;
  finish(): Promise<{ send: unknown; receive: unknown; handshakeHash: Bytes; remoteStatic: Bytes | null }>;
}

export interface NoiseHandshakeOptions {
  prologue?: Bytes;
  staticKey?: KeyPair;
  remoteStatic?: Bytes;
  psk?: Bytes;
  rekeyAfter?: number;
}

/** One Noise protocol over injected primitives — the canonical name is `name`. */
export declare function createNoise(options: { pattern: NoisePattern; dh: Dh; cipher: Cipher; kdf: Kdf }): {
  readonly name: string;
  readonly pattern: NoisePattern;
  readonly cipher: string;
  initiator(options?: NoiseHandshakeOptions): Promise<NoiseHandshake>;
  responder(options?: NoiseHandshakeOptions): Promise<NoiseHandshake>;
};

/**
 * An end-to-end identity. The SEED is the secret — 32 bytes to keep wherever
 * this client keeps secrets, and the same identity again from the same seed;
 * `publicKey` is what others seal to and verify against.
 */
export interface Identity {
  readonly seed: Bytes;
  readonly publicKey: Bytes;
  readonly keyPair: KeyPair;
}

export interface E2eePrimitives {
  cipher?: Cipher;
  dh?: Dh;
}

export declare function createIdentity(
  seed?: Bytes | null,
  options?: E2eePrimitives & { crypto?: Pick<Crypto, 'getRandomValues'> },
): Promise<Identity>;

/**
 * Seals one message to one recipient (HPKE, a fresh context per message):
 * `enc ‖ ciphertext`, bytes wrpc carries as they are through a server that
 * cannot read them. With `senderKey` the recipient can check who sealed it.
 * Not a messaging protocol: no forward secrecy for the recipient, no group
 * key, no replay memory — run Double Ratchet or MLS over the same bytes for
 * those. In a browser it protects against a server that READS, not one that
 * serves the page a different script.
 */
export declare function createSealer(
  options: E2eePrimitives & {
    recipientPublicKey: Bytes;
    senderKey?: KeyPair | null;
    /** What the messages are for — a room, a conversation id. */
    info?: Bytes | string | null;
  },
): { seal(data: Bytes | string, aad?: Bytes | string | null): Promise<Bytes> };

export declare function createOpener(
  options: E2eePrimitives & {
    keyPair: KeyPair;
    /** A message this identity did not seal does not open. Without it, anyone who knows the public key could have sent it. */
    senderPublicKey?: Bytes | null;
    info?: Bytes | string | null;
  },
): { open(sealed: Bytes, aad?: Bytes | string | null): Promise<Bytes> };
