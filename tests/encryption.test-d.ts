import { expectAssignable, expectError, expectType } from 'tsd';
import * as encryption from '../encryption.js';
import type {
  Cipher,
  CipherKey,
  Dh,
  Encryption,
  EncryptionInfo,
  EnvelopeEncryptionOptions,
  ServerEncryptionOptions,
  Kdf,
  KeyPair,
  KeyProvider,
  Keyring,
  KeysOption,
} from '../encryption.js';
import type { RpcServerOptions, Router, SessionStore } from '../index.js';
import { MemorySessionStore, RpcServer, WrpcClient } from '../index.js';
import type { Client, WrpcClientOptions } from '../index.js';

// A platform AEAD is a Cipher; `optional` widens the answer to null
const cipher = encryption.aead();
expectType<Cipher>(cipher);
expectType<Cipher | null>(encryption.aead({ algorithm: 'chacha20-poly1305', optional: true }));
expectError(encryption.aead({ algorithm: 'aes-128-cbc' }));
expectType<ReadonlyArray<'aes-256-gcm' | 'chacha20-poly1305'>>(encryption.ALGORITHMS);

// key() and both methods may answer a promise: crypto.subtle can only
expectType<CipherKey | Promise<CipherKey>>(cipher.key(new Uint8Array(32)));
declare const key: CipherKey;
expectType<Uint8Array | Promise<Uint8Array>>(key.seal(new Uint8Array(12), new Uint8Array(4)));
expectType<Uint8Array | Promise<Uint8Array>>(key.open(new Uint8Array(12), new Uint8Array(20), null));

// An injected cipher is structural
expectAssignable<Cipher>({
  id: 'xchacha20-poly1305',
  keyLength: 32,
  nonceLength: 24,
  tagLength: 16,
  key: () => ({ seal: (_nonce: Uint8Array, plaintext: Uint8Array) => plaintext, open: async () => new Uint8Array(0) }),
});
expectType<boolean>(encryption.isCipher({}));
expectType<boolean>(encryption.isCipherKey({}));

// X25519
const dh = encryption.x25519();
expectType<Dh>(dh);
expectType<Promise<KeyPair>>(dh.generateKeyPair());
expectType<Promise<KeyPair>>(dh.keyPair(new Uint8Array(32)));
declare const pair: KeyPair;
expectType<Promise<Uint8Array>>(dh.dh(pair.privateKey, pair.publicKey));
expectType<boolean>(encryption.isDh(dh));

// HKDF
const kdf = encryption.createKdf();
expectType<Kdf>(kdf);
expectType<Promise<Uint8Array>>(kdf.derive(new Uint8Array(32), new Uint8Array(0), new Uint8Array(0), 64));
expectError(kdf.expand(new Uint8Array(32), new Uint8Array(0)));

// Keys: one key, a ring, or a provider
expectAssignable<KeysOption>(new Uint8Array(32));
expectAssignable<KeysOption>('3q2-7w');
expectAssignable<KeysOption>({ current: 'k2', ring: { k1: new Uint8Array(32), k2: 'ab' } });
const provider: KeyProvider = { current: () => 'k1', get: () => null };
expectAssignable<KeysOption>(provider);
expectError<KeysOption>({ current: 'k1' });
const ring = encryption.normalizeKeys(encryption.generateKey());
expectType<Keyring>(ring);
expectType<string>(ring.current);
expectType<Uint8Array | null>(ring.get('0'));
expectType<boolean>(encryption.isKeyProvider(provider));

// The failure and the helpers
declare const failure: encryption.OpenError;
expectType<'open'>(failure.code);
expectType<string>(encryption.toBase64Url(new Uint8Array(4)));
expectType<Uint8Array | null>(encryption.fromBase64('AAAA'));
expectType<boolean>(encryption.equal(new Uint8Array(4), new Uint8Array(4)));

// Envelope encryption on the Node↔Node carriers
declare const router: Router;
expectAssignable<EnvelopeEncryptionOptions>({ keys: new Uint8Array(32) });
expectAssignable<EnvelopeEncryptionOptions>({
  keys: { current: 'k2', ring: { k1: 'aa', k2: 'bb' } },
  cipher: 'chacha20-poly1305',
  seal: false,
  acceptPlaintext: true,
  replayWindow: false,
});
expectAssignable<EnvelopeEncryptionOptions>({ keys: provider, cipher });
expectError<EnvelopeEncryptionOptions>({ cipher: 'aes-256-gcm' });
expectError<EnvelopeEncryptionOptions>({ keys: 'k', replayWindow: true });
expectAssignable<RpcServerOptions>({
  router,
  rooms: { encryption: { keys: 'k' } },
  cluster: { secret: 's', encryption: { keys: 'k', acceptPlaintext: true } },
});
expectAssignable<RpcServerOptions>({ router, rooms: { encryption: false }, cluster: { encryption: null } });
expectError<RpcServerOptions>({ router, rooms: { encryption: true } });

// A sealed session store is a session store
const sealed = encryption.sealedStore(new MemorySessionStore(), { keys: 'k', acceptPlaintext: true, logger: false });
expectType<SessionStore>(sealed);
expectAssignable<RpcServerOptions>({ router, sessions: { store: sealed } });
expectError(encryption.sealedStore(new MemorySessionStore()));
expectError(encryption.sealedStore({}, { keys: 'k' }));

// Session encryption: the client object, the server option, the facts both ends read
const session = encryption.createEncryption({ serverKey: 'k1:a:b' });
expectType<Encryption>(session);
expectType<'NN' | 'NK' | 'XX' | 'NNpsk0'>(session.pattern);
expectAssignable<WrpcClientOptions>({ encryption: session });
expectAssignable<WrpcClientOptions>({ encryption: null });
expectError<WrpcClientOptions>({ encryption: { keys: 'k' } });
encryption.createEncryption({ pattern: 'XX', staticKey: new Uint8Array(32), verifyServer: async () => true });
encryption.createEncryption({ pattern: 'NNpsk0', psk: new Uint8Array(32), cipher: 'chacha20-poly1305', rekeyAfter: 0 });
expectError(encryption.createEncryption({ pattern: 'IK' }));
expectType<boolean>(encryption.isEncryption(session));
expectType<{ kid: string; noise: Uint8Array; hpke: Uint8Array }>(encryption.parseBundle('k1:a:b'));

expectAssignable<ServerEncryptionOptions>({ keys: 'k' });
expectAssignable<ServerEncryptionOptions>({
  keys: { current: 'k2', ring: { k1: 'a', k2: 'b' } },
  required: true,
  patterns: ['NK', 'XX'],
  ciphers: ['aes-256-gcm'],
  authorize: async (peer) => peer.remoteStatic !== null,
  handshakeTimeout: 5000,
});
expectError<ServerEncryptionOptions>({ keys: 'k', patterns: ['IK'] });
expectAssignable<RpcServerOptions>({ router, encryption: { keys: 'k', required: true } });
expectAssignable<RpcServerOptions>({ router, encryption: false });

declare const rpc: RpcServer;
expectType<Promise<string | null>>(rpc.encryptionKey());
declare const peer: Client;
expectType<EncryptionInfo | null>(peer.encryption);
declare const wrpc: WrpcClient;
expectType<EncryptionInfo | null>(wrpc.encryption);
if (wrpc.encryption) expectType<Uint8Array>(wrpc.encryption.handshakeHash);
