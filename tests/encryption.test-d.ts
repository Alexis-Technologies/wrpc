import { expectAssignable, expectError, expectType } from 'tsd';
import * as encryption from '../encryption.js';
import type { Cipher, CipherKey, Dh, Kdf, KeyPair, KeyProvider, Keyring, KeysOption } from '../encryption.js';

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
