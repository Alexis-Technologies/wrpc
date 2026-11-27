import { expectAssignable, expectError, expectType } from 'tsd';
import * as deflate from '../src/deflate/index.js';
import type { DeflateCodecOptions, DeflateError, DeflateErrorCode } from '../src/deflate/index.js';
import type { Compressor, RpcServerOptions, Router } from '../index.js';
import type { ErrorCode } from '../src/errors.js';
import { buildDictionary } from '../index.js';

declare const router: Router;

// The codec is a Compressor, with or without a dictionary
const codec = deflate.createDeflateCodec({ dictionary: buildDictionary(router) });
expectAssignable<Compressor>(codec);
expectType<Uint8Array | null>(codec.dictionary);
expectAssignable<Compressor>(deflate.createDeflateCodec());
expectAssignable<DeflateCodecOptions>({ threshold: 32, native: false, nativeAbove: 8192, level: 9 });
expectError(deflate.createDeflateCodec({ level: 'max' }));
expectAssignable<RpcServerOptions>({ router, compression: { codec } });

// The primitives
expectType<Uint8Array>(deflate.deflateRaw(new Uint8Array(4)));
expectType<Uint8Array>(deflate.inflateRaw(new Uint8Array(4), { maxOutput: 1024 }));
declare const error: DeflateError;
expectType<DeflateErrorCode>(error.code);
expectAssignable<string>(error.code);

// Its codes are ALX_ERR_ ones, constructed as every wRPC error is
expectAssignable<ErrorCode>(error.code);
expectType<'DeflateError'>(error.name);
expectAssignable<DeflateError>(new deflate.DeflateError('x', { code: 'ALX_ERR_DEFLATE_TRUNCATED' }));
expectError(new deflate.DeflateError('x', { code: 'truncated' }));
