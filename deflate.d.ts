// Types for the `@alexify/wrpc/deflate` subpath: a per-message DEFLATE codec
// in plain JavaScript — the one way a browser compresses against a preset
// dictionary (CompressionStream takes none), and synchronously.

import type { Compressor } from './client.js';

export interface DeflateCodecOptions {
  /**
   * The preset dictionary both ends hold — `buildDictionary(router)`, or
   * any bytes. Without one the codec is plain raw deflate under the
   * platform id (`'deflate-raw'`), and the platform codecs read it.
   */
  dictionary?: Uint8Array | string | null;
  /** The size under which a message goes plain: 64 B with a dictionary, 1 KiB without. */
  threshold?: number;
  /**
   * Hand messages of `nativeAbove` bytes and more to the platform's
   * CompressionStream (dynamic Huffman, no dictionary — a large payload has
   * its history inside itself; the output still inflates on a peer holding
   * the dictionary). On by default in a browser, off in Node, where every
   * carrier may need a synchronous answer; when on, `encode` answers a
   * promise for those messages.
   */
  native?: boolean;
  /** Default 4096. */
  nativeAbove?: number;
  /** The LZ77 hash-chain depth, 1 (quick) to 9 (exhaustive). Default 6. */
  level?: number;
}

/** The codec: inject it as `compression: { codec }` wherever a codec goes. */
export declare function createDeflateCodec(
  options?: DeflateCodecOptions,
): Compressor & { readonly dictionary: Uint8Array | null };

/**
 * Raw DEFLATE → bytes, synchronously: stored, fixed and dynamic blocks,
 * `dictionary` preloading the window, `maxOutput` the cap past which a
 * DeflateError('too-large') is thrown rather than a bigger allocation.
 */
export declare function inflateRaw(
  input: ArrayBuffer | ArrayBufferView,
  options?: { dictionary?: ArrayBuffer | ArrayBufferView | null; maxOutput?: number },
): Uint8Array;

/**
 * Bytes → raw DEFLATE, synchronously, as one fixed-Huffman block (or stored
 * blocks when smaller); `dictionary` as zlib's option, `level` the
 * hash-chain depth.
 */
export declare function deflateRaw(
  input: ArrayBuffer | ArrayBufferView,
  options?: { dictionary?: ArrayBuffer | ArrayBufferView | null; level?: number },
): Uint8Array;

/** Every malformed input is one of these, with a code: truncated, huffman, stored, distance, block, too-large. */
export declare class DeflateError extends Error {
  readonly code: string;
}
