'use strict';

/**
 * The server side every transport benchmark calls — bench/transports.js
 * (through transport-worker.js) and the Chrome suite
 * (scripts/bench-browser.js transports) — in the bench-local `api`
 * shorthand wrpc-echo.js turns into a router:
 *
 *   bench/echo        answers its arguments
 *   bench/echoStream  reads an upload to its end, then sends as many bytes
 *                     back on a stream of its own (one round trip each way)
 *   bench/sink        reads an upload to its end and answers its size
 */

const CHUNK = 64 * 1024;
const BLOCK = new Uint8Array(CHUNK).fill(7);

// Writes `size` bytes in CHUNK pieces, waiting for 'drain' whenever the
// transport did not take one — the pacing createBlobUploader() does.
async function pump(writable, size) {
  for (let sent = 0; sent < size; sent += CHUNK) {
    const piece = size - sent >= CHUNK ? BLOCK : BLOCK.subarray(0, size - sent);
    if (!writable.write(piece)) {
      if (writable.closed) throw new Error('the stream closed mid-upload');
      await new Promise((resolve) => writable.once('drain', resolve));
    }
  }
  writable.end();
}

const drain = async (readable) => {
  let bytes = 0;
  for await (const chunk of readable) bytes += chunk.length;
  return bytes;
};

const api = {
  bench: {
    echo: { handler: async (args) => args },
    echoStream: {
      handler: async ({ stream }, context) => {
        const bytes = await drain(context.client.getStream(stream));
        const back = context.client.createStream('back', bytes);
        pump(back, bytes);
        return back.id;
      },
    },
    sink: { handler: async ({ stream }, context) => drain(context.client.getStream(stream)) },
  },
};

module.exports = { api, pump, CHUNK };
