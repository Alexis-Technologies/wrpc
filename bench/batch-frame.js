/**
 * The frame of a client batch (`flush` in src/client/core.js): the calls
 * waiting to be sent, each already serialized when it was queued, put
 * together as one JSON array.
 *
 * The readable spelling is `[${pending.map((entry) => entry.text).join(',')}]`
 * — an array of the texts built only to be joined. The one in the client
 * appends them to a string in an indexed loop. The rows compare the two for
 * the batch sizes a client makes (`batch.maxSize` is 10 by default) and a
 * large one, with call packets of about 100 bytes. One run printed, in
 * frames a second:
 *
 *   calls    map().join()    appended
 *   2          5,300,000    7,700,000
 *   10         2,650,000    3,480,000
 *   64           545,000      819,000
 *
 * Run with `node bench/batch-frame.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');

const entry = (i) => {
  const packet = { type: 'call', id: `c${i}`, method: 'chat/send', args: { room: 'general', text: `hello ${i}` } };
  return { packet, text: JSON.stringify(packet) };
};

const joined = (pending) => `[${pending.map((item) => item.text).join(',')}]`;

const appended = (pending) => {
  let frame = `[${pending[0].text}`;
  for (let i = 1; i < pending.length; i++) frame += `,${pending[i].text}`;
  frame += ']';
  return frame;
};

let sink = 0;

const main = async () => {
  console.log(`Batch frame benchmark — Node ${process.version}\n`);
  for (const size of [2, 10, 64]) {
    const pending = Array.from({ length: size }, (_, i) => entry(i));
    if (joined(pending) !== appended(pending)) throw new Error('the two spellings disagree');
    await bench(`${size} calls — map().join()`, () => {
      // charCodeAt flattens the string: what handing it to a socket does.
      sink += joined(pending).charCodeAt(size);
    });
    await bench(`${size} calls — appended in a loop`, () => {
      sink += appended(pending).charCodeAt(size);
    });
  }
  if (sink < 0) console.log(sink);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
