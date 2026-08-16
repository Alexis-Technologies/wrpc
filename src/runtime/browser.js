'use strict';

// `crypto.randomUUID` only exists in secure contexts (https, localhost).
// Ids here are correlation identifiers — packet ids, context uuids — not
// security tokens (the session token has its own generator, injected
// server-side), so a page served over plain http gets a Math.random-shaped
// fallback instead of a TypeError on its first call.
const pseudoUuidV4 = () =>
  'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (char) => {
    const random = (Math.random() * 16) | 0;
    const value = char === 'x' ? random : (random & 0x3) | 0x8;
    return value.toString(16);
  });

const generateUUID = () => (globalThis.crypto?.randomUUID ? globalThis.crypto.randomUUID() : pseudoUuidV4());

module.exports = { generateUUID };
