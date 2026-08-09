'use strict';

const generateUUID = () => globalThis.crypto.randomUUID();

module.exports = { generateUUID };
