'use strict';

const { createNodeEngine } = require('./node.js');

// Structural runtime check for user-provided engines (duck typing per the
// zero-dependency injection rule) — used by the Server shell to fail fast
// with a clear message instead of a deep TypeError.
const isEngine = (engine) =>
  typeof engine === 'object' &&
  engine !== null &&
  typeof engine.name === 'string' &&
  typeof engine.attach === 'function' &&
  typeof engine.close === 'function';

module.exports = { createNodeEngine, isEngine };
