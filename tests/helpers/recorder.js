'use strict';

// A structured logger writer that records every line, bindings included.
//
// The copies this replaces answered `child()` with `this`, so the bindings
// a component attaches — `component`, `peer`, `kid` — never reached the
// recorded entries, and a test could not assert on them. Here `child()`
// answers a new writer over the SAME array with the bindings merged in, so
// an entry reads `{ level, ...bindings, ...entry }`, exactly what a pino
// child would have written. `level` is 'debug' so nothing is dropped (a
// Console writer drops debug; a structured one keeps what its level
// allows). Not a *.test.js: node --test must not run helpers.

/** @returns {{ entries: object[], writer: object, find(event: string): object | undefined, all(event: string): object[], loud(): object[] }} */
const recorder = () => {
  const entries = [];
  const make = (bindings) => {
    const writer = {
      level: 'debug',
      child: (more) => make({ ...bindings, ...more }),
    };
    for (const level of ['log', 'debug', 'info', 'warn', 'error']) {
      writer[level] = (entry) =>
        entries.push({ level, ...bindings, ...(entry && typeof entry === 'object' ? entry : { message: entry }) });
    }
    return writer;
  };
  return {
    entries,
    writer: make({}),
    /** The first entry with this `event`. */
    find: (event) => entries.find((entry) => entry.event === event),
    /** Every entry with this `event`, in order. */
    all: (event) => entries.filter((entry) => entry.event === event),
    /** The lines an operator would be paged for: warn and error. */
    loud: () => entries.filter((entry) => entry.level === 'warn' || entry.level === 'error'),
  };
};

module.exports = { recorder };
