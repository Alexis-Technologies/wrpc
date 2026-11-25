'use strict';

// A test context for the before hook of a describe. bootServer/connectClient (tests/helpers/server.js) and the
// other boots register their teardown through t.after, but the SuiteContext a hook receives has no after, and
// node:test's own after() called inside a running hook fires as soon as that hook ends. The scope collects the
// teardown instead, in the order t.after runs it; the describe registers `after(() => scope.close())` BEFORE its
// before hook boots, so a failing hook or test still releases what was opened.

/** A `{ after }` stand-in for a test context; `close()` runs what was registered, first to last. */
const suiteScope = () => {
  const hooks = [];
  return {
    after: (fn) => void hooks.push(fn),
    close: async () => {
      for (const fn of hooks.splice(0)) await fn();
    },
  };
};

module.exports = { suiteScope };
