#!/usr/bin/env node

'use strict';

// A shim, on purpose: everything the command does lives in src/cli/types.js,
// where the coverage thresholds reach it and where it can be driven in-process
// by a test instead of through a child process.
const { main } = require('../src/cli/types.js');

main(process.argv.slice(2)).then(
  (code) => {
    process.exitCode = code;
  },
  // main() answers with an exit code rather than throwing, so this is the
  // "impossible" branch. It exists because the alternative — Node printing an
  // unhandled-rejection trace and exiting 1 — would replace the CLI's error
  // format with a stack, exactly when something is already confusing.
  (error) => {
    console.error(`wrpc: ${error?.stack ?? error}`);
    process.exitCode = 1;
  },
);
