// Types for the browser entry (`browser.js`, the `browser` condition of the
// root export). The runtime barrel exports the client surface alone, so the
// types do too: importing a server name from '@alexify/wrpc' in a browser
// bundle is a compile error here instead of an `undefined` at runtime, and
// nothing in this graph touches node types.
export * from './client.js';
