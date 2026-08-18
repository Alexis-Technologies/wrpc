---
layout: home

title: wrpc
titleTemplate: WebSocket-based RPC protocol for Node.js and the browser

hero:
  name: wrpc
  text: Fast, zero-dependency WebSocket RPC
  tagline: Router and procedures, subscriptions with resume, rooms that scale, and binary streams with real backpressure — for Node.js and browsers, with nothing in your lockfile.
  image:
    src: /logo-mark.svg
    alt: wrpc
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: Why wrpc?
      link: /guide/why
    - theme: alt
      text: View on GitHub
      link: https://github.com/Alexis-Technologies/wrpc

features:
  - icon: 📭
    title: Zero dependencies
    details: No runtime dependencies at all — no <code>dependencies</code> field, no peers, no optionals. Redis, uWebSockets.js, fastify and TanStack are injected by you, never installed by wrpc.
    link: /guide/scaling
    linkText: How injection works
  - icon: 🧩
    title: Router and procedures
    details: Declare units of procedures with access control, Standard Schema validators, timeouts and queues. The client builds its API from the server's own introspection — nothing to generate.
    link: /guide/router
    linkText: Write a router
  - icon: 🔔
    title: Subscriptions that resume
    details: An async generator per feed, with real backpressure. Track values with an event id and a reconnecting client replays exactly what it missed.
    link: /guide/subscriptions
    linkText: Push a feed
  - icon: 📡
    title: Rooms that scale out
    details: Join, leave and broadcast in one process; add a backplane and a room spans every instance. Redis included, or any pub/sub with three methods.
    link: /guide/rooms
    linkText: Broadcast
  - icon: 🧵
    title: Binary streams
    details: Upload and download bytes on the same connection, interleaved without head-of-line blocking, with backpressure that reaches all the way into TCP. 1 GiB streams at flat memory.
    link: /guide/streams
    linkText: Move bytes
  - icon: 🔌
    title: Runs where you already run
    details: A batteries-included server, or plug into fastify, express, bare node:http or uWebSockets.js. Where WebSockets cannot go, the same protocol rides Server-Sent Events.
    link: /guide/adapters/fastify
    linkText: Adapters
  - icon: 🧠
    title: Types without a build step
    details: Declare the contract once and connect&lt;Api&gt;() types every call, or generate it from a running server with the wrpc types CLI. No TypeScript at runtime, ever.
    link: /guide/typed-client
    linkText: Type the client
  - icon: 🛰️
    title: Cluster-aware out of the box
    details: Presence read from a local map, <code>fetchClients</code> across every node, addressed commands and node-to-node asks — on the same backplane the rooms use.
    link: /guide/cluster
    linkText: Go multi-instance
  - icon: 🌐
    title: Node.js and the browser
    details: One protocol implementation, ~10 KB min+gzip in a browser bundle with no Node builtins — plus offline through a Service Worker and TanStack Query bindings at ~1 KB.
    link: /guide/client
    linkText: Client guide
---
