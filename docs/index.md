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
    details: No runtime dependencies at all — no <code>dependencies</code> field, no peers, no optionals. Redis, uWebSockets.js, fastify, TanStack and OpenTelemetry are injected by you, never installed by wrpc.
    link: /guide/scaling
    linkText: How injection works
  - icon: 🧩
    title: Router, procedures, hooks
    details: Declare units of procedures with access control, Standard Schema validators, timeouts and queues — and named lifecycle hooks at router, unit and procedure level instead of <code>(ctx, next)</code> middleware.
    link: /guide/router
    linkText: Write a router
  - icon: 🌐
    title: Real REST, declared on the procedure
    details: <code>http&colon; { method, path, status }</code> turns a procedure into a proper endpoint&colon; path params, /vN version paths, fastify-shaped schemas, and an OpenAPI document from <code>wrpc types --openapi</code>.
    link: /guide/rest
    linkText: Declare an endpoint
  - icon: 🔔
    title: Subscriptions that resume
    details: An async generator per feed, with real backpressure. Track values with an event id and a reconnecting client replays exactly what it missed.
    link: /guide/subscriptions
    linkText: Push a feed
  - icon: 🛰️
    title: Rooms and cluster in one
    details: Join, leave and broadcast in one process; add any pub/sub with three methods and a room spans every instance — with presence read from a local map, <code>fetchClients</code>, addressed commands and node-to-node asks.
    link: /guide/cluster
    linkText: Go multi-instance
  - icon: 🧵
    title: Binary streams
    details: Upload and download bytes on the same connection, interleaved without head-of-line blocking, with backpressure that reaches all the way into TCP. 1 GiB streams at flat memory.
    link: /guide/streams
    linkText: Move bytes
  - icon: 🧠
    title: Types without a build step
    details: Declare the contract once and connect&lt;Api&gt;() types every call — server-push events included — or generate it from a running server with the wrpc types CLI. No TypeScript at runtime, ever.
    link: /guide/typed-client
    linkText: Type the client
  - icon: 🔐
    title: Sessions and auth, batteries included
    details: Cookie sessions by default; bearer and payload token carriers, client token stores and an authenticate/refresh lifecycle that re-presents the credential before every reconnect restore.
    link: /guide/sessions
    linkText: Wire up auth
  - icon: 🔌
    title: Runs where you already run
    details: A batteries-included server, or plug into fastify, express, bare node:http or uWebSockets.js; the same protocol rides Server-Sent Events where WebSockets cannot go — with pino-style logging and OTel traces built in.
    link: /guide/adapters/fastify
    linkText: Adapters
  - icon: 🌍
    title: Node.js and the browser
    details: One protocol implementation, under 15 KB min+gzip in a browser bundle with no Node builtins (a budget CI enforces) — plus offline through a Service Worker and TanStack Query bindings at ~1 KB.
    link: /guide/client
    linkText: Client guide
---
