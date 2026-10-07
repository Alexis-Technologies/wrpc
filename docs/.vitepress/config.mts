import { defineConfig } from 'vitepress';
import { withMermaid } from 'vitepress-plugin-mermaid';

const ogTitle = 'wRPC — Web RPC for Node.js and the browser';
const ogDescription =
  'wRPC (@alexify/wrpc) is a fast, zero-dependency Web RPC protocol for Node.js and browsers: one router ' +
  'over WebSocket, HTTP, SSE, WebTransport and WebRTC, plus Redis, NATS, RabbitMQ and Kafka — subscriptions ' +
  'with resume, rooms that scale, binary streams with real backpressure, compression, encryption and a typed client.';
const repo = 'https://github.com/Alexis-Technologies/wrpc';
const base = '/';
const hostname = 'https://wrpc.vercel.app/';
const ogImage = `${hostname}logo.png`;

// Mirrors package.json "keywords" — kept as a one-term-per-line list so the
// two stay easy to diff.
const keywords = [
  'wrpc',
  'websocket',
  'rpc',
  'protocol',
  'nodejs',
  'browser',
  'fast',
  'zero-dependency',
  'authentication',
  'realtime',
  'communication',
  'messaging',
  'remote-procedure-call',
  'rpc-framework',
  'websocket-rpc',
  'typescript',
  'javascript',
  'node',
  'opentelemetry',
  'observability',
  'sse',
  'server-sent-events',
  'rest',
  'typed-client',
  'codegen',
  'tanstack-query',
  'subscriptions',
  'pubsub',
  'cluster',
  'streams',
  'webrtc',
  'peer-to-peer',
  'data-channel',
  'webtransport',
  'http3',
  'quic',
  'message-broker',
  'broker',
  'kafka',
  'rabbitmq',
  'amqp',
  'nats',
  'jetstream',
  'redis-streams',
  'queue',
  'consumer',
  'event-driven',
  'durable-feed',
].join(', ');

// schema.org structured data — helps search and AI engines understand the
// package as a software entity, not just text on a page.
const jsonLd = {
  '@context': 'https://schema.org',
  '@type': 'SoftwareApplication',
  name: '@alexify/wrpc',
  alternateName: ['wRPC', 'Web RPC'],
  description: ogDescription,
  applicationCategory: 'DeveloperApplication',
  operatingSystem: 'Node.js >= 22, modern browsers',
  url: hostname,
  image: ogImage,
  downloadUrl: 'https://www.npmjs.com/package/@alexify/wrpc',
  codeRepository: repo,
  license: 'https://opensource.org/licenses/MIT',
  keywords,
  author: { '@type': 'Organization', name: 'Alexis Technologies' },
  offers: { '@type': 'Offer', price: '0', priceCurrency: 'USD' },
};

// https://vitepress.dev/reference/site-config
export default withMermaid({
  ...defineConfig({
    title: '@alexify/wrpc',
    titleTemplate: ':title — wRPC',
    description: ogDescription,
    lang: 'en-US',
    base,
    cleanUrls: true,
    lastUpdated: true,
    sitemap: { hostname },

    markdown: {
      config: (md) => {
        // VitePress builds a heading permalink's aria-label from the
        // heading's RAW source, so a heading that carries a badge and an
        // explicit id — `## WebRTC <Badge … text="since 2.0" /> {#webrtc}` —
        // was announced by a screen reader as its markup, tags and all.
        // Rewritten once the anchors exist: the badge's text in
        // parentheses, the id dropped, any other tag removed.
        md.core.ruler.push('wrpc-permalink-label', (state) => {
          for (const token of state.tokens) {
            if (token.type !== 'inline' || !token.children) continue;
            for (const child of token.children) {
              if (child.type !== 'link_open' || child.attrGet('class') !== 'header-anchor') continue;
              const label = child.attrGet('aria-label');
              if (label === null || !/[<{]/.test(label)) continue;
              child.attrSet(
                'aria-label',
                label
                  .replace(/\s*\{#[^}]*\}/g, '')
                  .replace(/<Badge\b[^>]*\btext="([^"]*)"[^>]*>/g, '($1)')
                  .replace(/<[^>]+>/g, '')
                  .replace(/\s+/g, ' ')
                  .replace(/\s+"$/, '"'),
              );
            }
          }
          return false;
        });
      },
    },

    head: [
      ['link', { rel: 'icon', type: 'image/svg+xml', href: `${base}logo-mark.svg` }],
      ['link', { rel: 'icon', type: 'image/png', href: `${base}favicon.png` }],
      ['meta', { name: 'theme-color', content: '#5FA04E' }],
      ['meta', { name: 'author', content: 'Alexis Technologies' }],
      ['meta', { name: 'keywords', content: keywords }],
      ['meta', { name: 'robots', content: 'index, follow' }],
      ['meta', { property: 'og:type', content: 'website' }],
      ['meta', { property: 'og:site_name', content: '@alexify/wrpc' }],
      ['meta', { property: 'og:title', content: ogTitle }],
      ['meta', { property: 'og:description', content: ogDescription }],
      ['meta', { property: 'og:image', content: ogImage }],
      ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
      ['meta', { name: 'twitter:title', content: ogTitle }],
      ['meta', { name: 'twitter:description', content: ogDescription }],
      ['meta', { name: 'twitter:image', content: ogImage }],
      ['script', { type: 'application/ld+json' }, JSON.stringify(jsonLd)],
    ],

    // Mermaid pulls in a few CJS-only transitive deps (dayjs among them). Vite's
    // dev server hands them to the browser unbundled, where `import dayjs from` has
    // no default export and the whole app fails to boot. Pre-bundling them fixes dev;
    // noExternal keeps mermaid inlined for the SSR build.
    vite: {
      optimizeDeps: { include: ['mermaid', 'dayjs'] },
      ssr: { noExternal: ['mermaid'] },
    },

    // Per-page canonical + og:url for clean SEO indexing.
    transformPageData(pageData) {
      const path = pageData.relativePath.replace(/index\.md$/, '').replace(/\.md$/, '');
      const canonical = `${hostname}${path}`;
      pageData.frontmatter.head ??= [];
      pageData.frontmatter.head.push(
        ['link', { rel: 'canonical', href: canonical }],
        ['meta', { property: 'og:url', content: canonical }],
      );
    },

    themeConfig: {
      logo: '/logo-mark.svg',

      // ─── Top navigation ──────────────────────────────────────────────
      nav: [
        { text: 'Guide', link: '/guide/getting-started', activeMatch: '/guide/' },
        { text: 'Reference', link: '/reference/protocol', activeMatch: '/reference/' },
        {
          // Hand-synced with package.json "version" — part of the release checklist.
          text: 'v2.0.0',
          items: [
            { text: 'Changelog', link: `${repo}/blob/main/CHANGELOG.md` },
            { text: 'npm', link: 'https://www.npmjs.com/package/@alexify/wrpc' },
            { text: 'Releases', link: `${repo}/releases` },
          ],
        },
      ],

      // ─── Sidebar ─────────────────────────────────────────────────────
      sidebar: {
        '/guide/': [
          {
            text: 'Introduction',
            items: [
              { text: 'Getting Started', link: '/guide/getting-started' },
              { text: 'Why wRPC?', link: '/guide/why' },
              { text: 'Architecture', link: '/guide/architecture' },
            ],
          },
          {
            text: 'Server',
            items: [
              { text: 'Server', link: '/guide/server' },
              { text: 'Router & procedures', link: '/guide/router' },
              { text: 'Declarative REST', link: '/guide/rest' },
              { text: 'Hooks', link: '/guide/hooks' },
              { text: 'Sessions', link: '/guide/sessions' },
              { text: 'Authentication', link: '/guide/auth' },
              { text: 'Metadata', link: '/guide/metadata' },
              { text: 'Rooms', link: '/guide/rooms' },
              { text: 'Subscriptions', link: '/guide/subscriptions' },
              { text: 'Binary streams', link: '/guide/streams' },
              { text: 'Scaling', link: '/guide/scaling' },
              { text: 'Cluster', link: '/guide/cluster' },
            ],
          },
          {
            text: 'Message brokers',
            items: [
              { text: 'Overview & contracts', link: '/guide/brokers' },
              { text: 'Durable feeds', link: '/guide/brokers/feeds' },
              { text: 'Queue consumers & publishing', link: '/guide/brokers/consumers' },
              { text: 'RPC over a broker', link: '/guide/brokers/rpc' },
              { text: 'Redis', link: '/guide/brokers/redis' },
              { text: 'NATS', link: '/guide/brokers/nats' },
              { text: 'RabbitMQ', link: '/guide/brokers/amqp' },
              { text: 'Kafka', link: '/guide/brokers/kafka' },
            ],
          },
          {
            text: 'Client',
            items: [
              { text: 'Client', link: '/guide/client' },
              { text: 'Typed client', link: '/guide/typed-client' },
              { text: 'Multiple backends', link: '/guide/multiple-backends' },
              { text: 'Browser & bundling', link: '/guide/browser' },
              { text: 'Codegen CLI', link: '/guide/cli' },
              { text: 'TanStack Query', link: '/guide/query' },
            ],
          },
          {
            text: 'Transports & hosts',
            items: [
              { text: 'Server-Sent Events', link: '/guide/sse' },
              { text: 'WebRTC', link: '/guide/webrtc' },
              { text: 'WebRTC: identity and trust', link: '/guide/webrtc-trust' },
              { text: 'WebTransport', link: '/guide/wt' },
              { text: 'Wire codec', link: '/guide/codec' },
              { text: 'uWebSockets.js', link: '/guide/adapters/uws' },
              { text: 'Fastify', link: '/guide/adapters/fastify' },
              { text: 'Express', link: '/guide/adapters/express' },
            ],
          },
          {
            text: 'Operations',
            items: [
              { text: 'Security', link: '/guide/security' },
              { text: 'Rate limiting & throttling', link: '/guide/rate-limiting' },
              { text: 'Running in production', link: '/guide/production' },
              { text: 'Testing', link: '/guide/testing' },
              { text: 'Performance', link: '/guide/performance' },
              { text: 'Compression', link: '/guide/compression' },
              { text: 'Encryption', link: '/guide/encryption' },
              { text: 'Logging', link: '/guide/logging' },
              { text: 'OpenTelemetry', link: '/guide/telemetry' },
            ],
          },
        ],
        '/reference/': [
          {
            text: 'Reference',
            items: [
              { text: 'Wire protocol', link: '/reference/protocol' },
              { text: 'Wire format', link: '/reference/wire-format' },
              { text: 'Engine port', link: '/reference/engine' },
              { text: 'Errors & close codes', link: '/reference/errors' },
              { text: 'Stability & deprecation', link: '/reference/stability' },
            ],
          },
        ],
      },

      // ─── Local, zero-config full-text search ─────────────────────────
      search: { provider: 'local' },

      socialLinks: [{ icon: 'github', link: repo }],

      editLink: {
        pattern: `${repo}/edit/main/docs/:path`,
        text: 'Edit this page on GitHub',
      },

      footer: {
        message: 'Released under the MIT License.',
        copyright: 'Copyright © 2026 Alexis Technologies',
      },

      docFooter: {
        prev: 'Previous page',
        next: 'Next page',
      },
    },
  }),

  // ─── Mermaid ───────────────────────────────────────────────────────────
  // Deliberately no `theme` here: vitepress-plugin-mermaid picks 'default' or
  // 'dark' from VitePress' own isDark and re-renders on toggle, and anything we
  // set would win over that and freeze the diagrams in one theme. Colors are
  // therefore left to mermaid; only typography and layout are ours.
  mermaid: {
    fontFamily: 'var(--vp-font-family-base)',
    flowchart: { useMaxWidth: true, htmlLabels: true, padding: 12 },
    sequence: { useMaxWidth: true, actorMargin: 40, wrap: true },
  },

  mermaidPlugin: {
    // `vp-raw` keeps VitePress' own markdown styling off the rendered SVG.
    class: 'mermaid vp-raw',
  },
});
