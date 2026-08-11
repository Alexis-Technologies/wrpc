import { defineConfig } from 'vitepress';
import { withMermaid } from 'vitepress-plugin-mermaid';

const ogDescription =
  'Fast and low overhead, zero-dependency, WebSocket-based RPC protocol for Node.js and browsers.';
const repo = 'https://github.com/Alexis-Technologies/wrpc';
const base = '/';
const hostname = 'https://wrpc.vercel.app/';

// https://vitepress.dev/reference/site-config
export default withMermaid(
  defineConfig({
    title: '@alexify/wrpc',
    titleTemplate: ':title — wrpc',
    description: ogDescription,
    lang: 'en-US',
    base,
    cleanUrls: true,
    lastUpdated: true,
    sitemap: { hostname },

    head: [
      ['meta', { name: 'author', content: 'Alexis Technologies' }],
      ['meta', { name: 'robots', content: 'index, follow' }],
      ['meta', { property: 'og:type', content: 'website' }],
      ['meta', { property: 'og:site_name', content: '@alexify/wrpc' }],
      ['meta', { property: 'og:description', content: ogDescription }],
    ],

    themeConfig: {
      nav: [
        { text: 'Guide', link: '/guide/getting-started', activeMatch: '/guide/' },
        { text: 'Reference', link: '/reference/protocol', activeMatch: '/reference/' },
      ],

      sidebar: {
        '/guide/': [
          {
            text: 'Introduction',
            items: [{ text: 'Getting Started', link: '/guide/getting-started' }],
          },
        ],
        '/reference/': [
          {
            text: 'Reference',
            items: [{ text: 'Wire protocol', link: '/reference/protocol' }],
          },
        ],
      },

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
    },
  }),
);
