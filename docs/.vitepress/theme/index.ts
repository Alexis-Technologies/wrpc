import DefaultTheme from 'vitepress/theme';
import type { Theme } from 'vitepress';
import './custom.css';

// Extends the default VitePress theme with our brand styling (see custom.css).
// This is exactly the pattern Pinia / Vite / Vue use to brand their docs — the
// layout and components stay the default theme, the look comes from CSS variables.
export default {
  extends: DefaultTheme,
  enhanceApp({ router }) {
    // Vercel Web Analytics + Speed Insights. The framework-agnostic `inject`
    // entrypoints are used rather than the `/vue` components: those require
    // `vue-router`, which VitePress does not use (it ships its own router).
    // Both are browser-only, so they are imported dynamically to stay out of
    // the SSR build that `pnpm docs:build` runs.
    if (import.meta.env.SSR) return;
    void import('@vercel/analytics').then(({ inject }) => {
      // `mode: 'auto'` logs to the console on localhost and only sends data
      // from the deployed site; the injected script picks up VitePress'
      // pushState navigations on its own, so no per-route call is needed.
      inject({ mode: 'auto', framework: 'vitepress' });
    });
    void import('@vercel/speed-insights').then(({ injectSpeedInsights }) => {
      const insights = injectSpeedInsights({ framework: 'vitepress' });
      if (!insights) return;
      // Speed Insights does need the route pushed to it by hand. Chain onto any
      // existing handler instead of replacing it.
      const previous = router.onAfterRouteChange;
      router.onAfterRouteChange = (to) => {
        insights.setRoute(to);
        return previous?.call(router, to);
      };
    });
  },
} satisfies Theme;
