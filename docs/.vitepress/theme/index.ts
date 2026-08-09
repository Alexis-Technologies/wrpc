import DefaultTheme from 'vitepress/theme';
import type { Theme } from 'vitepress';

// Minimal skeleton: no brand overrides yet. Add custom.css here once the
// visual identity is decided, following the pattern in kerberos/docs/.vitepress/theme.
export default {
  extends: DefaultTheme,
} satisfies Theme;
