import { createContext, useContext } from 'react';

import type { Theme } from '@mui/material/styles';
import { useTheme } from '@mui/material/styles';
import useMediaQuery from '@mui/material/useMediaQuery';

// A container (e.g. a scanner card) that measures its own width provides the
// resulting narrow flag here so descendants adapt to the container instead of
// the viewport. `null` (the default) means "no container override — fall back
// to the viewport media query."
export const NarrowContext = createContext<boolean | null>(null);

// Shared narrow-layout check. Prefers a container-provided override when one is
// present; otherwise falls back to a narrow-viewport media query (below the
// `sm` breakpoint). `noSsr` evaluates before first paint so the wider layout
// doesn't flash on mount.
export function useIsNarrow(): boolean {
  const override = useContext(NarrowContext);
  const theme = useTheme();
  const viewportNarrow = useMediaQuery(theme.breakpoints.down('sm'), {
    noSsr: true,
  });
  return override ?? viewportNarrow;
}

// Name of the query container established on the FeedPanel root (via
// `containerType: 'inline-size'`). Descendants query it by name so a scanner
// card's layout tracks its own width, not the viewport.
export const FEED_PANEL_CONTAINER = 'feedpanel';

// sx key that applies at/above the wide (non-narrow) layout threshold, measured
// against the FeedPanel container width. Use as `{ ...narrowStyles, [wide]:
// wideStyles }`. The threshold tracks the theme `sm` breakpoint so it matches
// the viewport fallback in `useIsNarrow`.
export function feedPanelWideQuery(theme: Theme): string {
  return `@container ${FEED_PANEL_CONTAINER} (min-width:${theme.breakpoints.values.sm}px)`;
}
