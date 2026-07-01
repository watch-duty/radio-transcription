import { useEffect, useRef } from 'react';

import { type AudioSegment } from '@transcription/common';

// Safety bound on the eager preload for pathological feeds (per direction).
const MAX_PRELOAD_PAGES = 30;

interface UseAudioWindowPreloadOptions {
  // Gate on the initial load having settled.
  enabled: boolean;
  // Total span to fill; undefined disables the preload.
  windowMs: number | undefined;
  // Date-filter time (centers the window on it), else null = live edge.
  anchorTimestamp: Date | null;
  // Loaded segments, newest-first.
  segments: AudioSegment[];
  hasOlder: boolean;
  hasNewer: boolean;
  isFetchingOlder: boolean;
  isFetchingNewer: boolean;
  fetchOlder: () => void;
  fetchNewer: () => void;
  // Identity of the current query; resets the page counters when it changes.
  resetKey: string;
}

// Eagerly pages the timeline overview window into the list, reusing the same
// older/newer fetches as continuous scroll, so the mini-map and in-window
// navigation need no separate fetch. Live mode pages older from the live edge
// ([now - window, now]); date-filter mode centers the window on the picked time
// ([T - window/2, T + window/2]) and pages both ways.
export function useAudioWindowPreload({
  enabled,
  windowMs,
  anchorTimestamp,
  segments,
  hasOlder,
  hasNewer,
  isFetchingOlder,
  isFetchingNewer,
  fetchOlder,
  fetchNewer,
  resetKey,
}: UseAudioWindowPreloadOptions): void {
  const olderPagesRef = useRef(0);
  const newerPagesRef = useRef(0);
  // Warn at most once per query so a busy feed that can't be covered within the
  // page cap doesn't spam the console on every render.
  const cappedWarnedRef = useRef(false);

  useEffect(() => {
    olderPagesRef.current = 0;
    newerPagesRef.current = 0;
    cappedWarnedRef.current = false;
  }, [resetKey]);

  useEffect(() => {
    if (!enabled || !windowMs || segments.length === 0) return;

    const warnCappedOnce = (side: 'older' | 'newer') => {
      if (cappedWarnedRef.current) return;
      cappedWarnedRef.current = true;
      console.warn(
        `useAudioWindowPreload: hit the ${MAX_PRELOAD_PAGES}-page cap paging ${side} ` +
          `before covering the ${windowMs}ms window; timeline overview density may be incomplete for this feed.`
      );
    };

    const anchorMs = anchorTimestamp?.getTime() ?? null;
    const winStartMs =
      anchorMs != null ? anchorMs - windowMs / 2 : Date.now() - windowMs;
    const winEndMs = anchorMs != null ? anchorMs + windowMs / 2 : null;

    const oldestMs = new Date(
      segments[segments.length - 1].startTimestamp
    ).getTime();
    if (oldestMs > winStartMs && hasOlder && !isFetchingOlder) {
      if (olderPagesRef.current < MAX_PRELOAD_PAGES) {
        olderPagesRef.current += 1;
        fetchOlder();
        return;
      }
      warnCappedOnce('older');
    }

    // Date mode also fills toward the future side of the centered window.
    const newestMs = new Date(segments[0].startTimestamp).getTime();
    if (
      winEndMs != null &&
      newestMs < winEndMs &&
      hasNewer &&
      !isFetchingNewer
    ) {
      if (newerPagesRef.current < MAX_PRELOAD_PAGES) {
        newerPagesRef.current += 1;
        fetchNewer();
        return;
      }
      warnCappedOnce('newer');
    }
  }, [
    enabled,
    windowMs,
    anchorTimestamp,
    segments,
    hasOlder,
    hasNewer,
    isFetchingOlder,
    isFetchingNewer,
    fetchOlder,
    fetchNewer,
  ]);
}
