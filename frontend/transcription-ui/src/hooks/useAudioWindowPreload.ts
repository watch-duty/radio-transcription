import { useEffect, useRef } from 'react';

import { type AudioSegment } from '@transcription/common';

import { getLiveEdgeMs } from '../utils/timeUtils';

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
// ([liveEdge - window, liveEdge]); date-filter mode centers the window on the picked time
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
  // One flag per direction so a capped feed warns once per side per query (not
  // spamming every render) while still surfacing both sides when both cap.
  const olderCappedWarnedRef = useRef(false);
  const newerCappedWarnedRef = useRef(false);
  const prevResetKeyRef = useRef(resetKey);

  useEffect(() => {
    // A query change (new resetKey) restarts pagination from page one, so zero
    // the per-query counters before the paging logic below reads them.
    if (prevResetKeyRef.current !== resetKey) {
      prevResetKeyRef.current = resetKey;
      olderPagesRef.current = 0;
      newerPagesRef.current = 0;
      olderCappedWarnedRef.current = false;
      newerCappedWarnedRef.current = false;
    }

    if (!enabled || !windowMs || segments.length === 0) return;

    const warnCappedOnce = (side: 'older' | 'newer') => {
      const warnedRef =
        side === 'older' ? olderCappedWarnedRef : newerCappedWarnedRef;
      if (warnedRef.current) return;
      warnedRef.current = true;
      console.warn(
        `useAudioWindowPreload: hit the ${MAX_PRELOAD_PAGES}-page cap paging ${side} ` +
          `before covering the ${windowMs}ms window; timeline overview density may be incomplete for this feed.`
      );
    };

    // Anchor on the same live edge the histogram uses (see useTimelineHistogram)
    // so the preload covers what the mini-map renders even on a quiet feed.
    const anchorMs = anchorTimestamp?.getTime() ?? null;
    const liveEdgeMs = getLiveEdgeMs(segments);
    if (anchorMs == null && liveEdgeMs == null) return;
    const winStartMs =
      anchorMs != null
        ? anchorMs - windowMs / 2
        : (liveEdgeMs as number) - windowMs;
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
    resetKey,
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
