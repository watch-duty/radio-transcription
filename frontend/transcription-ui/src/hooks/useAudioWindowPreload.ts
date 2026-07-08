import { useEffect, useRef } from 'react';

import { type AudioSegment } from '@transcription/common';

import { getLiveEdgeMs } from '../utils/timeUtils';

// Safety bound on the eager preload for pathological feeds (per direction).
const MAX_PRELOAD_PAGES = 30;

// Per-query preload progress. pagesFetched bounds each direction; cappedWarned
// keeps the cap warning to once per side per query (not once per render).
// lastBoundary is the segment id a fetch last kicked off from, so a re-render
// (or a page that didn't extend the range) doesn't re-fetch it — only a moved
// edge pages again. Keyed on id, not timestamp: co-timed segments (silence
// bundles share a start) would otherwise read as an unmoved edge.
type PreloadProgress = {
  olderPages: number;
  newerPages: number;
  olderCappedWarned: boolean;
  newerCappedWarned: boolean;
  lastOlderBoundary: string | null;
  lastNewerBoundary: string | null;
};

const INITIAL_PRELOAD_PROGRESS: PreloadProgress = {
  olderPages: 0,
  newerPages: 0,
  olderCappedWarned: false,
  newerCappedWarned: false,
  lastOlderBoundary: null,
  lastNewerBoundary: null,
};

interface UseAudioWindowPreloadOptions {
  // When false, the hook pages nothing; callers pass their query-success flag so
  // preloading only starts once the first page (and its anchor) has loaded.
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
  const progressRef = useRef<PreloadProgress>({ ...INITIAL_PRELOAD_PROGRESS });
  const prevResetKeyRef = useRef(resetKey);

  useEffect(() => {
    // A query change (new resetKey) restarts pagination from page one, so zero
    // the per-query counters before the paging logic below reads them.
    if (prevResetKeyRef.current !== resetKey) {
      prevResetKeyRef.current = resetKey;
      progressRef.current = { ...INITIAL_PRELOAD_PROGRESS };
    }

    if (!enabled || !windowMs || segments.length === 0) return;

    const warnCappedOnce = (side: 'older' | 'newer') => {
      const key = side === 'older' ? 'olderCappedWarned' : 'newerCappedWarned';
      if (progressRef.current[key]) return;
      progressRef.current[key] = true;
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

    const oldest = segments[segments.length - 1];
    const oldestMs = new Date(oldest.startTimestamp).getTime();
    if (
      oldestMs > winStartMs &&
      hasOlder &&
      !isFetchingOlder &&
      oldest.id !== progressRef.current.lastOlderBoundary
    ) {
      if (progressRef.current.olderPages < MAX_PRELOAD_PAGES) {
        progressRef.current.olderPages += 1;
        progressRef.current.lastOlderBoundary = oldest.id;
        fetchOlder();
        return;
      }
      warnCappedOnce('older');
    }

    // Date mode also fills toward the future side of the centered window.
    const newest = segments[0];
    const newestMs = new Date(newest.startTimestamp).getTime();
    if (
      winEndMs != null &&
      newestMs < winEndMs &&
      hasNewer &&
      !isFetchingNewer &&
      newest.id !== progressRef.current.lastNewerBoundary
    ) {
      if (progressRef.current.newerPages < MAX_PRELOAD_PAGES) {
        progressRef.current.newerPages += 1;
        progressRef.current.lastNewerBoundary = newest.id;
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
