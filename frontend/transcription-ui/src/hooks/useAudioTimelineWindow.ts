import { useCallback, useState } from 'react';

import { type AudioSegment } from '@transcription/common';

import { MAX_WINDOW_DURATION_MS } from '../utils/timeUtils';

// A window end within this of the live edge still counts as "live".
const LIVE_EDGE_EPS_MS = 1000;

// Whether a segment isn't fully inside the window ending at `windowEndMs`.
export function isSegmentOutsideWindow(
  tStart: number,
  tEnd: number,
  windowEndMs: number,
  windowDurationMs: number
): boolean {
  const windowStartMs = windowEndMs - windowDurationMs;
  return tStart < windowStartMs || tEnd > windowEndMs;
}

// Whether the window was still pinned to the previous live edge, so it should
// keep following new audio rather than stay where the user navigated to.
function isPinnedToLiveEdge(
  windowEndTime: number | null,
  prevLiveEnd: string | null,
  isInitialLoad: boolean
): boolean {
  if (isInitialLoad || !windowEndTime || !prevLiveEnd) return true;
  return (
    Math.abs(windowEndTime - new Date(prevLiveEnd).getTime()) < LIVE_EDGE_EPS_MS
  );
}

export interface AudioTimelineWindow {
  // The window's right edge (ms); null follows the live edge.
  windowEndTime: number | null;
  windowDurationMs: number;
  // True when the window is at the most recent loaded audio. Deliberately does
  // not claim real-time currency — there may be newer audio not yet fetched.
  isLatestTimeWindow: boolean;
  jumpToLive: () => void;
  // Move the window so its center sits at the given time, clamped to the live edge.
  centerWindowOn: (centerMs: number) => void;
}

interface UseAudioTimelineWindowParams {
  // The consolidated (rendered) segments the timeline displays, newest-first —
  // not the raw query stream. The window follows the head the user actually
  // sees, so it keys off the rendered list; this can differ from the raw head
  // mid-bundle (an ongoing silence bundle keeps one rendered entry while its
  // raw segments change), which is why list anchoring keys off raw instead.
  audioSegments: AudioSegment[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  // Changes when the list is replaced wholesale (feed / timestamp / filter).
  // Resets the window to the live edge.
  resetKey: string;
}

// Single source of truth for the timeline's visible window. Lifted out of
// AudioDisplay so the window state can also drive the controls (jump-to-live,
// the date/time chip) and, later, the mini-map.
export function useAudioTimelineWindow({
  audioSegments,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
  resetKey,
}: UseAudioTimelineWindowParams): AudioTimelineWindow {
  const windowDurationMs = MAX_WINDOW_DURATION_MS;
  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);

  // Previous-render snapshot, so input changes are handled during render rather
  // than in a syncing effect.
  const [prev, setPrev] = useState<{
    resetKey: string;
    firstId: string | null;
    firstEnd: string | null;
    highlightedId: string | null;
  }>({
    resetKey,
    firstId: null,
    firstEnd: null,
    highlightedId: null,
  });

  const firstSegment = audioSegments[0];
  const firstId = firstSegment?.id ?? null;
  const firstEnd = firstSegment?.endTimestamp ?? null;
  const liveEnd = firstEnd ? new Date(firstEnd).getTime() : null;

  // Keep the last non-empty head across transient blanks so a refill isn't
  // mistaken for an initial load (which would snap a scrolled-back window live).
  const nextFirstId = firstId !== null ? firstId : prev.firstId;
  const nextFirstEnd = firstId !== null ? firstEnd : prev.firstEnd;

  const isLatestTimeWindow =
    windowEndTime == null ||
    liveEnd == null ||
    windowEndTime >= liveEnd - LIVE_EDGE_EPS_MS;

  const headChanged =
    firstId !== null &&
    (firstId !== prev.firstId || firstEnd !== prev.firstEnd);
  // Highlight-driven (playback also sets the highlight), so stopping playback
  // alone doesn't read as a new selection and yank the window.
  const selectionChanged = highlightedSegmentId !== prev.highlightedId;

  // A wholesale list replacement (feed / timestamp / filter switch) returns to
  // the live edge, even if the user had scrubbed back in the previous list.
  if (resetKey !== prev.resetKey) {
    if (windowEndTime !== null) setWindowEndTime(null);
    setPrev({
      resetKey,
      firstId: nextFirstId,
      firstEnd: nextFirstEnd,
      highlightedId: highlightedSegmentId,
    });
  } else if (headChanged || selectionChanged) {
    // Order matters: follow the live edge first, then recenter the selection
    // against that (possibly advanced) edge.
    let nextEnd = windowEndTime;

    // Follow the live edge as new audio arrives or the head extends (an ongoing
    // silence bundle keeps its id but gets a later end), unless viewing back.
    if (
      headChanged &&
      isPinnedToLiveEdge(nextEnd, prev.firstEnd, !prev.firstId)
    ) {
      nextEnd = liveEnd;
    }

    // Recenter only on a selection change — not on head polls — so navigating
    // the window away from the selected clip (e.g. a mini-map click) isn't undone.
    if (selectionChanged) {
      const targetId = highlightedSegmentId || currentlyPlayingSegmentId;
      const target = targetId
        ? audioSegments.find((t) => t.id === targetId)
        : undefined;
      if (target) {
        const tStart = new Date(target.startTimestamp).getTime();
        const tEnd = new Date(target.endTimestamp).getTime();
        const newestEnd = liveEnd ?? 0;
        const currentEnd = nextEnd ?? newestEnd;
        if (
          isSegmentOutsideWindow(tStart, tEnd, currentEnd, windowDurationMs)
        ) {
          nextEnd = Math.min(tStart + windowDurationMs / 2, newestEnd);
        }
      }
    }

    if (nextEnd !== windowEndTime) {
      setWindowEndTime(nextEnd);
    }
    setPrev({
      resetKey,
      firstId: nextFirstId,
      firstEnd: nextFirstEnd,
      highlightedId: highlightedSegmentId,
    });
  }

  const jumpToLive = useCallback(() => {
    setWindowEndTime(null);
  }, []);

  const centerWindowOn = useCallback(
    (centerMs: number) => {
      const target = centerMs + windowDurationMs / 2;
      setWindowEndTime(liveEnd != null ? Math.min(target, liveEnd) : target);
    },
    [windowDurationMs, liveEnd]
  );

  return {
    windowEndTime,
    windowDurationMs,
    isLatestTimeWindow,
    jumpToLive,
    centerWindowOn,
  };
}
