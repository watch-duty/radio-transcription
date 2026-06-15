import { useCallback, useState } from 'react';

import { type AudioSegment } from '@transcription/common';

import { DEFAULT_AUDIO_WINDOW_DURATION_MS } from '../../utils/timeUtils';

// A window end within this of the live edge still counts as "live".
const LIVE_EDGE_EPS_MS = 1000;

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
  isScrubbed: boolean;
  jumpToLive: () => void;
}

interface UseAudioTimelineWindowParams {
  // Newest-first, as returned by the audio-segments query.
  audioSegments: AudioSegment[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  windowDurationMs?: number;
}

// Single source of truth for the timeline's visible window, shared by the
// waveform display and the date/time chip.
export function useAudioTimelineWindow({
  audioSegments,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
  windowDurationMs = DEFAULT_AUDIO_WINDOW_DURATION_MS,
}: UseAudioTimelineWindowParams): AudioTimelineWindow {
  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);

  // Previous-value trackers for the render-time follow/recenter transitions.
  const [prevFirstId, setPrevFirstId] = useState<string | null>(null);
  const [prevFirstEnd, setPrevFirstEnd] = useState<string | null>(null);
  const [prevPlayingId, setPrevPlayingId] = useState<string | null>(null);
  const [prevHighlightedId, setPrevHighlightedId] = useState<string | null>(
    null
  );

  const firstSegment = audioSegments[0];
  const firstId = firstSegment?.id ?? null;
  const firstEnd = firstSegment?.endTimestamp ?? null;
  const liveEdge = firstEnd ? new Date(firstEnd).getTime() : null;

  // Derived, not stored, so it stays correct however the window moved: scrubbed
  // iff the right edge sits before the live edge.
  const isScrubbed =
    windowEndTime != null &&
    liveEdge != null &&
    windowEndTime < liveEdge - LIVE_EDGE_EPS_MS;

  // Follow the live edge as new audio arrives, or as the head segment extends
  // (e.g. an ongoing silence bundle keeps the same id but a later end), unless
  // the user has navigated away.
  if (firstId !== prevFirstId || firstEnd !== prevFirstEnd) {
    const pinned = isPinnedToLiveEdge(
      windowEndTime,
      prevFirstEnd,
      !prevFirstId
    );
    setPrevFirstId(firstId);
    setPrevFirstEnd(firstEnd);
    if (pinned) {
      setWindowEndTime(liveEdge);
    }
    setPrevPlayingId(null); // re-check bounds below
  }

  // Recenter when the playing or highlighted segment leaves the window.
  if (
    currentlyPlayingSegmentId !== prevPlayingId ||
    highlightedSegmentId !== prevHighlightedId
  ) {
    setPrevPlayingId(currentlyPlayingSegmentId);
    setPrevHighlightedId(highlightedSegmentId);

    const targetId = highlightedSegmentId || currentlyPlayingSegmentId;
    if (targetId) {
      const target = audioSegments.find((t) => t.id === targetId);
      if (target) {
        const tStart = new Date(target.startTimestamp).getTime();
        const tEnd = new Date(target.endTimestamp).getTime();
        const newestEnd = liveEdge ?? 0;
        const currentEnd = windowEndTime || newestEnd;
        const currentStart = currentEnd - windowDurationMs;
        if (tStart < currentStart || tEnd > currentEnd) {
          setWindowEndTime(Math.min(tStart + windowDurationMs / 2, newestEnd));
        }
      }
    }
  }

  const jumpToLive = useCallback(() => {
    setWindowEndTime(null);
  }, []);

  return {
    windowEndTime,
    windowDurationMs,
    isScrubbed,
    jumpToLive,
  };
}
