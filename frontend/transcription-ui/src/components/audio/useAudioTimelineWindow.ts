import { useCallback, useEffect, useState } from 'react';

import { type AudioSegment } from '@transcription/common';

import {
  DEFAULT_AUDIO_WINDOW_DURATION_MS,
  TIMELINE_RANGE_DURATION_MS,
} from '../../utils/timeUtils';
import { clamp } from './timelineMath';

// A scrubbed window end within this of the live edge still counts as "live".
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
  rangeStartMs: number | null;
  maxEnd: number | null;
  // Center the window on `centerMs`, clamped to the overview range.
  scrubToCenter: (centerMs: number) => void;
  jumpToLive: () => void;
}

interface UseAudioTimelineWindowParams {
  // Newest-first, as returned by the audio-segments query.
  audioSegments: AudioSegment[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  // Right edge of the fixed 24h overview (date-filter time, else null=live).
  overviewAnchorMs: number | null;
  windowDurationMs?: number;
}

// Single source of truth for the timeline's visible window, shared by the
// waveform display, the overview mini-map, the date/time chip, and list-following.
export function useAudioTimelineWindow({
  audioSegments,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
  overviewAnchorMs,
  windowDurationMs = DEFAULT_AUDIO_WINDOW_DURATION_MS,
}: UseAudioTimelineWindowParams): AudioTimelineWindow {
  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);

  // Overview right edge; not Date.now() in render, which must stay pure.
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 30_000);
    return () => clearInterval(id);
  }, []);

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

  // Live edge of loaded audio — drives follow-live and the scrubbed check.
  const liveEnd = firstEnd ? new Date(firstEnd).getTime() : null;

  // Fixed 24h overview range, decoupled from the lazy list so scrubs don't shift it.
  const rangeEndMs = Math.max(overviewAnchorMs ?? nowMs, liveEnd ?? 0);
  const rangeStartMs = rangeEndMs - TIMELINE_RANGE_DURATION_MS;
  const maxEnd = rangeEndMs;
  const minEnd = rangeStartMs + windowDurationMs;

  // Derived, not stored, so it stays correct however the window moved (scrub,
  // clicking an old clip, jump-to-live): scrubbed iff the right edge sits before
  // the live edge of loaded audio.
  const isScrubbed =
    windowEndTime != null &&
    liveEnd != null &&
    windowEndTime < liveEnd - LIVE_EDGE_EPS_MS;

  // Follow the live edge as new audio arrives, or as the head segment extends
  // (e.g. an ongoing silence bundle keeps the same id but a later end), unless
  // the user has scrubbed away.
  // Skip empty lists: a refetch blank must not read as a fresh initial load.
  if (
    firstId !== null &&
    (firstId !== prevFirstId || firstEnd !== prevFirstEnd)
  ) {
    const pinned = isPinnedToLiveEdge(
      windowEndTime,
      prevFirstEnd,
      !prevFirstId
    );
    setPrevFirstId(firstId);
    setPrevFirstEnd(firstEnd);
    if (pinned) {
      setWindowEndTime(firstEnd ? new Date(firstEnd).getTime() : null);
    }
    setPrevPlayingId(null); // re-check bounds below
  }

  // Recenter when the playing/highlighted segment leaves the window. A highlight
  // differing from the playing clip is an explicit pick (clicking a clip/row); one
  // equal to it is just playback (which also moves the highlight, so the list
  // follows). Only an explicit pick recenters while scrubbed.
  if (
    currentlyPlayingSegmentId !== prevPlayingId ||
    highlightedSegmentId !== prevHighlightedId
  ) {
    const explicitPick =
      highlightedSegmentId !== prevHighlightedId &&
      highlightedSegmentId !== currentlyPlayingSegmentId;
    setPrevPlayingId(currentlyPlayingSegmentId);
    setPrevHighlightedId(highlightedSegmentId);

    const allowRecenter = !isScrubbed || explicitPick;
    const targetId = explicitPick
      ? highlightedSegmentId
      : currentlyPlayingSegmentId;
    if (allowRecenter && targetId) {
      const target = audioSegments.find((t) => t.id === targetId);
      if (target) {
        const tStart = new Date(target.startTimestamp).getTime();
        const tEnd = new Date(target.endTimestamp).getTime();
        const newestEnd = liveEnd ?? 0;
        const currentEnd = windowEndTime || newestEnd;
        const currentStart = currentEnd - windowDurationMs;
        if (tStart < currentStart || tEnd > currentEnd) {
          setWindowEndTime(Math.min(tStart + windowDurationMs / 2, newestEnd));
        }
      }
    }
  }

  const scrubToCenter = useCallback(
    (centerMs: number): void => {
      const half = windowDurationMs / 2;
      setWindowEndTime(clamp(centerMs + half, minEnd, maxEnd));
    },
    [minEnd, maxEnd, windowDurationMs]
  );

  const jumpToLive = useCallback(() => {
    setWindowEndTime(null);
  }, []);

  return {
    windowEndTime,
    windowDurationMs,
    isScrubbed,
    rangeStartMs,
    maxEnd,
    scrubToCenter,
    jumpToLive,
  };
}
