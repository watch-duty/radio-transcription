import { useCallback, useMemo, useState } from 'react';

import { type AudioSegment } from '@transcription/common';

import { findEvaluationAnnotationData } from '../../utils/annotationUtils';
import {
  DEFAULT_AUDIO_WINDOW_DURATION_MS,
  TIMELINE_RANGE_DURATION_MS,
} from '../../utils/timeUtils';
import {
  type TranscriptTime,
  clamp,
  representativeSegmentId,
} from './timelineMath';

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
  miniMapTimes: TranscriptTime[];
  // Center the window on `centerMs`. Returns the segment nearest that center so
  // the caller can scroll the list to match what the window now shows.
  scrubToCenter: (centerMs: number) => string | null;
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
// waveform display, the overview mini-map, the date/time chip, and list-following.
export function useAudioTimelineWindow({
  audioSegments,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
  windowDurationMs = DEFAULT_AUDIO_WINDOW_DURATION_MS,
}: UseAudioTimelineWindowParams): AudioTimelineWindow {
  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);
  const [isScrubbed, setIsScrubbed] = useState(false);

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

  // Follow the live edge as new audio arrives, or as the head segment extends
  // (e.g. an ongoing silence bundle keeps the same id but a later end), unless
  // the user has scrubbed away.
  if (firstId !== prevFirstId || firstEnd !== prevFirstEnd) {
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

  // Recenter on the playing/highlighted segment when it leaves the window. While
  // scrubbed, playback advancing must not yank the window — only an explicit
  // highlight (clicking a clip/row) recenters, which also exits scrub mode.
  if (
    currentlyPlayingSegmentId !== prevPlayingId ||
    highlightedSegmentId !== prevHighlightedId
  ) {
    const highlightChanged = highlightedSegmentId !== prevHighlightedId;
    setPrevPlayingId(currentlyPlayingSegmentId);
    setPrevHighlightedId(highlightedSegmentId);
    if (highlightChanged && isScrubbed) setIsScrubbed(false);

    const allowRecenter = !isScrubbed || highlightChanged;
    const targetId = highlightedSegmentId || currentlyPlayingSegmentId;
    if (allowRecenter && targetId) {
      const target = audioSegments.find((t) => t.id === targetId);
      if (target) {
        const tStart = new Date(target.startTimestamp).getTime();
        const tEnd = new Date(target.endTimestamp).getTime();
        const newestEnd = firstEnd ? new Date(firstEnd).getTime() : 0;
        const currentEnd = windowEndTime || newestEnd;
        const currentStart = currentEnd - windowDurationMs;
        if (tStart < currentStart || tEnd > currentEnd) {
          setWindowEndTime(Math.min(tStart + windowDurationMs / 2, newestEnd));
        }
      }
    }
  }

  // Overview of the mini-map's range (a fixed 24h ending at the live edge):
  // per-segment timing + alert, plus the span bounds. maxEnd is the live edge —
  // the value the window follows.
  const { miniMapTimes, rangeStartMs, maxEnd, minEnd } = useMemo(() => {
    if (audioSegments.length === 0) {
      return {
        miniMapTimes: [] as TranscriptTime[],
        rangeStartMs: null as number | null,
        maxEnd: null as number | null,
        minEnd: null as number | null,
      };
    }
    const miniMapTimes = audioSegments.map((t): TranscriptTime => {
      const startMs = new Date(t.startTimestamp).getTime();
      const endMs = new Date(t.endTimestamp).getTime();
      const evaluation = findEvaluationAnnotationData(t.annotations);
      return {
        id: t.id,
        startMs,
        endMs,
        hasAlert: !!evaluation && evaluation.decisions.length > 0,
      };
    });
    const liveEdge = new Date(audioSegments[0].endTimestamp).getTime();
    const rangeStart = liveEdge - TIMELINE_RANGE_DURATION_MS;
    return {
      miniMapTimes,
      rangeStartMs: rangeStart,
      maxEnd: liveEdge,
      minEnd: Math.min(rangeStart + windowDurationMs, liveEdge),
    };
  }, [audioSegments, windowDurationMs]);

  const scrubToCenter = useCallback(
    (centerMs: number): string | null => {
      if (minEnd == null || maxEnd == null) return null;
      const half = windowDurationMs / 2;
      const end = clamp(centerMs + half, minEnd, maxEnd);
      setWindowEndTime(end);
      setIsScrubbed(end < maxEnd - LIVE_EDGE_EPS_MS);
      return representativeSegmentId(miniMapTimes, end - half);
    },
    [minEnd, maxEnd, windowDurationMs, miniMapTimes]
  );

  const jumpToLive = useCallback(() => {
    setWindowEndTime(null);
    setIsScrubbed(false);
  }, []);

  return {
    windowEndTime,
    windowDurationMs,
    isScrubbed,
    rangeStartMs,
    maxEnd,
    miniMapTimes,
    scrubToCenter,
    jumpToLive,
  };
}
