import { useCallback, useState } from 'react';

import { type AudioSegment } from '@transcription/common';

import { AUDIO_WINDOW_DURATION_MS } from '../../utils/timeUtils';

// A window end within this of the live edge still counts as "live".
const LIVE_EDGE_EPS_MS = 1000;

// Whether a segment isn't fully inside the window ending at `windowEndMs` —
// i.e. selecting/playing it would move (recenter) the window. Shared by the
// hook's recenter and by callers deciding whether a navigation moves the window.
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
}

interface UseAudioTimelineWindowParams {
  // Newest-first, as returned by the audio-segments query.
  audioSegments: AudioSegment[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
}

// Single source of truth for the timeline's visible window, shared by the
// waveform display and the date/time chip. Lifted out of AudioDisplay so the
// window state can also drive the controls.
export function useAudioTimelineWindow({
  audioSegments,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
}: UseAudioTimelineWindowParams): AudioTimelineWindow {
  const windowDurationMs = AUDIO_WINDOW_DURATION_MS;
  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);

  // Snapshot of the inputs from the previous render so we can react to their
  // changes during render — the recommended alternative to a syncing effect.
  const [prev, setPrev] = useState<{
    firstId: string | null;
    firstEnd: string | null;
    playingId: string | null;
    highlightedId: string | null;
  }>({ firstId: null, firstEnd: null, playingId: null, highlightedId: null });

  const firstSegment = audioSegments[0];
  const firstId = firstSegment?.id ?? null;
  const firstEnd = firstSegment?.endTimestamp ?? null;
  const liveEnd = firstEnd ? new Date(firstEnd).getTime() : null;

  // Derived so it stays correct however the window moved: the window's right
  // edge sits at (or within epsilon of) the newest loaded audio.
  const isLatestTimeWindow =
    windowEndTime == null ||
    liveEnd == null ||
    windowEndTime >= liveEnd - LIVE_EDGE_EPS_MS;

  // Skip empty lists: a refetch blank must not read as a fresh initial load.
  const headChanged =
    firstId !== null &&
    (firstId !== prev.firstId || firstEnd !== prev.firstEnd);
  const selectionChanged =
    currentlyPlayingSegmentId !== prev.playingId ||
    highlightedSegmentId !== prev.highlightedId;

  // React to changed inputs in one render pass: follow the live edge, then
  // recenter on the selection against that (possibly advanced) edge.
  if (headChanged || selectionChanged) {
    let nextEnd = windowEndTime;

    // Follow the live edge as new audio arrives or the head segment extends (an
    // ongoing silence bundle keeps its id but gets a later end), unless the user
    // navigated back.
    if (
      headChanged &&
      isPinnedToLiveEdge(nextEnd, prev.firstEnd, !prev.firstId)
    ) {
      nextEnd = liveEnd;
    }

    // Recenter when the playing or highlighted segment falls outside the window.
    const targetId = highlightedSegmentId || currentlyPlayingSegmentId;
    if (targetId) {
      const target = audioSegments.find((t) => t.id === targetId);
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
      firstId,
      firstEnd,
      playingId: currentlyPlayingSegmentId,
      highlightedId: highlightedSegmentId,
    });
  }

  const jumpToLive = useCallback(() => {
    setWindowEndTime(null);
  }, []);

  return { windowEndTime, windowDurationMs, isLatestTimeWindow, jumpToLive };
}
