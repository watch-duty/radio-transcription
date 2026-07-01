import { useMemo } from 'react';

import { AudioClassification, type AudioSegment } from '@transcription/common';

import { segmentHasAlert } from '../utils/annotationUtils';
import { TIMELINE_RANGE_DURATION_MS } from '../utils/timeUtils';

export interface HistogramMark {
  startMs: number;
  endMs: number;
  count: number;
  hasSpeech: boolean;
  hasAlert: boolean;
}

// 24h / 288 = 5-minute buckets.
const HISTOGRAM_BUCKETS = 288;
export const HISTOGRAM_BUCKET_DURATION_MS =
  TIMELINE_RANGE_DURATION_MS / HISTOGRAM_BUCKETS;

function bucketSegments(
  segments: AudioSegment[],
  rangeStartMs: number
): HistogramMark[] {
  const byStart = new Map<number, HistogramMark>();
  for (const segment of segments) {
    const t = new Date(segment.startTimestamp).getTime();
    const index = Math.floor((t - rangeStartMs) / HISTOGRAM_BUCKET_DURATION_MS);
    // Segments outside the overview range (older clips loaded by scrolling, or
    // anything past the right edge) don't get a mark.
    if (index < 0 || index >= HISTOGRAM_BUCKETS) continue;
    const startMs = rangeStartMs + index * HISTOGRAM_BUCKET_DURATION_MS;
    const mark = byStart.get(startMs) ?? {
      startMs,
      endMs: startMs + HISTOGRAM_BUCKET_DURATION_MS,
      count: 0,
      hasSpeech: false,
      hasAlert: false,
    };
    mark.count += 1;
    mark.hasSpeech =
      mark.hasSpeech || segment.classification === AudioClassification.SPEECH;
    mark.hasAlert = mark.hasAlert || segmentHasAlert(segment);
    byStart.set(startMs, mark);
  }
  return [...byStart.values()].sort((a, b) => a.startMs - b.startMs);
}

interface UseTimelineHistogramOptions {
  // The loaded segments; the overview buckets whichever fall in its 24h window.
  segments: AudioSegment[];
  // Right edge of the overview: date-filter time, else null = live edge (now).
  anchorTimestamp: Date | null;
}

// Buckets the loaded segments into 5-minute marks for the 24h timeline overview.
// A pure derivation over the segments the list already loaded — no fetch of its
// own (the list eagerly preloads the window; see useAudioSegments). Live mode
// anchors the window's right edge at the live edge; a date filter centers it on
// the picked time so an approximate guess keeps context on both sides.
export function useTimelineHistogram({
  segments,
  anchorTimestamp,
}: UseTimelineHistogramOptions) {
  const anchorMs = anchorTimestamp?.getTime() ?? null;

  return useMemo(() => {
    let rangeStartMs: number | null = null;

    if (anchorMs != null) {
      rangeStartMs = anchorMs - TIMELINE_RANGE_DURATION_MS / 2;
    } else {
      // Live: right edge = newest loaded segment's end. Scan rather than read
      // segments[0] — the list is sorted by start time, not end.
      let liveEdgeMs = -Infinity;
      for (const s of segments) {
        const end = new Date(s.endTimestamp).getTime();
        if (end > liveEdgeMs) liveEdgeMs = end;
      }
      if (liveEdgeMs !== -Infinity) {
        rangeStartMs = liveEdgeMs - TIMELINE_RANGE_DURATION_MS;
      }
    }

    if (rangeStartMs == null) {
      return {
        marks: [] as HistogramMark[],
        rangeStartMs: null as number | null,
        rangeEndMs: null as number | null,
      };
    }

    return {
      marks: bucketSegments(segments, rangeStartMs),
      rangeStartMs: rangeStartMs as number | null,
      rangeEndMs: (rangeStartMs + TIMELINE_RANGE_DURATION_MS) as number | null,
    };
  }, [segments, anchorMs]);
}
