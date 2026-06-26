import { useQuery } from '@tanstack/react-query';
import { AudioClassification, type AudioSegment } from '@transcription/common';

import { listAudioSegments } from '../service/listAudioSegments';
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

// Long interval; the live edge advances on re-render, not from this poll.
const HISTOGRAM_REFETCH_INTERVAL_MS = 5 * 60 * 1000;

const PAGE_LIMIT = 1000;
// Safety bound on the number of pages fetched for the 24h range.
const MAX_PAGES = 25;

interface UseTimelineHistogramOptions {
  token: string | null;
  searchedFeedId: string | null;
  isFeedsSuccess: boolean;
  // Overview anchor: date-filter time, else null = live edge.
  anchorTimestamp: Date | null;
}

interface TimelineHistogram {
  marks: HistogramMark[];
  rangeStartMs: number;
  rangeEndMs: number;
}

function bucketSegments(
  segments: AudioSegment[],
  rangeStartMs: number
): HistogramMark[] {
  const byStart = new Map<number, HistogramMark>();
  for (const segment of segments) {
    const t = new Date(segment.startTimestamp).getTime();
    const index = Math.floor((t - rangeStartMs) / HISTOGRAM_BUCKET_DURATION_MS);
    // The server filters on end_timestamp, so a segment that started before the
    // window can come back; drop it rather than emit an out-of-range mark.
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

// Bucketed segment counts for the 24h overview, computed client-side from the
// existing audio-segments list endpoint.
export function useTimelineHistogram({
  token,
  searchedFeedId,
  isFeedsSuccess,
  anchorTimestamp,
}: UseTimelineHistogramOptions) {
  const anchorMs = anchorTimestamp?.getTime() ?? null;

  const { data, isLoading, isError } = useQuery<TimelineHistogram, Error>({
    queryKey: ['timelineHistogram', token, searchedFeedId, anchorMs],
    queryFn: async () => {
      const rangeEndMs = anchorMs ?? Date.now();
      const rangeStartMs = rangeEndMs - TIMELINE_RANGE_DURATION_MS;

      const segments: AudioSegment[] = [];
      let nextToken: string | undefined;
      let page = 0;
      do {
        const result = await listAudioSegments(
          searchedFeedId ?? '',
          token ?? '',
          PAGE_LIMIT,
          nextToken,
          rangeStartMs,
          rangeEndMs,
          // Newest-first so the page cap, if hit, drops the oldest end of the
          // 24h range rather than the live edge.
          'desc'
        );
        segments.push(...result.segments);
        nextToken = result.nextToken;
        page += 1;
      } while (nextToken && page < MAX_PAGES);

      if (nextToken) {
        console.warn(
          `useTimelineHistogram: hit ${MAX_PAGES}-page cap; oldest end of 24h histogram may be incomplete.`
        );
      }

      return {
        marks: bucketSegments(segments, rangeStartMs),
        rangeStartMs,
        rangeEndMs,
      };
    },
    enabled: !!token && !!searchedFeedId && isFeedsSuccess,
    refetchOnWindowFocus: false,
    refetchInterval: HISTOGRAM_REFETCH_INTERVAL_MS,
    // Keep the prior result across a same-feed/anchor refetch; clear it when the
    // feed or anchor changes so we never surface another query's histogram.
    placeholderData: (prev, prevQuery) =>
      prevQuery?.queryKey[2] === searchedFeedId &&
      prevQuery?.queryKey[3] === anchorMs
        ? prev
        : undefined,
  });

  return {
    marks: data?.marks ?? [],
    rangeStartMs: data?.rangeStartMs ?? null,
    rangeEndMs: data?.rangeEndMs ?? null,
    bucketDurationMs: HISTOGRAM_BUCKET_DURATION_MS,
    isLoading,
    isError,
  };
}
