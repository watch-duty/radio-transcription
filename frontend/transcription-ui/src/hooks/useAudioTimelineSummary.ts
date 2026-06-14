import { useQuery } from '@tanstack/react-query';
import type { AudioSegment } from '@transcription/common';

import { listAudioSegments } from '../service/listAudioSegments';
import { TIMELINE_RANGE_DURATION_MS } from '../utils/timeUtils';
import type { AlertFilter } from './useAudioSegments';

// Pulled in bulk for the timeline overview; the JSON is light next to audio, so
// a few large pages cover 24h cheaply.
const HEATMAP_PAGE_SIZE = 2000;
// Backstop against an unexpectedly dense feed looping forever.
const MAX_HEATMAP_PAGES = 30;
// Pick this up via a long interval so the overview backfills with newly archived
// segments; live-edge freshness is handled by unioning with the polling list.
const SUMMARY_REFETCH_INTERVAL_MS = 5 * 60 * 1000;

interface UseAudioTimelineSummaryOptions {
  token: string | null;
  searchedFeedId: string | null;
  alertFilter: AlertFilter;
  isFeedsSuccess: boolean;
}

// Loads the most recent 24h of segments for the timeline overview, independent
// of the transcript list's lazy pagination. Always anchored to the live edge.
export function useAudioTimelineSummary({
  token,
  searchedFeedId,
  alertFilter,
  isFeedsSuccess,
}: UseAudioTimelineSummaryOptions) {
  const {
    data: summarySegments = [],
    isLoading,
    isError,
  } = useQuery<AudioSegment[], Error>({
    queryKey: ['audioTimelineSummary', token, searchedFeedId, alertFilter],
    queryFn: async () => {
      const segments: AudioSegment[] = [];
      let nextToken: string | undefined = undefined;
      let liveEdge: number | null = null;
      // Temporary instrumentation for tuning HEATMAP_PAGE_SIZE; remove once tuned.
      const loadStart = performance.now();

      for (let page = 0; page < MAX_HEATMAP_PAGES; page++) {
        const pageStart = performance.now();
        const response = await listAudioSegments(
          searchedFeedId ?? '',
          token ?? '',
          HEATMAP_PAGE_SIZE,
          nextToken,
          /*startTime=*/ undefined,
          /*endTime=*/ undefined,
          /*order=*/ 'desc',
          alertFilter === 'alerts' ? true : undefined
        );
        const pageMs = Math.round(performance.now() - pageStart);

        if (response.segments.length === 0) break;
        segments.push(...response.segments);

        // First (newest) page establishes the live edge we measure 24h back from.
        liveEdge ??= new Date(response.segments[0].endTimestamp).getTime();
        const oldestStart = Math.min(
          ...response.segments.map((s) => new Date(s.startTimestamp).getTime())
        );
        const hoursCovered = (liveEdge - oldestStart) / (60 * 60 * 1000);
        console.info(
          `[heatmap] page ${page}: ${response.segments.length} segments in ${pageMs}ms ` +
            `(${segments.length} total, ${hoursCovered.toFixed(1)}h covered)`
        );

        nextToken = response.nextToken;
        if (
          !nextToken ||
          oldestStart <= liveEdge - TIMELINE_RANGE_DURATION_MS
        ) {
          break;
        }

        if (page === MAX_HEATMAP_PAGES - 1) {
          console.warn(
            `useAudioTimelineSummary hit the ${MAX_HEATMAP_PAGES}-page cap before covering 24h; overview may be truncated.`
          );
        }
      }

      console.info(
        `[heatmap] done: ${segments.length} segments in ${Math.round(performance.now() - loadStart)}ms ` +
          `@ ${HEATMAP_PAGE_SIZE}/page`
      );
      return segments;
    },
    enabled: !!token && !!searchedFeedId && isFeedsSuccess,
    refetchOnWindowFocus: false,
    refetchInterval: SUMMARY_REFETCH_INTERVAL_MS,
  });

  return { summarySegments, isLoading, isError };
}
