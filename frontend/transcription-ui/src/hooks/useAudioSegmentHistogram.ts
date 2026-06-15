import { useQuery } from '@tanstack/react-query';
import type { HistogramBucket } from '@transcription/common';

import { getAudioSegmentHistogram } from '../service/getAudioSegmentHistogram';
import { TIMELINE_RANGE_DURATION_MS } from '../utils/timeUtils';
import { type AlertFilter, isAlert } from './useAudioSegments';

// Long interval; the live edge advances on re-render, not from this poll.
const HISTOGRAM_REFETCH_INTERVAL_MS = 5 * 60 * 1000;
// 24h / 288 = 5-minute buckets.
const HISTOGRAM_BUCKETS = 288;

export const HISTOGRAM_BUCKET_DURATION_MS =
  TIMELINE_RANGE_DURATION_MS / HISTOGRAM_BUCKETS;

interface UseAudioSegmentHistogramOptions {
  token: string | null;
  searchedFeedId: string | null;
  alertFilter: AlertFilter;
  isFeedsSuccess: boolean;
  // Overview anchor (date-filter time, else null=live); not the scrub anchor.
  anchorTimestamp: Date | null;
}

// One small bucketed request for the 24h overview (counts, not segment DTOs).
export function useAudioSegmentHistogram({
  token,
  searchedFeedId,
  alertFilter,
  isFeedsSuccess,
  anchorTimestamp,
}: UseAudioSegmentHistogramOptions) {
  const anchorMs = anchorTimestamp?.getTime() ?? null;
  const {
    data: buckets = [],
    isLoading,
    isError,
  } = useQuery<HistogramBucket[], Error>({
    queryKey: [
      'audioSegmentHistogram',
      token,
      searchedFeedId,
      alertFilter,
      anchorMs,
    ],
    queryFn: async () => {
      const endMs = anchorMs ?? Date.now();
      const startMs = endMs - TIMELINE_RANGE_DURATION_MS;
      const response = await getAudioSegmentHistogram(
        searchedFeedId ?? '',
        token ?? '',
        startMs,
        anchorMs ?? undefined,
        HISTOGRAM_BUCKETS,
        isAlert(alertFilter)
      );
      return response.buckets;
    },
    enabled: !!token && !!searchedFeedId && isFeedsSuccess,
    refetchOnWindowFocus: false,
    refetchInterval: HISTOGRAM_REFETCH_INTERVAL_MS,
    // Keep the prior strip during an anchor refetch; cleared on feed/filter change.
    placeholderData: (prev, prevQuery) =>
      prevQuery?.queryKey[2] === searchedFeedId &&
      prevQuery?.queryKey[3] === alertFilter
        ? prev
        : undefined,
  });

  return {
    buckets,
    bucketDurationMs: HISTOGRAM_BUCKET_DURATION_MS,
    isLoading,
    isError,
  };
}
