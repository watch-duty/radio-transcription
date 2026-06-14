import { useQuery } from '@tanstack/react-query';
import type { AudioSegment } from '@transcription/common';

import { listAudioSegments } from '../service/listAudioSegments';
import { TIMELINE_RANGE_DURATION_MS } from '../utils/timeUtils';
import type { AlertFilter } from './useAudioSegments';

// Pick this up via a long interval so the overview backfills with newly archived
// segments; live-edge freshness is handled by unioning with the polling list.
const SUMMARY_REFETCH_INTERVAL_MS = 5 * 60 * 1000;
// High enough to return every segment in a 24h window in one response; the time
// window, not this count, is the real bound.
const SUMMARY_ROW_LIMIT = 100000;

interface UseAudioTimelineSummaryOptions {
  token: string | null;
  searchedFeedId: string | null;
  alertFilter: AlertFilter;
  isFeedsSuccess: boolean;
}

// Loads the last 24h of segments for the timeline overview in a single
// window-scoped request (start = now − 24h, no end), independent of the
// transcript list's lazy pagination and kicked off first so the mini-map can
// fill before the clips do.
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
      const startTime = Date.now() - TIMELINE_RANGE_DURATION_MS;
      const response = await listAudioSegments(
        searchedFeedId ?? '',
        token ?? '',
        SUMMARY_ROW_LIMIT,
        /*nextToken=*/ undefined,
        startTime,
        /*endTime=*/ undefined,
        /*order=*/ 'desc',
        alertFilter === 'alerts' ? true : undefined
      );
      return response.segments;
    },
    enabled: !!token && !!searchedFeedId && isFeedsSuccess,
    refetchOnWindowFocus: false,
    refetchInterval: SUMMARY_REFETCH_INTERVAL_MS,
  });

  return { summarySegments, isLoading, isError };
}
