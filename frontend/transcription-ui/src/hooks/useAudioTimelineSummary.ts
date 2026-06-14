import { useQuery } from '@tanstack/react-query';
import type { AudioSegment } from '@transcription/common';

import { listAudioSegments } from '../service/listAudioSegments';
import { TIMELINE_RANGE_DURATION_MS } from '../utils/timeUtils';
import { type AlertFilter, isAlert } from './useAudioSegments';

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
  // When the list is date-filtered, anchor the overview here instead of live so
  // the mini-map and waveform stay in sync with the list (which shows this time).
  searchedTimestamp: Date | null;
}

// Loads the 24h of segments ending at the anchor (the filtered time, else the
// live edge) for the timeline overview in one request — independent of the
// list's lazy pagination, and kicked off first so the mini-map fills first.
export function useAudioTimelineSummary({
  token,
  searchedFeedId,
  alertFilter,
  isFeedsSuccess,
  searchedTimestamp,
}: UseAudioTimelineSummaryOptions) {
  const anchorMs = searchedTimestamp?.getTime() ?? null;
  const {
    data: summarySegments = [],
    isLoading,
    isError,
  } = useQuery<AudioSegment[], Error>({
    queryKey: [
      'audioTimelineSummary',
      token,
      searchedFeedId,
      alertFilter,
      anchorMs,
    ],
    queryFn: async () => {
      const endMs = anchorMs ?? Date.now();
      const startTime = endMs - TIMELINE_RANGE_DURATION_MS;
      const response = await listAudioSegments(
        searchedFeedId ?? '',
        token ?? '',
        SUMMARY_ROW_LIMIT,
        /*nextToken=*/ undefined,
        startTime,
        /*endTime=*/ anchorMs ?? undefined,
        /*order=*/ 'desc',
        isAlert(alertFilter)
      );
      return response.segments;
    },
    enabled: !!token && !!searchedFeedId && isFeedsSuccess,
    refetchOnWindowFocus: false,
    refetchInterval: SUMMARY_REFETCH_INTERVAL_MS,
    // Keep the prior overview on screen while only the time anchor changes (e.g.
    // clicking a past clip), so AudioDisplay doesn't blank during the refetch.
    // Cleared on feed/filter changes so another feed's clips never show.
    placeholderData: (prev, prevQuery) =>
      prevQuery?.queryKey[2] === searchedFeedId &&
      prevQuery?.queryKey[3] === alertFilter
        ? prev
        : undefined,
  });

  return { summarySegments, isLoading, isError };
}
