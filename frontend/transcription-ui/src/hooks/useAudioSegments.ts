import { useCallback, useEffect, useMemo, useRef } from 'react';

import {
  type InfiniteData,
  useInfiniteQuery,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';
import type { AudioSegment } from '@transcription/common';

import { listAudioSegments } from '../service/listAudioSegments';

export type ListAudioSegmentsPage = {
  nextToken?: string;
  order: 'asc' | 'desc';
};

export type ListAudioSegmentsData = {
  segments: AudioSegment[];
} & ListAudioSegmentsPage;

export type AlertFilter = 'all' | 'alerts';

const MAX_AUDIO_SEGMENTS_POLLING_ITERATIONS = 10;

// How often the live "newer segments" poll runs while at the head of the stream.
const POLL_INTERVAL_MS = 10000;

interface UseAudioSegmentsOptions {
  token: string | null;
  searchedFeedId: string | null;
  searchedTimestamp: Date | null;
  alertFilter: AlertFilter;
  isFeedsSuccess: boolean;
  // Whether the view wants live polling (it gates this on the list being
  // scrolled to the top). Polling is additionally suppressed unless the head of
  // the stream is loaded.
  pollingEnabled: boolean;
  // Called once per poll with the brand-new segments that were merged into the
  // cache, so the view can run UI side effects (snackbar, autoplay, unread).
  onNewSegments?: (segments: AudioSegment[]) => void;
}

export function useAudioSegments({
  token,
  searchedFeedId,
  searchedTimestamp,
  alertFilter,
  isFeedsSuccess,
  pollingEnabled,
  onNewSegments,
}: UseAudioSegmentsOptions) {
  const queryClient = useQueryClient();

  const queryKey = useMemo(
    () => [
      'listAudioSegments',
      token,
      searchedFeedId,
      searchedTimestamp,
      alertFilter,
    ],
    [token, searchedFeedId, searchedTimestamp, alertFilter]
  );

  const {
    data: listAudioSegmentsResponse,
    fetchNextPage: loadOlderAudioSegments,
    fetchPreviousPage: loadNewerAudioSegments,
    hasNextPage: hasOlderAudioSegments,
    hasPreviousPage: hasNewerAudioSegments,
    isSuccess: isAudioSegmentsSuccess,
    isError: isAudioSegmentsError,
    error: audioSegmentsError,
    dataUpdatedAt: audioSegmentsDataUpdatedAt,
    isFetchingPreviousPage: isFetchingNewerAudioSegments,
    isFetchingNextPage: isFetchingOlderAudioSegments,
    isLoading,
    isFetching,
  } = useInfiniteQuery<ListAudioSegmentsData, Error>({
    queryKey,
    queryFn: async ({ pageParam }) => {
      const pageParamTyped = pageParam as ListAudioSegmentsPage | undefined;
      const order =
        pageParamTyped?.order ?? (searchedTimestamp ? 'asc' : 'desc');
      const limit = undefined;

      const originalTimestampMs =
        !pageParamTyped?.nextToken && searchedTimestamp
          ? searchedTimestamp.getTime()
          : undefined;

      const response = await listAudioSegments(
        searchedFeedId ?? '',
        token ?? '',
        limit,
        pageParamTyped?.nextToken,
        /*startTime=*/ order === 'asc' ? originalTimestampMs : undefined,
        /*endTime=*/ order === 'desc' ? originalTimestampMs : undefined,
        order,
        alertFilter === 'alerts' ? true : undefined
      );

      if (order === 'asc' && response.segments) {
        response.segments.reverse();
      }

      return { ...response, order };
    },
    initialPageParam: {
      order: searchedTimestamp ? 'asc' : 'desc',
    },
    getNextPageParam: (lastPage) => {
      if (lastPage.order === 'desc') {
        return lastPage.nextToken
          ? { order: 'desc', nextToken: lastPage.nextToken }
          : undefined;
      }
      return searchedTimestamp
        ? { order: 'desc', nextToken: undefined }
        : undefined;
    },
    getPreviousPageParam: (firstPage) => {
      if (!searchedTimestamp) return undefined;
      if (firstPage.order === 'asc') {
        return firstPage.nextToken
          ? { order: 'asc', nextToken: firstPage.nextToken }
          : undefined;
      }
      return undefined;
    },
    enabled: !!token && !!searchedFeedId && isFeedsSuccess,
    refetchOnWindowFocus: false,
  });

  const rawAudioSegments = useMemo(() => {
    const allSegments =
      listAudioSegmentsResponse?.pages.flatMap((page) => page.segments) ?? [];
    const seenIds = new Set<string>();
    const uniqueSegments = allSegments.filter((segment) => {
      if (seenIds.has(segment.id)) {
        return false;
      }
      seenIds.add(segment.id);
      return true;
    });
    return uniqueSegments.sort(
      (a, b) =>
        new Date(b.startTimestamp).getTime() -
        new Date(a.startTimestamp).getTime()
    );
  }, [listAudioSegmentsResponse]);

  const newestTimestamp = rawAudioSegments[0]?.startTimestamp;

  const pollNewerAudioSegments = useCallback(async (): Promise<
    AudioSegment[]
  > => {
    if (!searchedFeedId || !token) return [];

    const allNewAudioSegments: AudioSegment[] = [];

    if (newestTimestamp) {
      let currentNextToken: string | undefined = undefined;
      let iterations = 0;

      try {
        do {
          if (iterations > MAX_AUDIO_SEGMENTS_POLLING_ITERATIONS) {
            console.warn(
              'pollNewerAudioSegments has more than 10 pages of new audio segments. This is unexpected. If this message continues, please report a bug.'
            );
            break;
          }
          iterations++;

          const response = await listAudioSegments(
            searchedFeedId,
            token,
            /*limit=*/ undefined,
            currentNextToken,
            /*startTime=*/ new Date(newestTimestamp).getTime(),
            /*endTime=*/ undefined,
            /*order=*/ 'asc',
            alertFilter === 'alerts' ? true : undefined
          );

          if (response.segments && response.segments.length > 0) {
            allNewAudioSegments.push(...response.segments);
          }

          currentNextToken = response.nextToken;
        } while (currentNextToken);
      } catch (error) {
        console.error('Error polling for new audio segments:', error);
      }

      return allNewAudioSegments.reverse();
    } else {
      try {
        const response = await listAudioSegments(
          searchedFeedId,
          token,
          /*limit=*/ undefined,
          /*nextToken=*/ undefined,
          /*startTime=*/ undefined,
          /*endTime=*/ undefined,
          /*order=*/ 'desc',
          alertFilter === 'alerts' ? true : undefined
        );
        return response.segments || [];
      } catch (error) {
        console.error(
          'Error polling for new audio segments (no initial audio segments):',
          error
        );
        return [];
      }
    }
  }, [newestTimestamp, searchedFeedId, token, alertFilter]);

  const updateCacheWithNewAudioSegments = useCallback(
    (newAudioSegments: AudioSegment[]): AudioSegment[] => {
      if (!token) return [];
      let updatedAudioSegments: AudioSegment[] = [];

      queryClient.setQueryData<InfiniteData<ListAudioSegmentsData>>(
        queryKey,
        (oldData) => {
          if (!oldData) return oldData;

          const newAudioSegmentsMap = new Map(
            newAudioSegments.map((t) => [t.id, t])
          );

          const updatedPages = oldData.pages.map((page) => {
            let pageSegmentsChanged = false;
            const updatedSegments = page.segments.map((existingSegment) => {
              const newSegment = newAudioSegmentsMap.get(existingSegment.id);
              if (newSegment) {
                pageSegmentsChanged = true;
                return newSegment;
              }
              return existingSegment;
            });

            return pageSegmentsChanged
              ? { ...page, segments: updatedSegments }
              : page;
          });

          const existingIds = new Set(
            oldData.pages.flatMap((p) => p.segments.map((t) => t.id))
          );
          const brandNewSegments = newAudioSegments.filter(
            (t) => !existingIds.has(t.id)
          );

          if (brandNewSegments.length === 0) {
            const pagesChanged = updatedPages.some(
              (p, idx) => p !== oldData.pages[idx]
            );
            if (!pagesChanged) return oldData;
            return { ...oldData, pages: updatedPages };
          }

          updatedAudioSegments = brandNewSegments;

          const finalPages = [...updatedPages];
          finalPages[0] = {
            ...finalPages[0],
            segments: [...brandNewSegments, ...finalPages[0].segments],
          };

          return { ...oldData, pages: finalPages };
        }
      );
      return updatedAudioSegments;
    },
    [token, queryKey, queryClient]
  );

  // Live polling for newer segments at the head of the stream. React Query owns
  // the timer to take advantage of its refetch-on-focus/reconnect/background behavior.
  const pollingQuery = useQuery({
    queryKey: ['liveAudioSegmentsPoll', searchedFeedId, alertFilter],
    queryFn: pollNewerAudioSegments,
    enabled:
      isAudioSegmentsSuccess &&
      pollingEnabled &&
      !hasNewerAudioSegments &&
      !!searchedFeedId,
    refetchInterval: POLL_INTERVAL_MS,
    staleTime: POLL_INTERVAL_MS,
    refetchOnWindowFocus: true,
    refetchOnReconnect: true,
    refetchIntervalInBackground: true,
  });

  const lastProcessedPollRef = useRef(0);

  // Merge each completed poll's segments into the cache exactly once, then hand
  // the brand-new ones to the view for side effects. Guarded by dataUpdatedAt so
  // unrelated re-renders don't re-run it.
  useEffect(() => {
    if (!pollingQuery.isSuccess) return;
    if (pollingQuery.dataUpdatedAt === lastProcessedPollRef.current) return;
    lastProcessedPollRef.current = pollingQuery.dataUpdatedAt;

    const polledSegments = pollingQuery.data;
    if (polledSegments.length === 0) return;

    const brandNewAudioSegments =
      updateCacheWithNewAudioSegments(polledSegments);
    if (brandNewAudioSegments.length > 0) {
      onNewSegments?.(brandNewAudioSegments);
    }
  }, [
    pollingQuery.isSuccess,
    pollingQuery.dataUpdatedAt,
    pollingQuery.data,
    updateCacheWithNewAudioSegments,
    onNewSegments,
  ]);

  // "Last refresh" reflects the most recent of the paged load and the latest
  // successful poll, so it advances on every poll even when nothing new arrives.
  const audioSegmentsLastUpdated = useMemo(() => {
    const lastRefreshedAt = Math.max(
      audioSegmentsDataUpdatedAt,
      pollingQuery.dataUpdatedAt
    );
    return lastRefreshedAt > 0 ? lastRefreshedAt : null;
  }, [audioSegmentsDataUpdatedAt, pollingQuery.dataUpdatedAt]);

  const { isFetching: isAudioSegmentsPolling } = pollingQuery;
  return {
    rawAudioSegments,
    loadOlderAudioSegments,
    loadNewerAudioSegments,
    hasOlderAudioSegments,
    hasNewerAudioSegments,
    isAudioSegmentsSuccess,
    isAudioSegmentsError,
    audioSegmentsError,
    isFetchingNewerAudioSegments,
    isFetchingOlderAudioSegments,
    isAudioSegmentsPolling,
    audioSegmentsLastUpdated,
    isLoading,
    isFetching,
  };
}
