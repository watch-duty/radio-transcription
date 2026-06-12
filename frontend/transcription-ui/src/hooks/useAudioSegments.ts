import { useCallback, useMemo } from 'react';

import {
  type InfiniteData,
  useInfiniteQuery,
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

const MAX_TRANSCRIPTS_POLLING_ITERATIONS = 10;

interface UseAudioSegmentsOptions {
  token: string | null;
  searchedFeedId: string | null;
  searchedTimestamp: Date | null;
  alertFilter: AlertFilter;
  isFeedsSuccess: boolean;
}

export function useAudioSegments({
  token,
  searchedFeedId,
  searchedTimestamp,
  alertFilter,
  isFeedsSuccess,
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
    data: listTranscriptsResponse,
    fetchNextPage: loadOlderTranscripts,
    fetchPreviousPage: loadNewerTranscripts,
    hasNextPage: hasOlderTranscripts,
    hasPreviousPage: hasNewerTranscripts,
    isSuccess: isTranscriptsSuccess,
    isError: isTranscriptsError,
    error: transcriptsError,
    dataUpdatedAt: transcriptsDataUpdatedAt,
    isFetchingPreviousPage: isFetchingNewerTranscripts,
    isFetchingNextPage: isFetchingOlderTranscripts,
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
      listTranscriptsResponse?.pages.flatMap((page) => page.segments) ?? [];
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
  }, [listTranscriptsResponse]);

  const newestTimestamp = rawAudioSegments[0]?.startTimestamp;

  const pollNewerTranscripts = useCallback(async (): Promise<
    AudioSegment[]
  > => {
    if (!searchedFeedId || !token) return [];

    const allNewTranscripts: AudioSegment[] = [];

    if (newestTimestamp) {
      let currentNextToken: string | undefined = undefined;
      let hasMore = true;
      let iterations = 0;

      try {
        while (hasMore) {
          if (iterations > MAX_TRANSCRIPTS_POLLING_ITERATIONS) {
            console.warn(
              'pollNewerTranscripts has more than 10 pages of new transcripts. This is unexpected. If this message continues, please report a bug.'
            );
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
            allNewTranscripts.push(...response.segments);
          }

          currentNextToken = response.nextToken;
          hasMore = !!currentNextToken;
        }
      } catch (error) {
        console.error('Error polling for new transcripts:', error);
      }

      return allNewTranscripts.reverse();
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
          'Error polling for new transcripts (no initial transcripts):',
          error
        );
        return [];
      }
    }
  }, [newestTimestamp, searchedFeedId, token, alertFilter]);

  const updateCacheWithNewTranscripts = useCallback(
    (newTranscripts: AudioSegment[]): AudioSegment[] => {
      if (!token) return [];
      let updatedTranscripts: AudioSegment[] = [];

      queryClient.setQueryData<InfiniteData<ListAudioSegmentsData>>(
        queryKey,
        (oldData) => {
          if (!oldData) return oldData;

          const newTranscriptsMap = new Map(
            newTranscripts.map((t) => [t.id, t])
          );

          const updatedPages = oldData.pages.map((page) => {
            let pageSegmentsChanged = false;
            const updatedSegments = page.segments.map((existingSegment) => {
              const newSegment = newTranscriptsMap.get(existingSegment.id);
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
          const brandNewSegments = newTranscripts.filter(
            (t) => !existingIds.has(t.id)
          );

          if (brandNewSegments.length === 0) {
            const pagesChanged = updatedPages.some(
              (p, idx) => p !== oldData.pages[idx]
            );
            if (!pagesChanged) return oldData;
            return { ...oldData, pages: updatedPages };
          }

          updatedTranscripts = brandNewSegments;

          const finalPages = [...updatedPages];
          finalPages[0] = {
            ...finalPages[0],
            segments: [...brandNewSegments, ...finalPages[0].segments],
          };

          return { ...oldData, pages: finalPages };
        }
      );
      return updatedTranscripts;
    },
    [token, queryKey, queryClient]
  );

  return {
    rawAudioSegments,
    newestTimestamp,
    loadOlderTranscripts,
    loadNewerTranscripts,
    hasOlderTranscripts,
    hasNewerTranscripts,
    isTranscriptsSuccess,
    isTranscriptsError,
    transcriptsError,
    transcriptsDataUpdatedAt,
    isFetchingNewerTranscripts,
    isFetchingOlderTranscripts,
    pollNewerTranscripts,
    updateCacheWithNewTranscripts,
    isLoading,
    isFetching,
  };
}
