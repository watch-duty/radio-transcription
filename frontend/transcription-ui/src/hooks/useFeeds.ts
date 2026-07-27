import { useEffect, useMemo, useState } from 'react';

import { useInfiniteQuery } from '@tanstack/react-query';

import { listFeeds } from '../service/listFeeds';

export interface UseFeedsOptions {
  token: string | null;
  searchQuery?: string;
  sourceTypes?: string[];
  statuses?: string[];
  tags?: { key: string; value: string }[];
  limit?: number;
  debounceMs?: number;
  enabled?: boolean;
}

export function useFeeds({
  token,
  searchQuery = '',
  sourceTypes,
  statuses,
  tags,
  limit = 50,
  debounceMs = 300,
  enabled = true,
}: UseFeedsOptions) {
  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState(searchQuery);

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedSearchQuery(searchQuery);
    }, debounceMs);
    return () => clearTimeout(handler);
  }, [searchQuery, debounceMs]);

  const queryKey = useMemo(
    () => [
      'listFeeds',
      token,
      debouncedSearchQuery,
      sourceTypes,
      sourceTypes?.length,
      statuses,
      statuses?.length,
      tags,
      tags?.length,
      limit,
    ],
    [token, debouncedSearchQuery, sourceTypes, statuses, tags, limit]
  );

  const {
    data,
    error,
    isLoading,
    isFetching,
    isFetchingNextPage,
    hasNextPage,
    fetchNextPage,
    refetch,
  } = useInfiniteQuery({
    queryKey,
    queryFn: ({ pageParam }) =>
      listFeeds(token!, {
        limit,
        nextToken: pageParam || undefined,
        name: debouncedSearchQuery || undefined,
        sourceTypes:
          sourceTypes && sourceTypes.length > 0 ? sourceTypes : undefined,
        statuses: statuses && statuses.length > 0 ? statuses : undefined,
        tags: tags && tags.length > 0 ? tags : undefined,
      }),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage.nextToken || undefined,
    enabled: !!token && enabled,
    refetchOnWindowFocus: false,
  });

  const feeds = useMemo(
    () => data?.pages.flatMap((page) => page.feeds) ?? [],
    [data]
  );

  const total = useMemo(
    () => data?.pages[0]?.total ?? feeds.length,
    [data, feeds.length]
  );

  return {
    data,
    feeds,
    total,
    error,
    isLoading,
    isFetching,
    isFetchingNextPage,
    hasNextPage,
    fetchNextPage,
    refetch,
  };
}
