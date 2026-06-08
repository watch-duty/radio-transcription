import { useEffect, useMemo, useState } from 'react';

import { useInfiniteQuery } from '@tanstack/react-query';

import type { FeedFilters } from '../components/feeds/FeedTable';
import { listFeedsPage } from '../service/listFeeds';

const QUERY_DEBOUNCE_TIME_MS = 300;

interface UseListFeedsOptions {
  token: string | null;
  filters: FeedFilters;
  refetchInterval?: number;
  enabled?: boolean;
}

export function useListFeeds({
  token,
  filters,
  refetchInterval,
  enabled = true,
}: UseListFeedsOptions) {
  const [debouncedSearchQuery, setDebouncedSearchQuery] = useState(
    filters.searchQuery
  );

  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedSearchQuery(filters.searchQuery);
    }, QUERY_DEBOUNCE_TIME_MS);
    return () => clearTimeout(handler);
  }, [filters.searchQuery]);

  const queryResult = useInfiniteQuery({
    queryKey: [
      'listFeeds',
      token,
      'infinite',
      debouncedSearchQuery,
      filters.sourceTypes,
      filters.sourceTypes.length,
      filters.statuses,
      filters.statuses.length,
      filters.tags,
      filters.tags.length,
    ],
    queryFn: ({ pageParam }) =>
      listFeedsPage(token!, {
        name: debouncedSearchQuery || undefined,
        sourceTypes:
          filters.sourceTypes.length > 0 ? filters.sourceTypes : undefined,
        statuses: filters.statuses.length > 0 ? filters.statuses : undefined,
        tags: filters.tags.length > 0 ? filters.tags : undefined,
        limit: 50,
        nextToken: pageParam,
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.nextToken,
    enabled: !!token && enabled,
    refetchOnWindowFocus: false,
    refetchInterval,
  });

  const feeds = useMemo(
    () => queryResult.data?.pages.flatMap((page) => page.feeds) ?? [],
    [queryResult.data]
  );

  return {
    ...queryResult,
    feeds,
  };
}
