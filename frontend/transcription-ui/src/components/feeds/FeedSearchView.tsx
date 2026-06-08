import { useEffect, useState } from 'react';

import { Box } from '@mui/material';
import { useQuery } from '@tanstack/react-query';

import { useAuth } from '../../context/AuthContext';
import { useListFeeds } from '../../hooks/useListFeeds';
import { listFeeds } from '../../service/listFeeds';
import { type FeedFilters, FeedTable } from './FeedTable';

interface FeedSearchViewProps {
  title: string;
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const FEED_REFETCH_INTERVAL_MS = 15000; // 15 seconds

export function FeedSearchView({ title, onError }: FeedSearchViewProps) {
  const { token } = useAuth();

  const [filters, setFilters] = useState<FeedFilters>({
    searchQuery: '',
    sourceTypes: [],
    statuses: [],
    tags: [],
  });

  const {
    feeds,
    error: feedsError,
    isLoading: feedsLoading,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useListFeeds({
    token,
    filters,
    refetchInterval: FEED_REFETCH_INTERVAL_MS,
  });

  const { data: allFeeds = [] } = useQuery({
    queryKey: ['listFeeds', token, 'all', '', [], 0, [], 0, [], 0],
    queryFn: () => listFeeds(token!, {}),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (feedsError) {
      onError(feedsError, 'Loading Feeds');
    }
  }, [feedsError, onError]);

  return (
    <Box
      sx={{
        width: '100%',
        textAlign: 'left',
        display: 'flex',
        flexDirection: 'column',
        height: 'calc(100vh - 100px)',
      }}
    >
      <FeedTable
        title={title}
        feeds={feeds}
        allFeeds={allFeeds}
        isLoading={feedsLoading}
        filters={filters}
        onFiltersChange={setFilters}
        hasNextPage={hasNextPage}
        onLoadMore={fetchNextPage}
        isFetchingNextPage={isFetchingNextPage}
      />
    </Box>
  );
}

export default FeedSearchView;
