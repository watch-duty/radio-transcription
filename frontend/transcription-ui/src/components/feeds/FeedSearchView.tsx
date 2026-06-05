import { useEffect, useState } from 'react';

import { Box } from '@mui/material';
import { useQuery } from '@tanstack/react-query';
import type { Tag } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { listFeeds } from '../../service/listFeeds';
import { FeedTable } from './FeedTable';

interface FeedSearchViewProps {
  title: string;
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const FEED_REFETCH_INTERVAL_MS = 15000; // 15 seconds

export function FeedSearchView({ title, onError }: FeedSearchViewProps) {
  const { token } = useAuth();

  const [filters, setFilters] = useState<{
    searchQuery: string;
    sourceTypes: string[];
    statuses: string[];
    tags: Tag[];
  }>({
    searchQuery: '',
    sourceTypes: [],
    statuses: [],
    tags: [],
  });

  const {
    data: feeds,
    error: feedsError,
    isLoading: feedsLoading,
  } = useQuery({
    queryKey: ['listFeeds', token, filters],
    queryFn: () =>
      listFeeds(token!, {
        name: filters.searchQuery || undefined,
        sourceTypes:
          filters.sourceTypes.length > 0 ? filters.sourceTypes : undefined,
        statuses: filters.statuses.length > 0 ? filters.statuses : undefined,
        tags: filters.tags.length > 0 ? filters.tags : undefined,
      }),
    enabled: !!token,
    refetchOnWindowFocus: false,
    refetchInterval: FEED_REFETCH_INTERVAL_MS,
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
        feeds={feeds ?? []}
        isLoading={feedsLoading}
        onFiltersChange={setFilters}
      />
    </Box>
  );
}

export default FeedSearchView;
