import { useEffect } from 'react';

import { Box, Typography } from '@mui/material';
import { useQuery } from '@tanstack/react-query';

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

  const {
    data: feeds,
    error: feedsError,
    isLoading: feedsLoading,
  } = useQuery({
    queryKey: ['listFeeds', token],
    queryFn: () => listFeeds(token!),
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
        gap: 2,
      }}
    >
      <Typography variant="h5">{title}</Typography>
      <FeedTable feeds={feeds ?? []} isLoading={feedsLoading} />
    </Box>
  );
}

export default FeedSearchView;
