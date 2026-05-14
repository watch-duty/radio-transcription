import { useEffect } from 'react';

import { useQuery } from '@tanstack/react-query';

import { useAuth } from '../../context/AuthContext';
import { listFeeds } from '../../service/listFeeds';
import { Box, Typography } from '@mui/material';
import { FeedTable } from './FeedTable';

interface FeedSearchViewProps {
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

export function FeedSearchView({
  onError,
}: FeedSearchViewProps) {
  const { token } = useAuth();

  const {
    data: feeds,
    error: feedsError,
    isFetching: feedsFetching,
  } = useQuery({
    queryKey: ['listFeeds', token],
    queryFn: () => listFeeds(token!),
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
        gap: 2,
      }}
    >
      <Typography variant="h5" sx={{ fontWeight: 'bold' }}>
        Radio Feeds
      </Typography>
      <FeedTable feeds={feeds ?? []} isLoading={feedsFetching} />
    </Box>
  );
}

export default FeedSearchView;
