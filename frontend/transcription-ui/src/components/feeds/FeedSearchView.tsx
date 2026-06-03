import { Box } from '@mui/material';

import { FeedTable } from './FeedTable';

interface FeedSearchViewProps {
  title: string;
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const FEED_REFETCH_INTERVAL_MS = 15000; // 15 seconds

export function FeedSearchView({ title, onError }: FeedSearchViewProps) {
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
        onError={onError}
        refetchInterval={FEED_REFETCH_INTERVAL_MS}
      />
    </Box>
  );
}

export default FeedSearchView;
