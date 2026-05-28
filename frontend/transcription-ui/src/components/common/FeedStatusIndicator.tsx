import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import type { FeedStatus } from '@transcription/common';

import { getRelativeTimeString } from '../../utils/timeUtils';

const FEED_STATUS_UI_CONFIG: Record<
  FeedStatus,
  { displayText: string; color: 'success' | 'error' }
> = {
  active: { displayText: 'Active', color: 'success' },
  inactive: { displayText: 'Inactive', color: 'error' },
};

export function FeedStatusIndicator({
  status,
  lastHeartbeat,
}: {
  status: FeedStatus | undefined;
  lastHeartbeat?: string;
}) {
  if (!status) {
    return null;
  }

  const statusConfig = status ? FEED_STATUS_UI_CONFIG[status] : undefined;
  return (
    <Box
      sx={{
        display: 'flex',
        alignItems: 'center',
        gap: 1,
        minWidth: 0,
        overflow: 'hidden',
        width: '100%',
      }}
    >
      <Badge
        color={statusConfig?.color ?? 'error'}
        variant="dot"
        sx={{
          py: 0,
          px: 0.5,
          display: 'flex',
          alignItems: 'center',
          flexShrink: 0,
        }}
      ></Badge>
      <Typography
        variant="body2"
        sx={{
          color: `${statusConfig?.color ?? 'error'}.main`,
          fontWeight: 600,
          textTransform: 'uppercase',
          flexShrink: 0,
        }}
      >
        {statusConfig?.displayText ?? status}
      </Typography>
      {lastHeartbeat && (
        <Typography
          variant="caption"
          color="text.secondary"
          sx={{
            whiteSpace: 'nowrap',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            minWidth: 0,
          }}
        >
          Last updated: {getRelativeTimeString(lastHeartbeat)}
        </Typography>
      )}
    </Box>
  );
}

export default FeedStatusIndicator;
