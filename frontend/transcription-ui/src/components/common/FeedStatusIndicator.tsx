import Badge, { type BadgeProps } from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type { BackendFeedStatus, FeedStatus } from '@transcription/common';

import { getRelativeTimeString } from '../../utils/timeUtils';

const FEED_SUBSTATUS_UI_TEXT_DISPLAY: Record<BackendFeedStatus, string> = {
  unclaimed: 'Unclaimed',
  active: 'Active',
  failing: 'Failing',
  quarantined: 'Quarantined',
  deactivated: 'Deactivated',
};

const FEED_STATUS_UI_CONFIG: Record<
  FeedStatus,
  { displayText: string; color: BadgeProps['color'] }
> = {
  active: { displayText: 'Active', color: 'success' },
  inactive: { displayText: 'Inactive', color: 'default' },
  error: { displayText: 'Error', color: 'error' },
};

export function FeedStatusIndicator({
  status,
  substatus,
  lastHeartbeat,
}: {
  status: FeedStatus | undefined;
  substatus: BackendFeedStatus | undefined;
  lastHeartbeat?: string;
}) {
  if (!status) {
    return null;
  }

  const statusConfig = status ? FEED_STATUS_UI_CONFIG[status] ?? status : undefined;
  const substatusText = substatus ? FEED_SUBSTATUS_UI_TEXT_DISPLAY[substatus] ?? substatus : undefined;
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
      <Tooltip title={substatusText}>
        <Box sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 1,
        }}>
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
          />
          <Typography
            variant="body2"
            sx={{
              color:
                statusConfig?.color === 'default'
                  ? 'text.secondary'
                  : `${statusConfig?.color ?? 'error'}.main`,
              fontWeight: 600,
              textTransform: 'uppercase',
              flexShrink: 0,
            }}
          >
            {statusConfig?.displayText ?? status}
          </Typography>
        </Box>
      </Tooltip>
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
