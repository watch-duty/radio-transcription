import React from 'react';

import InventoryIcon from '@mui/icons-material/Inventory';
import LinkIcon from '@mui/icons-material/Link';
import RefreshIcon from '@mui/icons-material/Refresh';
import Alert from '@mui/material/Alert';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import CircularProgress from '@mui/material/CircularProgress';
import Link from '@mui/material/Link';
import Typography from '@mui/material/Typography';
import { type FeedStatus } from '@transcription/common';

import { getRelativeTimeString } from '../../utils/timeUtils';

export interface TranscriptActionsBarProps {
  sourceUrl?: string;
  archiveUrl?: string;
  status?: FeedStatus;
  lastHeartbeat?: string;
  hasNewerTranscripts: boolean;
  isTranscriptsFetching: boolean;
  isTranscriptsPolling: boolean;
  pollingIntervalDisplay: string;
  onRefresh: () => Promise<void>;
}

const FEED_STATUS_UI_CONFIG: Record<
  FeedStatus,
  { displayText: string; severity: 'success' | 'warning' | 'error' | 'info' }
> = {
  active: { displayText: 'Active', severity: 'success' },
  failing: { displayText: 'Failing', severity: 'warning' },
  quarantined: { displayText: 'Inactive', severity: 'error' },
  unclaimed: { displayText: 'Pending', severity: 'info' },
  deactivated: { displayText: 'Inactive', severity: 'info' },
};

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  sourceUrl,
  archiveUrl,
  status,
  lastHeartbeat,
  hasNewerTranscripts,
  isTranscriptsFetching,
  isTranscriptsPolling,
  pollingIntervalDisplay,
  onRefresh,
}) => {
  const statusConfig = status ? FEED_STATUS_UI_CONFIG[status] : undefined;

  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'space-between',
        mb: 1,
      }}
    >
      {sourceUrl || archiveUrl || status ? (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          {sourceUrl && (
            <Link
              href={sourceUrl}
              target="_blank"
              rel="noopener noreferrer"
              variant="body2"
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 0.5,
              }}
            >
              <LinkIcon fontSize="small" />
              Original source link
            </Link>
          )}
          {archiveUrl && (
            <Link
              href={archiveUrl}
              target="_blank"
              rel="noopener noreferrer"
              variant="body2"
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 0.5,
              }}
            >
              <InventoryIcon fontSize="small" />
              Archives
            </Link>
          )}
          {status && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Alert
                severity={statusConfig?.severity ?? 'info'}
                variant="outlined"
                sx={{
                  py: 0,
                  px: 1,
                  display: 'flex',
                  alignItems: 'center',
                }}
              >
                {statusConfig?.displayText ?? status}
              </Alert>
              {lastHeartbeat && (
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{ whiteSpace: 'nowrap' }}
                >
                  Last updated: {getRelativeTimeString(lastHeartbeat)}
                </Typography>
              )}
            </Box>
          )}
        </Box>
      ) : (
        <Box />
      )}
      {!hasNewerTranscripts && (
        <Button
          size="small"
          variant="text"
          onClick={onRefresh}
          disabled={isTranscriptsFetching || isTranscriptsPolling}
          startIcon={
            isTranscriptsPolling ? (
              <CircularProgress size={16} color="inherit" />
            ) : (
              <RefreshIcon />
            )
          }
          sx={{ textTransform: 'none' }}
        >
          {isTranscriptsPolling
            ? 'Refreshing...'
            : `Refresh (${pollingIntervalDisplay})`}
        </Button>
      )}
    </Box>
  );
};

export default TranscriptActionsBar;
