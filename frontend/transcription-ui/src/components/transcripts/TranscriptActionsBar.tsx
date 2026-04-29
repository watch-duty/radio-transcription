import React from 'react';

import InventoryIcon from '@mui/icons-material/Inventory';
import LinkIcon from '@mui/icons-material/Link';
import RefreshIcon from '@mui/icons-material/Refresh';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import CircularProgress from '@mui/material/CircularProgress';
import Link from '@mui/material/Link';

export interface TranscriptActionsBarProps {
  sourceUrl?: string;
  archiveUrl?: string;
  hasNewerTranscripts: boolean;
  isTranscriptsFetching: boolean;
  isTranscriptsPolling: boolean;
  pollingIntervalDisplay: string;
  onRefresh: () => Promise<void>;
}

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  sourceUrl,
  archiveUrl,
  hasNewerTranscripts,
  isTranscriptsFetching,
  isTranscriptsPolling,
  pollingIntervalDisplay,
  onRefresh,
}) => {
  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'space-between',
        mb: 1,
      }}
    >
      {sourceUrl || archiveUrl ? (
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
