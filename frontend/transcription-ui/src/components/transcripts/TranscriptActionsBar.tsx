import React from 'react';

import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import FormControlLabel from '@mui/material/FormControlLabel';
import Switch from '@mui/material/Switch';

export interface TranscriptActionsBarProps {
  hasNewerTranscripts: boolean;
  redactTranscripts: boolean;
  setRedactTranscripts: (redact: boolean) => void;
  onClickViewLatest: () => void;
}

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  hasNewerTranscripts,
  redactTranscripts,
  setRedactTranscripts,
  onClickViewLatest,
}) => {
  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'space-between',
        mb: 0.5,
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Button
          variant="contained"
          sx={{ textTransform: 'none', gap: 1 }}
          onClick={onClickViewLatest}
          disabled={!hasNewerTranscripts}
        >
          {hasNewerTranscripts ? 'Jump to latest' : 'Viewing latest'}
        </Button>
      </Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <FormControlLabel
          control={
            <Switch
              checked={redactTranscripts}
              onChange={(e) => setRedactTranscripts(e.target.checked)}
              size="small"
            />
          }
          label="Redact transcripts"
          slotProps={{ typography: { variant: 'body2' } }}
        />
      </Box>
    </Box>
  );
};

export default TranscriptActionsBar;
