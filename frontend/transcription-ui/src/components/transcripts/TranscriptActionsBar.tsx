import React from 'react';

import Box from '@mui/material/Box';
import FormControlLabel from '@mui/material/FormControlLabel';
import Switch from '@mui/material/Switch';

export interface TranscriptActionsBarProps {
  redactTranscripts: boolean;
  setRedactTranscripts: (redact: boolean) => void;
}

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  redactTranscripts,
  setRedactTranscripts,
}) => {
  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'space-between',
        mb: 0.5,
      }}
    >
      <Box />
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
