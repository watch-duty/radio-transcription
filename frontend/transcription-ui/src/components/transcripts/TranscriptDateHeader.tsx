import type React from 'react';

import Box from '@mui/material/Box';
import ListItem from '@mui/material/ListItem';
import Typography from '@mui/material/Typography';

export interface TranscriptDateHeaderProps {
  title: string;
  children?: React.ReactNode;
}

export function TranscriptDateHeader({
  title,
  children,
}: TranscriptDateHeaderProps) {
  return (
    <ListItem
      sx={{
        position: 'sticky',
        top: 0,
        zIndex: 1,
        py: 0,
        px: 0,
        bgcolor: 'background.paper',
      }}
    >
      <Box
        sx={{
          width: '100%',
          py: 0.5,
          px: 2,
          bgcolor: 'action.hover',
          display: 'flex',
          alignItems: 'center',
          gap: 1,
        }}
      >
        <Typography
          variant="caption"
          color="text.secondary"
          sx={{ fontWeight: 'bold' }}
        >
          {title}
        </Typography>
        {children}
      </Box>
    </ListItem>
  );
}

export default TranscriptDateHeader;
