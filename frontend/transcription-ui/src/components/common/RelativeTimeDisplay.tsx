import Typography from '@mui/material/Typography';

import { getRelativeTimeString } from '../../utils/timeUtils';

export interface RelativeTimeDisplayProps {
  label: string;
  timestamp?: number;
}

export function RelativeTimeDisplay({
  label,
  timestamp,
}: RelativeTimeDisplayProps) {
  return (
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
      {label}: {timestamp ? getRelativeTimeString(timestamp) : 'N/A'}
    </Typography>
  );
}

export default RelativeTimeDisplay;
