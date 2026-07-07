import Chip from '@mui/material/Chip';
import Typography from '@mui/material/Typography';

import { type GroupedTag } from './tagDisplay';

export function FeedTagChip({ group }: { group: GroupedTag }) {
  return (
    <Chip
      label={
        <Typography variant="body2">
          <b>{group.key}</b>: {group.values.join(', ')}
        </Typography>
      }
      size="small"
      variant="filled"
    />
  );
}

export default FeedTagChip;
