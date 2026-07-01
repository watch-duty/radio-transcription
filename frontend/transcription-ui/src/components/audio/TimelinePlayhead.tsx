import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';

import { type PlaybackState } from '../../utils/playbackUtils';

interface TimelinePlayheadProps {
  state: PlaybackState;
  // Horizontal position within the timeline, as a percentage (0–100).
  leftOffsetPct: number;
  label: string;
}

function TimelinePlayhead({
  state,
  leftOffsetPct,
  label,
}: TimelinePlayheadProps) {
  const theme = useTheme();
  const color = {
    playing: theme.palette.error.main,
    paused: theme.palette.warning.main,
    listening: theme.palette.success.main,
  }[state];

  // Keep the lozenge from spilling past the timeline edges.
  const lozengeTransform =
    leftOffsetPct < 8
      ? 'translateX(0)'
      : leftOffsetPct > 92
        ? 'translateX(-100%)'
        : 'translateX(-50%)';

  return (
    <Box
      data-testid="timeline-playhead"
      sx={{
        position: 'absolute',
        left: `${leftOffsetPct}%`,
        top: 0,
        bottom: 0,
        width: '2px',
        bgcolor: color,
        zIndex: 2,
        pointerEvents: 'none',
      }}
    >
      <Box
        sx={{
          position: 'absolute',
          top: -20,
          left: 0,
          transform: lozengeTransform,
          bgcolor: color,
          color: theme.palette.getContrastText(color),
          px: 0.75,
          py: 0.25,
          borderRadius: 1,
          whiteSpace: 'nowrap',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        <Typography
          variant="caption"
          sx={{ fontWeight: 600, lineHeight: 1, display: 'block' }}
        >
          {label}
        </Typography>
      </Box>
    </Box>
  );
}

export default TimelinePlayhead;
