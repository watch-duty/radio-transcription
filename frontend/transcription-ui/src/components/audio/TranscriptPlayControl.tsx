import PauseIcon from '@mui/icons-material/PauseCircleFilledOutlined';
import PlayArrowIcon from '@mui/icons-material/PlayCircleFilledOutlined';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import { useTheme } from '@mui/material/styles';

export interface TranscriptPlayControlProps {
  audioUri: string;
  segmentId: string;
  onToggleAudio: (segmentId: string, audioUri: string) => void;
  isAudioPlaying: boolean;
  currentlyPlayingSegmentId: string | null;
  hideButton?: boolean;
}

function TranscriptPlayControl({
  audioUri,
  segmentId,
  onToggleAudio,
  isAudioPlaying,
  currentlyPlayingSegmentId,
  hideButton = false,
}: TranscriptPlayControlProps) {
  const theme = useTheme();

  const isPlayingSegment = segmentId === currentlyPlayingSegmentId;
  const showPauseIcon = isAudioPlaying && isPlayingSegment;

  if (!isPlayingSegment && hideButton) {
    return <Box sx={{ height: theme.spacing(5), width: theme.spacing(5) }} />;
  }

  return (
    <IconButton
      onClick={(e) => {
        e.stopPropagation();
        if (audioUri) {
          onToggleAudio(segmentId, audioUri);
        }
      }}
      color="primary"
      aria-label={showPauseIcon ? 'pause' : 'play'}
      disabled={!audioUri}
    >
      {showPauseIcon ? <PauseIcon /> : <PlayArrowIcon />}
    </IconButton>
  );
}

export default TranscriptPlayControl;
