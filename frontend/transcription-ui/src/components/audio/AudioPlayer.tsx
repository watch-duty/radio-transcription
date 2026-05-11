import PauseIcon from '@mui/icons-material/Pause';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import IconButton from '@mui/material/IconButton';

export interface AudioPlayerProps {
  audioUri: string;
  transmissionId: string;
  currentlyPlayingTransmissionId: string | null;
  isPlaying: boolean;
  onToggleAudio: (transmissionId: string, audioUri: string) => void;
}

function AudioPlayer({ audioUri, transmissionId, currentlyPlayingTransmissionId, isPlaying, onToggleAudio }: AudioPlayerProps) {
  const showPauseIcon = isPlaying && transmissionId === currentlyPlayingTransmissionId;

  return (
    <IconButton
      onClick={() => onToggleAudio(transmissionId, audioUri)}
      color="primary"
      aria-label={showPauseIcon ? 'pause' : 'play'}
    >
      {showPauseIcon ? <PauseIcon /> : <PlayArrowIcon />}
    </IconButton>
  );
}

export default AudioPlayer;
