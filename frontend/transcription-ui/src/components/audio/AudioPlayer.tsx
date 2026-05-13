import PauseIcon from '@mui/icons-material/Pause';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import IconButton from '@mui/material/IconButton';

export interface AudioPlayerProps {
  audioUri: string;
  transmissionId: string;
  onToggleAudio: (transmissionId: string, audioUri: string) => void;
  isAudioPlaying: boolean;
  currentlyPlayingTransmissionId: string | null;
}

function AudioPlayer({
  audioUri,
  transmissionId,
  onToggleAudio,
  isAudioPlaying,
  currentlyPlayingTransmissionId,
}: AudioPlayerProps) {
  const showPauseIcon =
    isAudioPlaying && transmissionId === currentlyPlayingTransmissionId;

  return (
    <IconButton
      onClick={(e) => {
        e.stopPropagation();
        onToggleAudio(transmissionId, audioUri);
      }}
      color="primary"
      aria-label={showPauseIcon ? 'pause' : 'play'}
    >
      {showPauseIcon ? <PauseIcon /> : <PlayArrowIcon />}
    </IconButton>
  );
}

export default AudioPlayer;
