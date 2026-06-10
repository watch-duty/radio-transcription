import PauseIcon from '@mui/icons-material/PauseCircleFilledOutlined';
import PlayArrowIcon from '@mui/icons-material/PlayCircleFilledOutlined';
import IconButton from '@mui/material/IconButton';

export interface AudioPlayerProps {
  audioUri: string;
  segmentId: string;
  onToggleAudio: (segmentId: string, audioUri: string) => void;
  isAudioPlaying: boolean;
  currentlyPlayingSegmentId: string | null;
}

function AudioPlayer({
  audioUri,
  segmentId,
  onToggleAudio,
  isAudioPlaying,
  currentlyPlayingSegmentId,
}: AudioPlayerProps) {
  const showPauseIcon =
    isAudioPlaying && segmentId === currentlyPlayingSegmentId;

  return (
    <IconButton
      onClick={(e) => {
        e.stopPropagation();
        onToggleAudio(segmentId, audioUri);
      }}
      color="primary"
      aria-label={showPauseIcon ? 'pause' : 'play'}
    >
      {showPauseIcon ? <PauseIcon /> : <PlayArrowIcon />}
    </IconButton>
  );
}

export default AudioPlayer;
