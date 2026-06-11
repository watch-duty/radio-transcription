import PauseIcon from '@mui/icons-material/PauseCircleFilledOutlined';
import PlayArrowIcon from '@mui/icons-material/PlayCircleFilledOutlined';
import IconButton from '@mui/material/IconButton';

export interface AudioPlayerProps {
  audioUri: string;
  segmentId: string;
  onToggleAudio: (segmentId: string, audioUri: string) => void;
  isAudioPlaying: boolean;
  currentlyPlayingSegmentId: string | null;
  size?: 'small' | 'medium';
}

function AudioPlayer({
  audioUri,
  segmentId,
  onToggleAudio,
  isAudioPlaying,
  currentlyPlayingSegmentId,
  size = 'medium',
}: AudioPlayerProps) {
  const showPauseIcon =
    isAudioPlaying && segmentId === currentlyPlayingSegmentId;

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
      size={size}
    >
      {showPauseIcon ? (
        <PauseIcon fontSize={size === 'small' ? 'small' : 'inherit'} />
      ) : (
        <PlayArrowIcon fontSize={size === 'small' ? 'small' : 'inherit'} />
      )}
    </IconButton>
  );
}

export default AudioPlayer;
