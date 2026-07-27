import Forward5Icon from '@mui/icons-material/Forward5';
import PauseIcon from '@mui/icons-material/PauseCircleFilledOutlined';
import PlayArrowIcon from '@mui/icons-material/PlayCircleFilledOutlined';
import Replay5Icon from '@mui/icons-material/Replay5';
import SkipNextIcon from '@mui/icons-material/SkipNext';
import SkipPreviousIcon from '@mui/icons-material/SkipPrevious';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import Icon, { type IconProps } from '@mui/material/Icon';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import { type SxProps, type Theme } from '@mui/material/styles';

import { DEFAULT_PAN, DEFAULT_SPEED } from '../../audio/audioSettings';
import { useIsNarrow } from '../../hooks/useIsNarrow';
import { AudioSettingsMenu } from './control/AudioSettingsMenu';
import { VolumeControl } from './control/VolumeControl';

const PAN_LABELS: Record<number, string> = { '-1': 'L', '0': 'C', '1': 'R' };

export interface AudioControlProps {
  isAudioPlaying: boolean;
  onTogglePlayPause: () => void;
  onSkipToNext: () => void;
  onSkipToPrevious: () => void;
  onFastForward: () => void;
  onFastRewind: () => void;
  onSkipTime: (offsetSeconds: number) => void;
  disableControls?: boolean;
  volumeDb?: number;
  setVolumeDb?: (db: number) => void;
  pan?: number;
  setPan?: (pan: number) => void;
  speed?: number;
  setSpeed?: (speed: number) => void;
  onReset?: () => void;
  sx?: SxProps<Theme>;
}

export function AudioControl({
  isAudioPlaying,
  onTogglePlayPause,
  onSkipToNext,
  onSkipToPrevious,
  onFastForward,
  onFastRewind,
  onSkipTime,
  disableControls = false,
  volumeDb,
  setVolumeDb,
  pan,
  setPan,
  speed,
  setSpeed,
  onReset,
  sx,
}: AudioControlProps) {
  const isNarrow = useIsNarrow();

  const controlSize = isNarrow ? 'medium' : 'large';

  const hasAudioSettings =
    volumeDb !== undefined &&
    setVolumeDb !== undefined &&
    pan !== undefined &&
    setPan !== undefined &&
    speed !== undefined &&
    setSpeed !== undefined;

  return (
    <Box
      sx={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        position: 'relative',
        width: '100%',
        mb: 2.5,
        flexWrap: { xs: 'wrap', md: 'nowrap' },
        gap: { xs: 1, md: 0 },
        ...sx,
      }}
    >
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 0,
          justifyContent: 'center',
          mx: 'auto',
        }}
      >
        <Tooltip title="Rewind to previous detected speech">
          <span>
            <IconButton
              onClick={onFastRewind}
              size={controlSize}
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="rewind to previous detected speech"
              disabled={disableControls}
            >
              <MoveToSpeechIcon
                fontSize={controlSize}
                sx={{ transform: 'scaleX(-1)' }}
              />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Rewind to previous segment">
          <span>
            <IconButton
              onClick={onSkipToPrevious}
              size={controlSize}
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="rewind to previous segment"
              disabled={disableControls}
            >
              <SkipPreviousIcon fontSize={controlSize} />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Rewind 5 seconds">
          <span>
            <IconButton
              onClick={() => onSkipTime(-5)}
              size={controlSize}
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="rewind 5 seconds"
              disabled={disableControls}
            >
              <Replay5Icon fontSize={controlSize} />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title={isAudioPlaying ? 'Pause' : 'Play'}>
          <span>
            <IconButton
              onClick={onTogglePlayPause}
              size={controlSize}
              color="primary"
              sx={{ p: 0.5 }}
              aria-label={isAudioPlaying ? 'pause' : 'play'}
              disabled={disableControls}
            >
              {isAudioPlaying ? (
                <PauseIcon fontSize={controlSize} />
              ) : (
                <PlayArrowIcon fontSize={controlSize} />
              )}
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Advance 5 seconds">
          <span>
            <IconButton
              onClick={() => onSkipTime(5)}
              size={controlSize}
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="advance 5 seconds"
              disabled={disableControls}
            >
              <Forward5Icon fontSize={controlSize} />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Advance to next segment">
          <span>
            <IconButton
              onClick={onSkipToNext}
              size={controlSize}
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="advance to next segment"
              disabled={disableControls}
            >
              <SkipNextIcon fontSize={controlSize} />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Advance to next detected speech">
          <span>
            <IconButton
              onClick={onFastForward}
              size={controlSize}
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="advance to next detected speech"
              disabled={disableControls}
            >
              <MoveToSpeechIcon fontSize={controlSize} />
            </IconButton>
          </span>
        </Tooltip>
      </Box>

      {hasAudioSettings && (
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: { xs: 0.25, sm: 0.5 },
            position: 'absolute',
            left: 0,
          }}
        >
          <VolumeControl
            volumeDb={volumeDb}
            setVolumeDb={setVolumeDb}
            disableControls={disableControls}
          />
          <AudioSettingsMenu
            pan={pan}
            setPan={setPan}
            speed={speed}
            setSpeed={setSpeed}
            onReset={onReset}
            disableControls={disableControls}
          />
          {pan !== undefined && pan !== DEFAULT_PAN && (
            <Chip
              size="small"
              variant="outlined"
              label={`Pan ${PAN_LABELS[pan] ?? pan}`}
              onDelete={setPan ? () => setPan(DEFAULT_PAN) : undefined}
            />
          )}
          {speed !== undefined && speed !== DEFAULT_SPEED && (
            <Chip
              size="small"
              variant="outlined"
              label={`${speed}×`}
              onDelete={setSpeed ? () => setSpeed(DEFAULT_SPEED) : undefined}
            />
          )}
        </Box>
      )}
    </Box>
  );
}

function MoveToSpeechIcon(props: IconProps) {
  return (
    <Icon baseClassName="material-symbols-outlined" {...props}>
      chat_paste_go
    </Icon>
  );
}
