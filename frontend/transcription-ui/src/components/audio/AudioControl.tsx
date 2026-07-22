import type { ReactNode } from 'react';

import Forward5Icon from '@mui/icons-material/Forward5';
import PauseIcon from '@mui/icons-material/PauseCircleFilledOutlined';
import PlayArrowIcon from '@mui/icons-material/PlayCircleFilledOutlined';
import Replay5Icon from '@mui/icons-material/Replay5';
import SkipNextIcon from '@mui/icons-material/SkipNext';
import SkipPreviousIcon from '@mui/icons-material/SkipPrevious';
import Box from '@mui/material/Box';
import Icon, { type IconProps } from '@mui/material/Icon';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import { type SxProps, type Theme } from '@mui/material/styles';

import { useIsNarrow } from '../../hooks/useIsNarrow';

export interface AudioControlProps {
  isAudioPlaying: boolean;
  onTogglePlayPause: () => void;
  onSkipToNext: () => void;
  onSkipToPrevious: () => void;
  onFastForward: () => void;
  onFastRewind: () => void;
  onSkipTime: (offsetSeconds: number) => void;
  disableControls?: boolean;
  // Rendered just after the transport buttons (the audio-settings button).
  settingsButton?: ReactNode;
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
  settingsButton,
  sx,
}: AudioControlProps) {
  const isNarrow = useIsNarrow();

  const controlSize = isNarrow ? 'medium' : 'large';

  return (
    <Box
      sx={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        width: '100%',
        mb: 2.5,
        ...sx,
      }}
    >
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 0,
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
        {settingsButton && (
          <Box sx={{ display: 'inline-flex', ml: { xs: 0.5, sm: 1 } }}>
            {settingsButton}
          </Box>
        )}
      </Box>
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
