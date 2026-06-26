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
import type { SxProps, Theme } from '@mui/material/styles';

export interface AudioControlProps {
  // Playing and live "listening" both show the pause icon; only paused shows play.
  showPauseIcon: boolean;
  onTogglePlayPause: () => void;
  onSkipToNext: () => void;
  onSkipToPrevious: () => void;
  onFastForward: () => void;
  onFastRewind: () => void;
  onSkipTime: (offsetSeconds: number) => void;
  disableControls?: boolean;
  endSlot?: ReactNode;
  sx?: SxProps<Theme>;
}

export function AudioControl({
  showPauseIcon,
  onTogglePlayPause,
  onSkipToNext,
  onSkipToPrevious,
  onFastForward,
  onFastRewind,
  onSkipTime,
  disableControls = false,
  endSlot,
  sx,
}: AudioControlProps) {
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
              size="large"
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="rewind to previous detected speech"
              disabled={disableControls}
            >
              <MoveToSpeechIcon
                fontSize="large"
                sx={{ transform: 'scaleX(-1)' }}
              />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Rewind to previous segment">
          <span>
            <IconButton
              onClick={onSkipToPrevious}
              size="large"
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="rewind to previous segment"
              disabled={disableControls}
            >
              <SkipPreviousIcon fontSize="large" />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Rewind 5 seconds">
          <span>
            <IconButton
              onClick={() => onSkipTime(-5)}
              size="large"
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="rewind 5 seconds"
              disabled={disableControls}
            >
              <Replay5Icon fontSize="large" />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title={showPauseIcon ? 'Pause' : 'Play'}>
          <span>
            <IconButton
              onClick={onTogglePlayPause}
              size="large"
              color="primary"
              sx={{ p: 0.5 }}
              aria-label={showPauseIcon ? 'pause' : 'play'}
              disabled={disableControls}
            >
              {showPauseIcon ? (
                <PauseIcon fontSize="large" />
              ) : (
                <PlayArrowIcon fontSize="large" />
              )}
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Advance 5 seconds">
          <span>
            <IconButton
              onClick={() => onSkipTime(5)}
              size="large"
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="advance 5 seconds"
              disabled={disableControls}
            >
              <Forward5Icon fontSize="large" />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Advance to next segment">
          <span>
            <IconButton
              onClick={onSkipToNext}
              size="large"
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="advance to next segment"
              disabled={disableControls}
            >
              <SkipNextIcon fontSize="large" />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Advance to next detected speech">
          <span>
            <IconButton
              onClick={onFastForward}
              size="large"
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="advance to next detected speech"
              disabled={disableControls}
            >
              <MoveToSpeechIcon fontSize="large" />
            </IconButton>
          </span>
        </Tooltip>
        {endSlot}
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
