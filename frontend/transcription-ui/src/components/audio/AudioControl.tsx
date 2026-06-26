import SkipNextIcon from '@mui/icons-material/SkipNext';
import SkipPreviousIcon from '@mui/icons-material/SkipPrevious';
import Box from '@mui/material/Box';
import Checkbox from '@mui/material/Checkbox';
import FormControlLabel from '@mui/material/FormControlLabel';
import Icon, { type IconProps } from '@mui/material/Icon';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import type { SxProps, Theme } from '@mui/material/styles';

import TranscriptPlayControl from './TranscriptPlayControl';

export interface AudioControlProps {
  isAudioPlaying: boolean;
  onTogglePlayPause: () => void;
  onSkipToNext: () => void;
  onSkipToPrevious: () => void;
  onFastForward: () => void;
  onFastRewind: () => void;
  onSkipTime: (offsetSeconds: number) => void;
  playLatestAudio: boolean;
  togglePlayLatestAudio: (checked: boolean) => void;
  disableControls?: boolean;
  disableCheckbox?: boolean;
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
  playLatestAudio,
  togglePlayLatestAudio,
  disableControls = false,
  disableCheckbox = false,
  sx,
}: AudioControlProps) {
  return (
    <Box
      sx={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        width: '100%',
        ...sx,
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Tooltip title="Rewind to previous transmission">
          <span>
            <IconButton
              onClick={onSkipToPrevious}
              size="large"
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="rewind to previous transmission"
              disabled={disableControls}
            >
              <SkipPreviousIcon fontSize="large" />
            </IconButton>
          </span>
        </Tooltip>

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
              <Icon baseClassName="material-symbols-outlined" fontSize="large">
                replay_5
              </Icon>
            </IconButton>
          </span>
        </Tooltip>

        <TranscriptPlayControl
          audioUri={disableControls ? '' : 'dummy'}
          segmentId="dummy"
          onToggleAudio={onTogglePlayPause}
          isAudioPlaying={isAudioPlaying}
          currentlyPlayingSegmentId="dummy"
          hideButton={false}
        />

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
              <Icon baseClassName="material-symbols-outlined" fontSize="large">
                forward_5
              </Icon>
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

        <Tooltip title="Advance to next transmission">
          <span>
            <IconButton
              onClick={onSkipToNext}
              size="large"
              color="primary"
              sx={{ p: 0.5 }}
              aria-label="advance to next transmission"
              disabled={disableControls}
            >
              <SkipNextIcon fontSize="large" />
            </IconButton>
          </span>
        </Tooltip>
      </Box>

      <FormControlLabel
        control={
          <Checkbox
            checked={playLatestAudio}
            onChange={(e) => togglePlayLatestAudio(e.target.checked)}
            disabled={disableCheckbox}
          />
        }
        label="Always play latest audio"
      />
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
