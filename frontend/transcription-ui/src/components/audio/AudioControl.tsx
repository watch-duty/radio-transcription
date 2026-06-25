import Forward5Icon from '@mui/icons-material/Forward5';
import PauseIcon from '@mui/icons-material/PauseCircleFilledOutlined';
import PlayArrowIcon from '@mui/icons-material/PlayCircleFilledOutlined';
import Replay5Icon from '@mui/icons-material/Replay5';
import SkipNextIcon from '@mui/icons-material/SkipNext';
import SkipPreviousIcon from '@mui/icons-material/SkipPrevious';
import Box from '@mui/material/Box';
import Checkbox from '@mui/material/Checkbox';
import FormControlLabel from '@mui/material/FormControlLabel';
import Icon from '@mui/material/Icon';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import type { SxProps, Theme } from '@mui/material/styles';

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
        mb: 2.5,
        ...sx,
      }}
    >
      {/* Left spacer to balance the checkbox on the right */}
      <Box sx={{ flex: 1, display: 'flex', justifyContent: 'flex-start' }} />

      {/* Center: 7 control buttons */}
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
              <Icon
                baseClassName="material-symbols-outlined"
                fontSize="large"
                sx={{ transform: 'scaleX(-1)' }}
              >
                chat_paste_go
              </Icon>
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
        <Tooltip title={isAudioPlaying ? 'Pause' : 'Play'}>
          <span>
            <IconButton
              onClick={onTogglePlayPause}
              size="large"
              color="primary"
              sx={{ p: 0.5 }}
              aria-label={isAudioPlaying ? 'pause' : 'play'}
              disabled={disableControls}
            >
              {isAudioPlaying ? (
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
              <Icon baseClassName="material-symbols-outlined" fontSize="large">
                chat_paste_go
              </Icon>
            </IconButton>
          </span>
        </Tooltip>
      </Box>

      {/* Right side: Checkbox */}
      <Box sx={{ flex: 1, display: 'flex', justifyContent: 'flex-end' }}>
        <FormControlLabel
          control={
            <Checkbox
              checked={playLatestAudio}
              onChange={(e) => togglePlayLatestAudio(e.target.checked)}
              disabled={disableCheckbox}
            />
          }
          label="Always play latest audio"
          slotProps={{ typography: { variant: 'body2' } }}
        />
      </Box>
    </Box>
  );
}
