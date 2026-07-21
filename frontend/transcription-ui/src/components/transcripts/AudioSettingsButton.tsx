import { useState } from 'react';

import VolumeOffIcon from '@mui/icons-material/VolumeOff';
import VolumeUpIcon from '@mui/icons-material/VolumeUp';
import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';
import Popover from '@mui/material/Popover';
import Slider from '@mui/material/Slider';
import ToggleButton from '@mui/material/ToggleButton';
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';

import {
  formatVolumeDb,
  snapVolumeToDefault,
} from '../../audio/WebAudioPlayer';
import {
  DEFAULT_PAN,
  DEFAULT_SPEED,
  DEFAULT_VOLUME_DB,
  PAN_OPTIONS,
  SPEED_OPTIONS,
  VOLUME_MAX_DB,
  VOLUME_MIN_DB,
} from '../../audio/audioSettings';
import { isSafari } from '../../utils/browser';

const PAN_LABELS: Record<number, string> = { '-1': 'L', '0': 'C', '1': 'R' };

// Only resize the speaker icon once the gain is more than this far from default.
const VOLUME_ICON_SCALE_THRESHOLD_DB = 4;

function volumeIconScaleFor(volumeDb: number): number {
  if (volumeDb < DEFAULT_VOLUME_DB - VOLUME_ICON_SCALE_THRESHOLD_DB) return 0.7;
  if (volumeDb > DEFAULT_VOLUME_DB + VOLUME_ICON_SCALE_THRESHOLD_DB) return 1.3;
  return 1;
}

export interface AudioSettingsButtonProps {
  volumeDb: number;
  setVolumeDb: (db: number) => void;
  pan: number;
  setPan: (pan: number) => void;
  speed: number;
  setSpeed: (speed: number) => void;
  onReset: () => void;
  disableControls: boolean;
  // Silenced by another feed's solo (scanner). Distinct from the user's own
  // volume being at mute.
  muted?: boolean;
}

export const AudioSettingsButton: React.FC<AudioSettingsButtonProps> = ({
  volumeDb,
  setVolumeDb,
  pan,
  setPan,
  speed,
  setSpeed,
  onReset,
  disableControls,
  muted = false,
}) => {
  const theme = useTheme();
  const [audioAnchorEl, setAudioAnchorEl] = useState<HTMLElement | null>(null);
  const speedDisabled = isSafari();

  const volumeLabel = formatVolumeDb(volumeDb);
  const isMuted = volumeLabel === 'Muted';
  const volumeActive = volumeDb !== DEFAULT_VOLUME_DB;
  const panLabel = pan !== DEFAULT_PAN ? PAN_LABELS[pan] : null;
  const speedActive = speed !== DEFAULT_SPEED;
  // The icon stays put except for mute; the scale below conveys cut vs. boost.
  const VolumeIcon = isMuted || muted ? VolumeOffIcon : VolumeUpIcon;
  const volumeIconScale = volumeIconScaleFor(volumeDb);

  // Pan and speed each surface a badge; the border only shows to anchor them.
  const hasBadge = Boolean(panLabel) || speedActive;

  const activeSummary = [
    volumeActive ? volumeLabel : null,
    panLabel ? `Pan ${panLabel}` : null,
    speedActive ? `${speed}×` : null,
  ].filter(Boolean);
  const audioTooltip = muted
    ? 'Muted — another feed is soloed'
    : activeSummary.length
      ? activeSummary.join(', ')
      : 'Audio controls';
  const audioBadgeSx = {
    '& .MuiBadge-badge': {
      fontSize: '0.5rem',
      height: 14,
      minWidth: 14,
      px: 0.5,
      boxShadow: `0 0 0 2px ${theme.palette.background.paper}`,
    },
  };

  return (
    <>
      <Tooltip title={audioTooltip}>
        <Badge
          color="primary"
          badgeContent={speedActive ? `${speed}×` : undefined}
          invisible={!speedActive}
          anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
          sx={audioBadgeSx}
        >
          <Badge
            color="primary"
            badgeContent={panLabel}
            invisible={!panLabel}
            anchorOrigin={{
              vertical: 'bottom',
              horizontal: pan < DEFAULT_PAN ? 'left' : 'right',
            }}
            sx={audioBadgeSx}
          >
            <IconButton
              color="primary"
              size="small"
              aria-label="audio controls"
              disabled={disableControls}
              onClick={(e) => setAudioAnchorEl(e.currentTarget)}
              sx={{ border: hasBadge ? 1 : 0, borderColor: 'divider' }}
            >
              <VolumeIcon
                fontSize="small"
                sx={{
                  transform: `scale(${volumeIconScale})`,
                  transition: 'transform 0.15s ease',
                }}
              />
            </IconButton>
          </Badge>
        </Badge>
      </Tooltip>
      <Popover
        open={Boolean(audioAnchorEl)}
        anchorEl={audioAnchorEl}
        onClose={() => setAudioAnchorEl(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        transformOrigin={{ vertical: 'top', horizontal: 'left' }}
        sx={{ zIndex: 1300 }}
      >
        <Box
          sx={{
            p: 1.5,
            display: 'flex',
            flexDirection: 'column',
            gap: 1.25,
            width: 210,
          }}
        >
          <Box>
            <Box
              sx={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'baseline',
              }}
            >
              <Typography variant="subtitle2">Volume</Typography>
              <Typography variant="caption" color="text.secondary">
                {formatVolumeDb(volumeDb)}
              </Typography>
            </Box>
            <Slider
              aria-label="Volume"
              size="small"
              value={volumeDb}
              min={VOLUME_MIN_DB}
              max={VOLUME_MAX_DB}
              step={1}
              marks={[{ value: DEFAULT_VOLUME_DB }]}
              onChange={(event, value) => {
                const db = value as number;
                // MUI emits keyboard changes as a `keydown` event and pointer
                // drags as mouse/touch events. Magnetize the default on a drag
                // only, so arrow-key steps stay exact and every dB is reachable.
                const fromKeyboard = event.type === 'keydown';
                setVolumeDb(fromKeyboard ? db : snapVolumeToDefault(db));
              }}
              sx={{ display: 'block', mt: 0.5, mb: 0, py: 0.5 }}
            />
          </Box>

          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
            <Typography variant="subtitle2">Pan</Typography>
            <ToggleButtonGroup
              exclusive
              fullWidth
              size="small"
              value={pan}
              onChange={(_, value) => {
                if (value !== null) setPan(value as number);
              }}
            >
              {PAN_OPTIONS.map((option) => (
                <ToggleButton
                  key={option}
                  value={option}
                  aria-label={`Pan ${PAN_LABELS[option]}`}
                >
                  {PAN_LABELS[option]}
                </ToggleButton>
              ))}
            </ToggleButtonGroup>
          </Box>

          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
            <Typography variant="subtitle2">Speed</Typography>
            <Tooltip
              title={
                speedDisabled ? 'Speed control is unavailable in Safari' : ''
              }
            >
              {/* Span wrapper so the tooltip still fires when the group is
                  disabled — a disabled control has no pointer events of its own. */}
              <Box
                component="span"
                sx={{ display: 'inline-flex', width: '100%' }}
              >
                <ToggleButtonGroup
                  exclusive
                  fullWidth
                  size="small"
                  value={speed}
                  disabled={speedDisabled}
                  onChange={(_, value) => {
                    if (value !== null) setSpeed(value as number);
                  }}
                  sx={{ '& .MuiToggleButton-root': { px: 0.5, fontSize: 12 } }}
                >
                  {SPEED_OPTIONS.map((option) => (
                    <ToggleButton
                      key={option}
                      value={option}
                      aria-label={`Speed ${option}x`}
                    >
                      {option}×
                    </ToggleButton>
                  ))}
                </ToggleButtonGroup>
              </Box>
            </Tooltip>
          </Box>

          <Divider />

          <Box
            sx={{
              display: 'flex',
              justifyContent: 'flex-end',
            }}
          >
            {/* Always enabled (not hidden when at defaults) so keyboard focus
                isn't lost the moment a reset returns everything to default. */}
            <Button
              size="small"
              variant="outlined"
              onClick={onReset}
              sx={{ textTransform: 'none' }}
            >
              Reset
            </Button>
          </Box>
        </Box>
      </Popover>
    </>
  );
};

export default AudioSettingsButton;
