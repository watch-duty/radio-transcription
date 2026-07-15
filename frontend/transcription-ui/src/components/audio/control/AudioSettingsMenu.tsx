import { useState } from 'react';

import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Divider from '@mui/material/Divider';
import Icon from '@mui/material/Icon';
import IconButton from '@mui/material/IconButton';
import Popover from '@mui/material/Popover';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';

import { PanControl } from './PanControl';
import { SpeedControl } from './SpeedControl';

export interface AudioSettingsMenuProps {
  pan: number;
  setPan: (pan: number) => void;
  speed: number;
  setSpeed: (speed: number) => void;
  onReset?: () => void;
  disableControls?: boolean;
}

export function AudioSettingsMenu({
  pan,
  setPan,
  speed,
  setSpeed,
  onReset,
  disableControls = false,
}: AudioSettingsMenuProps) {
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);

  return (
    <>
      <Tooltip title="Audio controls">
        <span>
          <IconButton
            color="primary"
            disabled={disableControls}
            aria-label="audio controls"
            onClick={(e) => setAnchorEl(e.currentTarget)}
          >
            <Icon baseClassName="material-symbols-outlined">
              settings_slow_motion
            </Icon>
          </IconButton>
        </span>
      </Tooltip>
      <Popover
        open={Boolean(anchorEl)}
        anchorEl={anchorEl}
        onClose={() => setAnchorEl(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        transformOrigin={{ vertical: 'top', horizontal: 'right' }}
        sx={{ zIndex: 1300 }}
      >
        <Box
          sx={{
            p: 1.5,
            display: 'flex',
            flexDirection: 'column',
            gap: 1.5,
            width: 220,
          }}
        >
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
            <Typography variant="subtitle2">Pan</Typography>
            <PanControl
              pan={pan}
              setPan={setPan}
              disableControls={disableControls}
            />
          </Box>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
            <Typography variant="subtitle2">Speed</Typography>
            <SpeedControl
              speed={speed}
              setSpeed={setSpeed}
              disableControls={disableControls}
            />
          </Box>

          <Divider />

          <Box
            sx={{
              display: 'flex',
              justifyContent: 'flex-end',
            }}
          >
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
}

export default AudioSettingsMenu;
