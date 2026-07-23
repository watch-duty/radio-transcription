import Box from '@mui/material/Box';
import ToggleButton from '@mui/material/ToggleButton';
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup';
import Tooltip from '@mui/material/Tooltip';

import { SPEED_OPTIONS } from '../../../audio/audioSettings';
import { isSafari } from '../../../utils/browser';

export interface SpeedControlProps {
  speed: number;
  setSpeed: (speed: number) => void;
  disableControls?: boolean;
}

export function SpeedControl({
  speed,
  setSpeed,
  disableControls = false,
}: SpeedControlProps) {
  const speedDisabled = disableControls || isSafari();

  return (
    <Tooltip title={isSafari() ? 'Speed control is unavailable in Safari' : ''}>
      <Box component="span" sx={{ display: 'inline-flex', width: '100%' }}>
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
  );
}

export default SpeedControl;
