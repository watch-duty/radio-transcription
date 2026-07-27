import VolumeOffIcon from '@mui/icons-material/VolumeOff';
import VolumeUpIcon from '@mui/icons-material/VolumeUp';
import Box from '@mui/material/Box';
import Slider from '@mui/material/Slider';
import Tooltip from '@mui/material/Tooltip';

import {
  formatVolumeDb,
  snapVolumeToDefault,
} from '../../../audio/WebAudioPlayer';
import {
  DEFAULT_VOLUME_DB,
  VOLUME_MAX_DB,
  VOLUME_MIN_DB,
} from '../../../audio/audioSettings';

export interface VolumeControlProps {
  volumeDb: number;
  setVolumeDb: (db: number) => void;
  disableControls?: boolean;
}

export function VolumeControl({
  volumeDb,
  setVolumeDb,
  disableControls = false,
}: VolumeControlProps) {
  const volumeLabel = formatVolumeDb(volumeDb);
  const isMuted = volumeLabel === 'Muted' || volumeDb <= VOLUME_MIN_DB;
  const VolumeIcon = isMuted ? VolumeOffIcon : VolumeUpIcon;

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
      <VolumeIcon
        fontSize="small"
        color={disableControls ? 'disabled' : 'action'}
      />
      <Tooltip title={volumeLabel}>
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            width: { xs: 60, sm: 90 },
          }}
        >
          <Slider
            aria-label="Volume"
            size="small"
            value={volumeDb}
            min={VOLUME_MIN_DB}
            max={VOLUME_MAX_DB}
            step={1}
            marks={[{ value: DEFAULT_VOLUME_DB }]}
            disabled={disableControls}
            onChange={(event, value) => {
              const db = value as number;
              const fromKeyboard = event.type === 'keydown';
              setVolumeDb(fromKeyboard ? db : snapVolumeToDefault(db));
            }}
            sx={{ py: 0.5 }}
          />
        </Box>
      </Tooltip>
    </Box>
  );
}

export default VolumeControl;
