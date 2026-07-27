import ToggleButton from '@mui/material/ToggleButton';
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup';

import { PAN_OPTIONS } from '../../../audio/audioSettings';

const PAN_LABELS: Record<number, string> = { '-1': 'L', '0': 'C', '1': 'R' };

export interface PanControlProps {
  pan: number;
  setPan: (pan: number) => void;
  disableControls?: boolean;
}

export function PanControl({
  pan,
  setPan,
  disableControls = false,
}: PanControlProps) {
  return (
    <ToggleButtonGroup
      exclusive
      fullWidth
      size="small"
      value={pan}
      disabled={disableControls}
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
  );
}

export default PanControl;
