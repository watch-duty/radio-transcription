import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import { DateTimePicker as MuiDateTimePicker } from '@mui/x-date-pickers/DateTimePicker';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import type { PickersActionBarAction } from '@mui/x-date-pickers/PickersActionBar';

export interface DateTimePickerProps {
  label: string;
  dateTime: Date | null;
  setDateTime: (dateTime: Date | null) => void;
  error?: boolean;
  helperText?: string;
  width?: string | number;
  // Picker-popup buttons; wrappers with their own Clear/Apply pass ['accept'].
  actions?: PickersActionBarAction[];
}

export function DateTimePicker({
  label,
  dateTime,
  setDateTime,
  error,
  helperText,
  width,
  actions = ['clear', 'cancel', 'accept'],
}: DateTimePickerProps) {
  return (
    <LocalizationProvider dateAdapter={AdapterDateFns}>
      <MuiDateTimePicker
        label={label}
        value={dateTime}
        onChange={setDateTime}
        ampm={false}
        slotProps={{
          textField: {
            size: 'small',
            error: error,
            helperText: helperText,
            sx: width ? { width } : undefined,
          },
          actionBar: {
            actions,
          },
        }}
      />
    </LocalizationProvider>
  );
}

export default DateTimePicker;
