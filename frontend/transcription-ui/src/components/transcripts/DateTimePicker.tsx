import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import { DateTimePicker as MuiDateTimePicker } from '@mui/x-date-pickers/DateTimePicker';

export interface DateTimePickerProps {
  label: string;
  dateTime: Date | null;
  setDateTime: (dateTime: Date | null) => void;
  error?: boolean;
  helperText?: string;
  width?: string | number;
}

export function DateTimePicker({
  label,
  dateTime,
  setDateTime,
  error,
  helperText,
  width,
}: DateTimePickerProps) {
  return (
    <LocalizationProvider dateAdapter={AdapterDateFns}>
      <MuiDateTimePicker 
        label={label} 
        value={dateTime} 
        onChange={setDateTime} 
        slotProps={{ 
          textField: { 
            size: 'small',
            error: error,
            helperText: helperText,
            sx: width ? { width } : undefined
          } 
        }}
      />
    </LocalizationProvider>
  );
}

export default DateTimePicker;