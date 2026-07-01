import React, { useRef, useState } from 'react';

import CalendarMonthIcon from '@mui/icons-material/CalendarMonth';
import SearchIcon from '@mui/icons-material/Search';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import FormControl from '@mui/material/FormControl';
import FormControlLabel from '@mui/material/FormControlLabel';
import IconButton from '@mui/material/IconButton';
import InputAdornment from '@mui/material/InputAdornment';
import MenuItem from '@mui/material/MenuItem';
import Popover from '@mui/material/Popover';
import Select from '@mui/material/Select';
import Switch from '@mui/material/Switch';
import TextField from '@mui/material/TextField';
import Tooltip from '@mui/material/Tooltip';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import { DesktopDateTimePicker } from '@mui/x-date-pickers/DesktopDateTimePicker';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';

import type { AlertFilter } from '../../hooks/useAudioSegments';

export interface TranscriptActionsBarProps {
  // True when the view is already fully live, so "Jump to live" has nothing to
  // do; the owner derives it from the window/playback/date-filter signals.
  disableJumpToLive: boolean;
  redactTranscripts: boolean;
  setRedactTranscripts: (redact: boolean) => void;
  dateTime: Date | null;
  setDateTime: (dateTime: Date | null) => void;
  alertFilter: AlertFilter;
  setAlertFilter: (alertFilter: AlertFilter) => void;
  onClickViewLatest: () => void;
  searchQuery?: string;
  setSearchQuery?: (query: string) => void;
}

// The calendar icon is the picker's trigger, so it needs no text-input field.
const HiddenPickerField = () => null;

// Shared height, radius, and font so the live control, calendar button, filter
// dropdown, and redact toggle read as one toolbar.
const CONTROL_HEIGHT = 32;
const CONTROL_RADIUS = 1;
const CONTROL_FONT_SIZE = '0.8125rem';

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  disableJumpToLive,
  redactTranscripts,
  setRedactTranscripts,
  dateTime,
  setDateTime,
  alertFilter,
  setAlertFilter,
  onClickViewLatest,
  searchQuery = '',
  setSearchQuery = () => {},
}) => {
  const [pickerOpen, setPickerOpen] = useState(false);
  // The picker's draft selection, so "Clear" can hide when nothing is picked.
  const [pickerValue, setPickerValue] = useState<Date | null>(dateTime);
  const calendarButtonRef = useRef<HTMLButtonElement>(null);

  // Search lives in a popover; the draft only commits to searchQuery on Apply.
  const [searchAnchorEl, setSearchAnchorEl] = useState<HTMLElement | null>(
    null
  );
  const [localSearchQuery, setLocalSearchQuery] = useState(searchQuery);
  const [prevSearchQuery, setPrevSearchQuery] = useState(searchQuery);
  if (searchQuery !== prevSearchQuery) {
    setPrevSearchQuery(searchQuery);
    setLocalSearchQuery(searchQuery);
  }

  const handleSearchOpen = (event: React.MouseEvent<HTMLElement>) => {
    setSearchAnchorEl(event.currentTarget);
    setLocalSearchQuery(searchQuery);
  };
  const handleSearchClose = () => {
    setSearchAnchorEl(null);
    setLocalSearchQuery(searchQuery);
  };
  const handleSearchApply = () => {
    setSearchQuery(localSearchQuery);
    setSearchAnchorEl(null);
  };
  const handleSearchClear = () => {
    setLocalSearchQuery('');
    setSearchQuery('');
    setSearchAnchorEl(null);
  };

  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'space-between',
        mb: 0.5,
        // Lift the bar (and its overflowing speaker badges) above the list's
        // sticky headers (zIndex 1) so they aren't clipped behind them.
        position: 'relative',
        zIndex: 2,
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
        <Button
          variant="contained"
          size="small"
          disableElevation
          disabled={disableJumpToLive}
          sx={{
            textTransform: 'none',
            fontSize: CONTROL_FONT_SIZE,
            height: CONTROL_HEIGHT,
            borderRadius: CONTROL_RADIUS,
          }}
          onClick={onClickViewLatest}
        >
          Jump to live
        </Button>

        <Tooltip title="Filter by date and time">
          <IconButton
            ref={calendarButtonRef}
            aria-label="filter by date"
            size="small"
            onClick={() => {
              setPickerValue(dateTime);
              setPickerOpen(true);
            }}
            color={dateTime ? 'primary' : 'default'}
            sx={{ width: CONTROL_HEIGHT, height: CONTROL_HEIGHT }}
          >
            <CalendarMonthIcon />
          </IconButton>
        </Tooltip>
        <LocalizationProvider dateAdapter={AdapterDateFns}>
          <DesktopDateTimePicker
            open={pickerOpen}
            value={pickerValue}
            ampm={false}
            onChange={(value) => setPickerValue(value)}
            onClose={() => setPickerOpen(false)}
            // Accept applies the pick (Clear removes the filter → live).
            onAccept={(value) => setDateTime(value)}
            localeText={{ okButtonLabel: 'Apply' }}
            slots={{ field: HiddenPickerField }}
            slotProps={{
              // Open below the icon but shift up to stay fully on screen (the
              // icon sits low), rather than forcing a fixed side.
              popper: {
                // Non-null: the calendar button is always mounted (it's the trigger).
                anchorEl: () => calendarButtonRef.current!,
                placement: 'bottom-start',
                modifiers: [
                  {
                    name: 'preventOverflow',
                    options: { altAxis: true, padding: 8 },
                  },
                ],
              },
              // Only offer Clear when there's a selection to clear.
              actionBar: {
                actions: pickerValue
                  ? ['clear', 'cancel', 'accept']
                  : ['cancel', 'accept'],
              },
            }}
          />
        </LocalizationProvider>
      </Box>

      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Tooltip title="Search transcripts">
          <IconButton
            aria-label="search"
            size="small"
            onClick={handleSearchOpen}
            color={searchQuery ? 'primary' : 'default'}
            sx={{ width: CONTROL_HEIGHT, height: CONTROL_HEIGHT }}
          >
            <SearchIcon />
          </IconButton>
        </Tooltip>
        <Popover
          open={Boolean(searchAnchorEl)}
          anchorEl={searchAnchorEl}
          onClose={handleSearchClose}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
          transformOrigin={{ vertical: 'top', horizontal: 'left' }}
        >
          <Box
            sx={{
              p: 2,
              display: 'flex',
              flexDirection: 'column',
              gap: 1.5,
              width: 280,
            }}
          >
            <TextField
              autoFocus
              size="small"
              placeholder="Search transcripts…"
              value={localSearchQuery}
              onChange={(e) => setLocalSearchQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleSearchApply();
              }}
              slotProps={{
                input: {
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon fontSize="small" />
                    </InputAdornment>
                  ),
                },
              }}
            />
            <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 1 }}>
              <Button
                size="small"
                onClick={handleSearchClear}
                sx={{ textTransform: 'none' }}
              >
                Clear
              </Button>
              <Button
                size="small"
                variant="contained"
                onClick={handleSearchApply}
                sx={{ textTransform: 'none' }}
              >
                Apply
              </Button>
            </Box>
          </Box>
        </Popover>
        <FormControl size="small" sx={{ minWidth: 120 }}>
          <Select
            value={alertFilter}
            onChange={(e) => setAlertFilter(e.target.value as AlertFilter)}
            inputProps={{ 'aria-label': 'Transcript filter' }}
            sx={{
              height: CONTROL_HEIGHT,
              borderRadius: CONTROL_RADIUS,
              fontSize: CONTROL_FONT_SIZE,
              '& .MuiSelect-select': {
                py: 0,
                display: 'flex',
                alignItems: 'center',
              },
            }}
          >
            <MenuItem value="all" sx={{ fontSize: CONTROL_FONT_SIZE }}>
              All transcripts
            </MenuItem>
            <MenuItem value="alerts" sx={{ fontSize: CONTROL_FONT_SIZE }}>
              Alerts only
            </MenuItem>
          </Select>
        </FormControl>
        <FormControlLabel
          control={
            <Switch
              checked={redactTranscripts}
              onChange={(e) => setRedactTranscripts(e.target.checked)}
              size="small"
            />
          }
          label="Redact transcripts"
          slotProps={{ typography: { sx: { fontSize: CONTROL_FONT_SIZE } } }}
          sx={{ ml: 0, mr: 0 }}
        />
      </Box>
    </Box>
  );
};

export default TranscriptActionsBar;
