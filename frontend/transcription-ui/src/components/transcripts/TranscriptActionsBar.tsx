import React from 'react';

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

import type { AlertFilter } from '../../hooks/useAudioSegments';
import { useDraftPopover } from '../../hooks/useDraftPopover';
import { DateTimePicker } from '../common/DateTimePicker';

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

// Shared height, radius, and font so the live control, calendar button, filter
// dropdown, and redact toggle read as one toolbar.
const CONTROL_HEIGHT = 32;
const CONTROL_RADIUS = 1;
const CONTROL_FONT_SIZE = '0.8125rem';
const FOOTER_BUTTON_SX = { textTransform: 'none' } as const;

// Filled rounded-rect for an active filter button — reads as "on" and cues it's clearable.
const ACTIVE_FILTER_SX = {
  borderRadius: 1,
  bgcolor: 'primary.main',
  color: 'primary.contrastText',
  '&:hover': { bgcolor: 'primary.dark' },
} as const;

interface DraftFilterPopoverProps {
  anchorEl: HTMLElement | null;
  onCancel: () => void;
  onClear: () => void;
  onApply: () => void;
  // Focus the first input once the open transition finishes (autoFocus alone is
  // unreliable inside the portalled, transitioning popover).
  autoFocusInput?: boolean;
  children: React.ReactNode;
}

// Shared chrome for the search and date filter popovers.
function DraftFilterPopover({
  anchorEl,
  onCancel,
  onClear,
  onApply,
  autoFocusInput,
  children,
}: DraftFilterPopoverProps) {
  return (
    <Popover
      open={Boolean(anchorEl)}
      anchorEl={anchorEl}
      onClose={onCancel}
      // Don't restore focus to the trigger, or its active fill shows a focus ring.
      disableRestoreFocus
      anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
      transformOrigin={{ vertical: 'top', horizontal: 'left' }}
      slotProps={
        autoFocusInput
          ? {
              transition: {
                onEntered: (node: HTMLElement) =>
                  node.querySelector<HTMLElement>('input, textarea')?.focus(),
              },
            }
          : undefined
      }
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
        {children}
        <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 1 }}>
          <Button size="small" onClick={onClear} sx={FOOTER_BUTTON_SX}>
            Clear
          </Button>
          <Button size="small" onClick={onCancel} sx={FOOTER_BUTTON_SX}>
            Cancel
          </Button>
          <Button
            size="small"
            variant="contained"
            onClick={onApply}
            sx={FOOTER_BUTTON_SX}
          >
            Apply
          </Button>
        </Box>
      </Box>
    </Popover>
  );
}

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
  const dateFilter = useDraftPopover<Date | null>(
    dateTime,
    (value) => {
      // Ignore a partially-typed (invalid) date, and skip a no-op commit so the
      // parent's navigation side effects don't fire when nothing changed.
      if (value && Number.isNaN(value.getTime())) return;
      if ((value?.getTime() ?? null) !== (dateTime?.getTime() ?? null)) {
        setDateTime(value);
      }
    },
    null
  );

  const searchFilter = useDraftPopover<string>(searchQuery, setSearchQuery, '');

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
            aria-label="filter by date"
            size="small"
            onClick={dateFilter.open}
            sx={{
              width: CONTROL_HEIGHT,
              height: CONTROL_HEIGHT,
              ...(dateTime && ACTIVE_FILTER_SX),
            }}
          >
            <CalendarMonthIcon />
          </IconButton>
        </Tooltip>
        <DraftFilterPopover
          anchorEl={dateFilter.anchorEl}
          onCancel={dateFilter.cancel}
          onClear={dateFilter.clear}
          onApply={dateFilter.apply}
        >
          {/* Popover owns Clear/Cancel/Apply, so the picker shows only OK. */}
          <DateTimePicker
            label="Date/time"
            dateTime={dateFilter.draft}
            setDateTime={dateFilter.setDraft}
            actions={['accept']}
            width="100%"
          />
        </DraftFilterPopover>
      </Box>

      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Tooltip title="Search transcripts">
          <IconButton
            aria-label="search"
            size="small"
            onClick={searchFilter.open}
            sx={{
              width: CONTROL_HEIGHT,
              height: CONTROL_HEIGHT,
              ...(searchQuery && ACTIVE_FILTER_SX),
            }}
          >
            <SearchIcon />
          </IconButton>
        </Tooltip>
        <DraftFilterPopover
          anchorEl={searchFilter.anchorEl}
          onCancel={searchFilter.cancel}
          onClear={searchFilter.clear}
          onApply={searchFilter.apply}
          autoFocusInput
        >
          <TextField
            size="small"
            placeholder="Search transcripts…"
            value={searchFilter.draft}
            onChange={(e) => searchFilter.setDraft(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') searchFilter.apply();
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
        </DraftFilterPopover>
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
