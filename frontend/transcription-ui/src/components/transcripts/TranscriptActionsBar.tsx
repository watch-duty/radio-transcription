import React from 'react';

import CalendarMonthIcon from '@mui/icons-material/CalendarMonth';
import ClearIcon from '@mui/icons-material/Clear';
import TuneIcon from '@mui/icons-material/Tune';
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
import { useIsNarrow } from '../../hooks/useIsNarrow';
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

// Fixed width sized to hold the placeholder plus the clear button.
const SEARCH_WIDTH = 150;

// Wait for a typing pause before committing the query so we don't refetch on
// every keystroke.
const SEARCH_DEBOUNCE_MS = 300;

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
  children: React.ReactNode;
}

// Clear/Cancel/Apply footer around the date filter's draft picker.
function DraftFilterPopover({
  anchorEl,
  onCancel,
  onClear,
  onApply,
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

  const [searchDraft, setSearchDraft] = React.useState(searchQuery);
  // Mirror external commits into the draft during render, not in an effect.
  const [prevSearchQuery, setPrevSearchQuery] = React.useState(searchQuery);
  if (searchQuery !== prevSearchQuery) {
    setPrevSearchQuery(searchQuery);
    setSearchDraft(searchQuery);
  }

  // Commit the draft after a typing pause; Enter/Clear commit immediately below.
  React.useEffect(() => {
    if (searchDraft === searchQuery) return;
    const id = setTimeout(
      () => setSearchQuery(searchDraft),
      SEARCH_DEBOUNCE_MS
    );
    return () => clearTimeout(id);
  }, [searchDraft, searchQuery, setSearchQuery]);

  const clearSearch = () => {
    setSearchDraft('');
    if (searchQuery) setSearchQuery('');
  };

  // Narrow: collapse search/filter/redact into a settings popover so the row fits.
  const isNarrow = useIsNarrow();
  const [controlsAnchor, setControlsAnchor] =
    React.useState<HTMLElement | null>(null);
  const hasActiveControls =
    alertFilter !== 'all' || redactTranscripts || Boolean(searchQuery);

  const renderSearchField = (inPopover: boolean) => (
    <TextField
      size="small"
      fullWidth={inPopover}
      placeholder="Search transcripts…"
      value={searchDraft}
      onChange={(e) => setSearchDraft(e.target.value)}
      onKeyDown={(e) => {
        if (e.key === 'Enter') setSearchQuery(searchDraft);
      }}
      sx={{
        ...(inPopover ? {} : { width: SEARCH_WIDTH, flexShrink: 0 }),
        '& .MuiOutlinedInput-root': {
          height: CONTROL_HEIGHT,
          borderRadius: CONTROL_RADIUS,
          fontSize: CONTROL_FONT_SIZE,
        },
      }}
      slotProps={{
        input: {
          'aria-label': 'Search transcripts',
          endAdornment: searchDraft ? (
            <InputAdornment position="end">
              <IconButton
                aria-label="clear search"
                size="small"
                edge="end"
                onClick={clearSearch}
              >
                <ClearIcon fontSize="small" />
              </IconButton>
            </InputAdornment>
          ) : null,
        },
      }}
    />
  );

  const redactControl = (
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
      sx={{ ml: 0, mr: 0, whiteSpace: 'nowrap', flexShrink: 0 }}
    />
  );

  const filterSelect = (fullWidth: boolean) => (
    <FormControl
      size="small"
      fullWidth={fullWidth}
      sx={{ flexShrink: 0, minWidth: fullWidth ? undefined : 110 }}
    >
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
  );

  return (
    <Box
      sx={{
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: 'center',
        gap: 0.75,
        mb: 0.5,
        // Lift the bar (and its overflowing speaker badges) above the list's
        // sticky headers (zIndex 1) so they aren't clipped behind them.
        position: 'relative',
        zIndex: 2,
      }}
    >
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
          whiteSpace: 'nowrap',
          flexShrink: 0,
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
            flexShrink: 0,
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

      {isNarrow ? (
        <>
          <Tooltip title="Transcript settings">
            <IconButton
              aria-label="Transcript settings"
              size="small"
              onClick={(e) => setControlsAnchor(e.currentTarget)}
              sx={{
                width: CONTROL_HEIGHT,
                height: CONTROL_HEIGHT,
                flexShrink: 0,
                ml: 'auto',
                ...(hasActiveControls && ACTIVE_FILTER_SX),
              }}
            >
              <TuneIcon />
            </IconButton>
          </Tooltip>
          <Popover
            // Guard against a stale anchor retained across a breakpoint switch:
            // the stored button unmounts when the bar goes wide, so only open
            // while it is still in the document.
            open={Boolean(controlsAnchor?.isConnected)}
            anchorEl={controlsAnchor}
            onClose={() => setControlsAnchor(null)}
            anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
            transformOrigin={{ vertical: 'top', horizontal: 'right' }}
          >
            <Box
              sx={{
                p: 2,
                display: 'flex',
                flexDirection: 'column',
                gap: 1.5,
                width: 260,
              }}
            >
              {renderSearchField(true)}
              {filterSelect(true)}
              {redactControl}
            </Box>
          </Popover>
        </>
      ) : (
        <Box
          sx={{
            display: 'flex',
            flexWrap: 'wrap',
            alignItems: 'center',
            justifyContent: 'flex-end',
            gap: 0.75,
            ml: 'auto',
          }}
        >
          {renderSearchField(false)}
          {filterSelect(false)}
          {redactControl}
        </Box>
      )}
    </Box>
  );
};

export default TranscriptActionsBar;
