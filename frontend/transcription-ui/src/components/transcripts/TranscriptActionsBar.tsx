import React, { useState } from 'react';

import FilterIcon from '@mui/icons-material/Tune';
import { FormControl, InputLabel } from '@mui/material';
import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Chip from '@mui/material/Chip';
import FormControlLabel from '@mui/material/FormControlLabel';
import MenuItem from '@mui/material/MenuItem';
import Popover from '@mui/material/Popover';
import Select from '@mui/material/Select';
import Switch from '@mui/material/Switch';
import Tooltip from '@mui/material/Tooltip';
import { useTheme } from '@mui/material/styles';

import type { AlertFilter } from '../../hooks/useAudioSegments';
import { formatClockTime } from '../../utils/timeUtils';
import { DateTimePicker } from '../common/DateTimePicker';

export interface TranscriptActionsBarProps {
  hasNewerAudioSegments: boolean;
  // True when the audio window is at the most recent loaded audio.
  isLatestTimeWindow?: boolean;
  // The window's right edge (ms) when viewing back, shown in the chip; null when latest.
  activeWindowTime?: number | null;
  redactTranscripts: boolean;
  setRedactTranscripts: (redact: boolean) => void;
  dateTime: Date | null;
  setDateTime: (dateTime: Date | null) => void;
  alertFilter: AlertFilter;
  setAlertFilter: (alertFilter: AlertFilter) => void;
  onClickViewLatest: () => void;
}

const APPLIED_FILTER_BG_COLOR = '#bbdefb';
const DEFAULT_FILTER_BG_COLOR = '#f9bf90';

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  hasNewerAudioSegments,
  isLatestTimeWindow = true,
  activeWindowTime = null,
  redactTranscripts,
  setRedactTranscripts,
  dateTime,
  setDateTime,
  alertFilter,
  setAlertFilter,
  onClickViewLatest,
}) => {
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';

  const [filterAnchorEl, setFilterAnchorEl] = useState<HTMLElement | null>(
    null
  );
  const [localDateTime, setLocalDateTime] = useState<Date | null>(dateTime);
  const [localAlertFilter, setLocalAlertFilter] =
    useState<AlertFilter>(alertFilter);

  const [prevDateTime, setPrevDateTime] = useState<Date | null>(dateTime);
  const [prevAlertFilter, setPrevAlertFilter] =
    useState<AlertFilter>(alertFilter);

  if (dateTime !== prevDateTime) {
    setPrevDateTime(dateTime);
    setLocalDateTime(dateTime);
  }
  if (alertFilter !== prevAlertFilter) {
    setPrevAlertFilter(alertFilter);
    setLocalAlertFilter(alertFilter);
  }

  const handleFilterOpen = (event: React.MouseEvent<HTMLElement>) => {
    setFilterAnchorEl(event.currentTarget);
    setLocalDateTime(dateTime);
    setLocalAlertFilter(alertFilter);
  };

  const handleFilterClose = () => {
    setFilterAnchorEl(null);
    setLocalDateTime(dateTime);
    setLocalAlertFilter(alertFilter);
  };

  const handleFilterApply = () => {
    setDateTime(localDateTime);
    setAlertFilter(localAlertFilter);
    setFilterAnchorEl(null);
  };

  const handleFilterClear = () => {
    setLocalDateTime(null);
    setLocalAlertFilter('all');
  };

  const handleDeleteDateTime = () => {
    setDateTime(null);
  };

  const handleDeleteAlertFilter = () => {
    setAlertFilter('all');
  };

  let badgeContent = 0;
  if (dateTime) {
    badgeContent++;
  }
  if (alertFilter === 'alerts') {
    badgeContent++;
  }

  const formatDateClock = (ms: number) =>
    `${new Date(ms).toLocaleDateString()} ${formatClockTime(ms)}`;
  const dateTimeLabel = dateTime ? formatDateClock(dateTime.getTime()) : null;
  // A past window (without a date filter) shows where you're viewing.
  const pastWindowLabel =
    !dateTime && activeWindowTime != null
      ? formatDateClock(activeWindowTime)
      : null;
  const isViewingPast = Boolean(dateTimeLabel || pastWindowLabel);

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
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Button
          variant="contained"
          sx={{ textTransform: 'none', gap: 1 }}
          onClick={onClickViewLatest}
          disabled={!hasNewerAudioSegments && isLatestTimeWindow && !dateTime}
        >
          Jump to live
        </Button>

        <Tooltip title="Filter transcripts">
          <Badge
            color="primary"
            badgeContent={badgeContent}
            invisible={badgeContent === 0}
          >
            <Button
              color="primary"
              variant="outlined"
              sx={{
                minWidth: 0,
                p: 0.75,
                textTransform: 'none',
                display: 'flex',
                gap: 1,
              }}
              aria-label="filter"
              onClick={handleFilterOpen}
            >
              <FilterIcon />
              Filters
            </Button>
          </Badge>
        </Tooltip>
        <Popover
          open={Boolean(filterAnchorEl)}
          anchorEl={filterAnchorEl}
          onClose={handleFilterClose}
          anchorOrigin={{
            vertical: 'bottom',
            horizontal: 'left',
          }}
          transformOrigin={{
            vertical: 'top',
            horizontal: 'left',
          }}
          sx={{ zIndex: 1300 }}
        >
          <Box sx={{ p: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <DateTimePicker
              label="Date/time"
              dateTime={localDateTime}
              setDateTime={setLocalDateTime}
            />
            <FormControl>
              <InputLabel id="filter-alerts-label">Show</InputLabel>
              <Select
                labelId="filter-alerts-label"
                size="small"
                value={localAlertFilter}
                onChange={(e) => {
                  const newFilter = e.target.value as AlertFilter;
                  setLocalAlertFilter(newFilter);
                }}
                label="Show"
              >
                <MenuItem value="all">All transcripts</MenuItem>
                <MenuItem value="alerts">Alerts only</MenuItem>
              </Select>
            </FormControl>
            <Box
              sx={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <Button size="small" onClick={handleFilterClear}>
                Clear
              </Button>
              <Box sx={{ display: 'flex', gap: 1 }}>
                <Button size="small" onClick={handleFilterClose}>
                  Cancel
                </Button>
                <Button
                  size="small"
                  variant="contained"
                  color="primary"
                  onClick={handleFilterApply}
                >
                  Apply
                </Button>
              </Box>
            </Box>
          </Box>
        </Popover>

        <Chip
          sx={
            isDarkTheme
              ? {
                  backgroundColor: isViewingPast
                    ? theme.palette.primary.main
                    : '#f9bf90',
                  color: 'black',
                  '& .MuiChip-deleteIcon': {
                    color: 'black',
                  },
                }
              : {
                  backgroundColor: isViewingPast
                    ? APPLIED_FILTER_BG_COLOR
                    : DEFAULT_FILTER_BG_COLOR,
                  color: 'black',
                }
          }
          label={
            dateTimeLabel ? (
              <Box>
                <b>Date/time:</b> {dateTimeLabel}
              </Box>
            ) : pastWindowLabel ? (
              <Box>
                <b>Viewing:</b> {pastWindowLabel}
              </Box>
            ) : (
              <Box>
                <b>Date/time:</b> Viewing live
              </Box>
            )
          }
          variant="filled"
          size="small"
          onDelete={dateTime ? handleDeleteDateTime : undefined}
        />
        {alertFilter === 'alerts' && (
          <Chip
            sx={{
              backgroundColor: APPLIED_FILTER_BG_COLOR,
              color: 'black',
            }}
            label={
              <Box>
                <b>Show:</b> Alerts only
              </Box>
            }
            variant="filled"
            size="small"
            onDelete={handleDeleteAlertFilter}
          />
        )}
      </Box>

      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <FormControlLabel
          control={
            <Switch
              checked={redactTranscripts}
              onChange={(e) => setRedactTranscripts(e.target.checked)}
              size="small"
            />
          }
          label="Redact transcripts"
          slotProps={{ typography: { variant: 'body2' } }}
          sx={{ ml: 0, mr: 0 }}
        />
      </Box>
    </Box>
  );
};

export default TranscriptActionsBar;
