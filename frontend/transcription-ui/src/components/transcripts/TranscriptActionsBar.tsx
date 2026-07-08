import React, { useState } from 'react';

import ClearIcon from '@mui/icons-material/Clear';
import SearchIcon from '@mui/icons-material/Search';
import FilterIcon from '@mui/icons-material/Tune';
import { FormControl, InputLabel } from '@mui/material';
import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Chip from '@mui/material/Chip';
import Divider from '@mui/material/Divider';
import FormControlLabel from '@mui/material/FormControlLabel';
import IconButton from '@mui/material/IconButton';
import InputAdornment from '@mui/material/InputAdornment';
import MenuItem from '@mui/material/MenuItem';
import Popover from '@mui/material/Popover';
import Select from '@mui/material/Select';
import Switch from '@mui/material/Switch';
import TextField from '@mui/material/TextField';
import Tooltip from '@mui/material/Tooltip';
import { useTheme } from '@mui/material/styles';

import type { AlertFilter } from '../../hooks/useAudioSegments';
import { DateTimePicker } from '../common/DateTimePicker';

export interface TranscriptActionsBarProps {
  hasNewerAudioSegments: boolean;
  searchedTimestamp: Date | null;
  // True when the window is at the newest loaded audio. Keeps "Jump to live"
  // actionable after scrubbing back, even with no newer segments left to load.
  isLatestTimeWindow?: boolean;
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

const APPLIED_FILTER_BG_COLOR = '#bbdefb';
const DEFAULT_FILTER_BG_COLOR = '#f9bf90';

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  hasNewerAudioSegments,
  isLatestTimeWindow = true,
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
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';

  const [filterAnchorEl, setFilterAnchorEl] = useState<HTMLElement | null>(
    null
  );
  const [localDateTime, setLocalDateTime] = useState<Date | null>(dateTime);
  const [localAlertFilter, setLocalAlertFilter] =
    useState<AlertFilter>(alertFilter);
  const [localSearchQuery, setLocalSearchQuery] = useState<string>(searchQuery);

  const [prevDateTime, setPrevDateTime] = useState<Date | null>(dateTime);
  const [prevAlertFilter, setPrevAlertFilter] =
    useState<AlertFilter>(alertFilter);
  const [prevSearchQuery, setPrevSearchQuery] = useState<string>(searchQuery);

  if (dateTime !== prevDateTime) {
    setPrevDateTime(dateTime);
    setLocalDateTime(dateTime);
  }
  if (alertFilter !== prevAlertFilter) {
    setPrevAlertFilter(alertFilter);
    setLocalAlertFilter(alertFilter);
  }
  if (searchQuery !== prevSearchQuery) {
    setPrevSearchQuery(searchQuery);
    setLocalSearchQuery(searchQuery);
  }

  const handleFilterOpen = (event: React.MouseEvent<HTMLElement>) => {
    setFilterAnchorEl(event.currentTarget);
    setLocalDateTime(dateTime);
    setLocalAlertFilter(alertFilter);
    setLocalSearchQuery(searchQuery);
  };

  const handleFilterClose = () => {
    setFilterAnchorEl(null);
    setLocalDateTime(dateTime);
    setLocalAlertFilter(alertFilter);
    setLocalSearchQuery(searchQuery);
  };

  const handleFilterApply = () => {
    setDateTime(localDateTime);
    setAlertFilter(localAlertFilter);
    setSearchQuery(localSearchQuery);
    setFilterAnchorEl(null);
  };

  const handleFilterClear = () => {
    setLocalDateTime(null);
    setLocalAlertFilter('all');
    setLocalSearchQuery('');
  };

  const handleDeleteDateTime = () => {
    setDateTime(null);
  };

  const handleDeleteAlertFilter = () => {
    setAlertFilter('all');
  };

  const handleDeleteSearchQuery = () => {
    setSearchQuery('');
  };

  let badgeContent = 0;
  if (dateTime) {
    badgeContent++;
  }
  if (alertFilter === 'alerts') {
    badgeContent++;
  }
  if (searchQuery) {
    badgeContent++;
  }

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
            <TextField
              size="small"
              placeholder="Search transcripts..."
              value={localSearchQuery}
              onChange={(e) => setLocalSearchQuery(e.target.value)}
              slotProps={{
                input: {
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon fontSize="small" />
                    </InputAdornment>
                  ),
                  endAdornment: localSearchQuery ? (
                    <InputAdornment position="end">
                      <IconButton
                        data-testid="clear-search-button"
                        size="small"
                        onClick={() => setLocalSearchQuery('')}
                        edge="end"
                      >
                        <ClearIcon fontSize="small" />
                      </IconButton>
                    </InputAdornment>
                  ) : null,
                },
              }}
              sx={{ width: 220 }}
            />
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

            <Divider />

            <Box
              sx={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <Button
                size="small"
                variant="outlined"
                onClick={handleFilterClear}
                sx={{ textTransform: 'none' }}
              >
                Clear
              </Button>
              <Box sx={{ display: 'flex', gap: 1 }}>
                <Button
                  size="small"
                  variant="outlined"
                  onClick={handleFilterClose}
                  sx={{ textTransform: 'none' }}
                >
                  Cancel
                </Button>
                <Button
                  size="small"
                  variant="contained"
                  color="primary"
                  onClick={handleFilterApply}
                  sx={{ textTransform: 'none' }}
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
                  backgroundColor: dateTime
                    ? theme.palette.primary.main
                    : '#f9bf90',
                  color: 'black',
                  '& .MuiChip-deleteIcon': {
                    color: 'black',
                  },
                }
              : {
                  backgroundColor: dateTime
                    ? APPLIED_FILTER_BG_COLOR
                    : DEFAULT_FILTER_BG_COLOR,
                  color: 'black',
                }
          }
          label={
            dateTime ? (
              <Box>
                <b>Date/time:</b>{' '}
                {`${dateTime.toLocaleDateString()} ${dateTime.toLocaleTimeString()}`}
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
        {searchQuery && (
          <Chip
            sx={{
              backgroundColor: APPLIED_FILTER_BG_COLOR,
              color: 'black',
            }}
            label={
              <Box>
                <b>Search:</b> {searchQuery}
              </Box>
            }
            variant="filled"
            size="small"
            onDelete={handleDeleteSearchQuery}
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
