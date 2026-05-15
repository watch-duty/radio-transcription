import React from 'react';

import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import ArrowDropUpIcon from '@mui/icons-material/ArrowDropUp';
import SyncIcon from '@mui/icons-material/Sync';
import { Badge, Tooltip } from '@mui/material';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import ButtonGroup from '@mui/material/ButtonGroup';
import CircularProgress from '@mui/material/CircularProgress';
import ClickAwayListener from '@mui/material/ClickAwayListener';
import FormControlLabel from '@mui/material/FormControlLabel';
import Grow from '@mui/material/Grow';
import MenuItem from '@mui/material/MenuItem';
import MenuList from '@mui/material/MenuList';
import Paper from '@mui/material/Paper';
import Popover from '@mui/material/Popover';
import Popper from '@mui/material/Popper';
import Switch from '@mui/material/Switch';
import FilterIcon from '@mui/icons-material/FilterList';

import { DateTimePicker } from '../common/DateTimePicker';

export interface TranscriptActionsBarProps {
  hasNewerTranscripts: boolean;
  searchedTimestamp: Date | null;
  isTranscriptsFetching: boolean;
  isTranscriptsPolling: boolean;
  refreshInterval: number;
  setRefreshInterval: (interval: number) => void;
  onRefresh: () => Promise<void>;
  redactTranscripts: boolean;
  setRedactTranscripts: (redact: boolean) => void;
  dateTime: Date | null;
  setDateTime: (dateTime: Date | null) => void;
}

const refreshIntervalOptions = [
  { value: 5000, label: '5s' },
  { value: 10000, label: '10s' },
  { value: 15000, label: '15s' },
  { value: 30000, label: '30s' },
  { value: 60000, label: '1m' },
  { value: 300000, label: '5m' },
  { value: -1, label: 'Off' },
];

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  hasNewerTranscripts,
  isTranscriptsFetching,
  isTranscriptsPolling,
  refreshInterval,
  setRefreshInterval,
  onRefresh,
  redactTranscripts,
  setRedactTranscripts,
  dateTime,
  setDateTime,
}) => {
  const [anchorEl, setAnchorEl] = React.useState<HTMLElement | null>(null);
  const [refreshMenuOpen, setRefreshMenuOpen] = React.useState(false);

  const [filterAnchorEl, setFilterAnchorEl] = React.useState<HTMLElement | null>(null);
  const [localDateTime, setLocalDateTime] = React.useState<Date | null>(dateTime);

  React.useEffect(() => {
    setLocalDateTime(dateTime);
  }, [dateTime]);

  const handleFilterOpen = (event: React.MouseEvent<HTMLElement>) => {
    setFilterAnchorEl(event.currentTarget);
    setLocalDateTime(dateTime);
  };

  const handleFilterClose = () => {
    setFilterAnchorEl(null);
    setLocalDateTime(dateTime);
  };

  const handleFilterApply = () => {
    setDateTime(localDateTime);
    setFilterAnchorEl(null);
  };

  const handleFilterClear = () => {
    setLocalDateTime(null);
  };

  const buttonGroupRef = React.useCallback((node: HTMLElement | null) => {
    if (node !== null) {
      setAnchorEl(node);
    }
  }, []);

  const handleToggle = () => {
    setRefreshMenuOpen((prevOpen) => !prevOpen);
  };

  const handleClose = (event: Event) => {
    if (anchorEl && anchorEl.contains(event.target as HTMLElement)) {
      return;
    }

    setRefreshMenuOpen(false);
  };

  const currentRefreshLabel = refreshIntervalOptions.find(
    (option) => option.value === refreshInterval
  )?.label;

  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'space-between',
        mb: 0.5,
      }}
    >
      <Box>
        <Tooltip title="Filter transcripts">
          <Badge
            color='primary'
            badgeContent={dateTime ? '1' : '0'}
            invisible={!dateTime}
          >
            <Button
              color="primary"
              variant="outlined"
              sx={{ minWidth: 0, p: 0.75 }}
              aria-label="filter"
              onClick={handleFilterOpen}
            >
              <FilterIcon />
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
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
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
          sx={{
            '& .MuiFormControlLabel-label': {
              fontSize: '0.875rem',
              userSelect: 'none',
            },
          }}
        />

        {!hasNewerTranscripts && (
          <>
            <ButtonGroup variant="contained" ref={buttonGroupRef}>
              <Tooltip
                title={
                  !isTranscriptsPolling
                    ? 'Refresh transcripts'
                    : 'Refreshing transcripts...'
                }
              >
                <Button
                  size="small"
                  color="primary"
                  onClick={
                    isTranscriptsFetching || isTranscriptsPolling
                      ? undefined
                      : onRefresh
                  }
                  sx={{
                    ...(isTranscriptsFetching || isTranscriptsPolling
                      ? {
                          color: 'action.disabled',
                          bgcolor: 'action.disabledBackground',
                          boxShadow: 'none',
                          '&:hover': {
                            bgcolor: 'action.disabledBackground',
                          },
                        }
                      : {}),
                  }}
                  aria-label={isTranscriptsPolling ? 'refreshing' : 'refresh'}
                >
                  {isTranscriptsPolling ? (
                    <CircularProgress size={24} color="inherit" />
                  ) : (
                    <SyncIcon />
                  )}
                </Button>
              </Tooltip>
              <Tooltip title="Select refresh rate">
                <Button
                  size="small"
                  aria-controls={
                    refreshMenuOpen ? 'split-button-menu' : undefined
                  }
                  aria-expanded={refreshMenuOpen ? 'true' : undefined}
                  aria-label="select refresh interval"
                  aria-haspopup="menu"
                  onClick={handleToggle}
                  sx={{
                    textTransform: 'none',
                  }}
                >
                  {currentRefreshLabel}
                  {refreshMenuOpen ? (
                    <ArrowDropUpIcon />
                  ) : (
                    <ArrowDropDownIcon />
                  )}
                </Button>
              </Tooltip>
            </ButtonGroup>
            <Popper
              // The menu's z-index needs to be higher than the transcript view group headers.
              sx={{ zIndex: 1300 }}
              open={refreshMenuOpen}
              anchorEl={anchorEl}
              role={undefined}
              transition
              disablePortal
            >
              {({ TransitionProps, placement }) => (
                <Grow
                  {...TransitionProps}
                  style={{
                    transformOrigin:
                      placement === 'bottom' ? 'center top' : 'center bottom',
                  }}
                >
                  <Paper>
                    <ClickAwayListener onClickAway={handleClose}>
                      <MenuList id="split-button-menu" autoFocusItem>
                        {refreshIntervalOptions.map((option) => (
                          <MenuItem
                            key={option.value}
                            selected={option.value === refreshInterval}
                            onClick={() => {
                              setRefreshInterval(option.value);
                              setRefreshMenuOpen(false);
                            }}
                          >
                            {option.label}
                          </MenuItem>
                        ))}
                      </MenuList>
                    </ClickAwayListener>
                  </Paper>
                </Grow>
              )}
            </Popper>
          </>
        )}
      </Box>
    </Box>
  );
};

export default TranscriptActionsBar;
