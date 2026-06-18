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
import { DateTimePicker } from '../common/DateTimePicker';

export interface TranscriptActionsBarProps {
  hasNewerAudioSegments: boolean;
  searchedTimestamp: Date | null;
  redactTranscripts: boolean;
  setRedactTranscripts: (redact: boolean) => void;
  dateTime: Date | null;
  setDateTime: (dateTime: Date | null) => void;
  alertFilter: AlertFilter;
  setAlertFilter: (alertFilter: AlertFilter) => void;
  onClickViewLatest: () => void;
}

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  hasNewerAudioSegments,
  redactTranscripts,
  setRedactTranscripts,
  dateTime,
  setDateTime,
  alertFilter,
  setAlertFilter,
  onClickViewLatest,
}) => {
  const theme = useTheme();
  const [filterAnchorEl, setFilterAnchorEl] = useState<HTMLElement | null>(
    null
  );
  const [localDateTime, setLocalDateTime] = useState<Date | null>(dateTime);
  const [localAlertFilter, setLocalAlertFilter] = useState<AlertFilter>('all');

  React.useEffect(() => {
    setLocalDateTime(dateTime);
    setLocalAlertFilter(alertFilter);
  }, [dateTime, alertFilter]);

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

  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'space-between',
        mb: 0.5,
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Button
          variant="contained"
          sx={{ textTransform: 'none', gap: 1 }}
          onClick={onClickViewLatest}
          disabled={!hasNewerAudioSegments}
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
              {
              backgroundColor: dateTime
                ? theme.palette.warning.light
                : theme.palette.warning.dark,
              color: theme.palette.warning.contrastText,
              '& .MuiChip-deleteIcon': {
                color: theme.palette.warning.contrastText,
              },
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
              backgroundColor: theme.palette.warning.light,
              color: theme.palette.warning.contrastText,
              '& .MuiChip-deleteIcon': {
                color: theme.palette.warning.contrastText,
              },
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
