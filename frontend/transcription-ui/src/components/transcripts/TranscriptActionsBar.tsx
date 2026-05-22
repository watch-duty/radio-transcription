import React from 'react';

import FilterIcon from '@mui/icons-material/Tune';
import { Typography } from '@mui/material';
import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Chip from '@mui/material/Chip';
import FormControlLabel from '@mui/material/FormControlLabel';
import Popover from '@mui/material/Popover';
import Switch from '@mui/material/Switch';
import Tooltip from '@mui/material/Tooltip';

import { DateTimePicker } from '../common/DateTimePicker';
import { useTheme } from '@mui/material/styles';

export interface TranscriptActionsBarProps {
  hasNewerTranscripts: boolean;
  searchedTimestamp: Date | null;
  redactTranscripts: boolean;
  setRedactTranscripts: (redact: boolean) => void;
  dateTime: Date | null;
  setDateTime: (dateTime: Date | null) => void;
  onClickViewLatest: () => void;
}

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  hasNewerTranscripts,
  redactTranscripts,
  setRedactTranscripts,
  dateTime,
  setDateTime,
  onClickViewLatest,
}) => {
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';

  const [filterAnchorEl, setFilterAnchorEl] =
    React.useState<HTMLElement | null>(null);
  const [localDateTime, setLocalDateTime] = React.useState<Date | null>(
    dateTime
  );

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

  const handleDeleteDateTime = () => {
    setDateTime(null);
  };

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
          disabled={!hasNewerTranscripts}
        >
          Jump to live
        </Button>

        <Tooltip title="Filter transcripts">
          <Badge
            color="primary"
            badgeContent={dateTime ? '1' : '0'}
            invisible={!dateTime}
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
                  backgroundColor: dateTime
                    ? theme.palette.primary.main
                    : '#f9bf90',
                  color: 'black',
                  '& .MuiChip-deleteIcon': {
                    color: 'black',
                  },
                }
              : {
                  backgroundColor: dateTime ? '#bbdefb' : '#f9bf90',
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
