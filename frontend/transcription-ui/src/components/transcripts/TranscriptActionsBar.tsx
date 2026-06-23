import React, { useState } from 'react';

import RestartAltIcon from '@mui/icons-material/RestartAlt';
import FilterIcon from '@mui/icons-material/Tune';
import VolumeDownIcon from '@mui/icons-material/VolumeDown';
import VolumeOffIcon from '@mui/icons-material/VolumeOff';
import VolumeUpIcon from '@mui/icons-material/VolumeUp';
import { FormControl, InputLabel } from '@mui/material';
import Badge from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Chip from '@mui/material/Chip';
import FormControlLabel from '@mui/material/FormControlLabel';
import IconButton from '@mui/material/IconButton';
import MenuItem from '@mui/material/MenuItem';
import Popover from '@mui/material/Popover';
import Select from '@mui/material/Select';
import Slider from '@mui/material/Slider';
import Switch from '@mui/material/Switch';
import ToggleButton from '@mui/material/ToggleButton';
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';

import {
  DEFAULT_PAN,
  DEFAULT_SPEED,
  DEFAULT_VOLUME_DB,
  PAN_OPTIONS,
  SPEED_OPTIONS,
  VOLUME_MAX_DB,
  VOLUME_MIN_DB,
  formatVolumeDb,
  snapVolumeToDefault,
} from '../../audio/WebAudioPlayer';
import type { AlertFilter } from '../../hooks/useAudioSegments';
import { isSafari } from '../../utils/browser';
import { DateTimePicker } from '../common/DateTimePicker';

const PAN_LABELS: Record<number, string> = { '-1': 'L', '0': 'C', '1': 'R' };

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
  volumeDb: number;
  setVolumeDb: (db: number) => void;
  pan: number;
  setPan: (pan: number) => void;
  speed: number;
  setSpeed: (speed: number) => void;
}

const APPLIED_FILTER_BG_COLOR = '#bbdefb';
const DEFAULT_FILTER_BG_COLOR = '#f9bf90';

export const TranscriptActionsBar: React.FC<TranscriptActionsBarProps> = ({
  hasNewerAudioSegments,
  redactTranscripts,
  setRedactTranscripts,
  dateTime,
  setDateTime,
  alertFilter,
  setAlertFilter,
  onClickViewLatest,
  volumeDb,
  setVolumeDb,
  pan,
  setPan,
  speed,
  setSpeed,
}) => {
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';

  const [filterAnchorEl, setFilterAnchorEl] = useState<HTMLElement | null>(
    null
  );
  const [audioAnchorEl, setAudioAnchorEl] = useState<HTMLElement | null>(null);
  const speedDisabled = isSafari();

  const volumeLabel = formatVolumeDb(volumeDb);
  const isMuted = volumeLabel === 'Muted';
  const volumeActive = volumeDb !== DEFAULT_VOLUME_DB;
  const panLabel = pan !== DEFAULT_PAN ? PAN_LABELS[pan] : null;
  const speedActive = speed !== DEFAULT_SPEED;
  const VolumeIcon = isMuted
    ? VolumeOffIcon
    : volumeDb < 0
      ? VolumeDownIcon
      : VolumeUpIcon;

  const activeSummary = [
    volumeActive ? volumeLabel : null,
    panLabel ? `Pan ${panLabel}` : null,
    speedActive ? `${speed}×` : null,
  ].filter(Boolean);
  const audioTooltip = activeSummary.length
    ? `Audio controls — ${activeSummary.join(', ')}`
    : 'Audio controls';
  const audioBadgeSx = {
    '& .MuiBadge-badge': {
      fontSize: '0.625rem',
      height: 16,
      minWidth: 16,
      px: 0.5,
      boxShadow: `0 0 0 2px ${theme.palette.background.paper}`,
    },
  };
  const audioButtonSx = {
    minWidth: 0,
    p: 0.75,
    ...(volumeActive
      ? {}
      : {
          bgcolor: 'background.paper',
          color: 'primary.main',
          '&:hover': { bgcolor: 'action.hover' },
        }),
  };
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

  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'space-between',
        mb: 0.5,
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Tooltip title={audioTooltip}>
          <Badge
            color="primary"
            badgeContent={speedActive ? `${speed}×` : undefined}
            invisible={!speedActive}
            anchorOrigin={{ vertical: 'top', horizontal: 'left' }}
            sx={audioBadgeSx}
          >
            <Badge
              color="primary"
              badgeContent={panLabel}
              invisible={!panLabel}
              sx={audioBadgeSx}
            >
              <Button
                color="primary"
                variant="contained"
                sx={audioButtonSx}
                aria-label="audio controls"
                onClick={(e) => setAudioAnchorEl(e.currentTarget)}
              >
                <VolumeIcon />
              </Button>
            </Badge>
          </Badge>
        </Tooltip>
        <Popover
          open={Boolean(audioAnchorEl)}
          anchorEl={audioAnchorEl}
          onClose={() => setAudioAnchorEl(null)}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
          transformOrigin={{ vertical: 'top', horizontal: 'left' }}
          sx={{ zIndex: 1300 }}
        >
          <Box
            sx={{
              p: 1.5,
              display: 'flex',
              flexDirection: 'column',
              gap: 1.25,
              width: 210,
            }}
          >
            <Box>
              <Box
                sx={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                }}
              >
                <Typography variant="subtitle2">Volume</Typography>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                  <Typography variant="caption" color="text.secondary">
                    {formatVolumeDb(volumeDb)}
                  </Typography>
                  {volumeActive && (
                    <Tooltip title="Reset to default">
                      <IconButton
                        size="small"
                        aria-label="Reset volume"
                        onClick={() => setVolumeDb(DEFAULT_VOLUME_DB)}
                        sx={{ p: 0.25 }}
                      >
                        <RestartAltIcon sx={{ fontSize: 16 }} />
                      </IconButton>
                    </Tooltip>
                  )}
                </Box>
              </Box>
              <Slider
                aria-label="Volume"
                size="small"
                value={volumeDb}
                min={VOLUME_MIN_DB}
                max={VOLUME_MAX_DB}
                step={1}
                marks={[{ value: 0 }]}
                onChange={(event, value) => {
                  const db = value as number;
                  // Snap to the default while dragging, but leave keyboard
                  // steps exact so every dB stays reachable with the arrows.
                  setVolumeDb(
                    event.type === 'keydown' ? db : snapVolumeToDefault(db)
                  );
                }}
                sx={{ display: 'block', mt: 0.5, mb: 0, py: 0.5 }}
              />
            </Box>

            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
              <Typography variant="subtitle2">Pan</Typography>
              <ToggleButtonGroup
                exclusive
                fullWidth
                size="small"
                value={pan}
                onChange={(_, value) => {
                  if (value !== null) setPan(value as number);
                }}
              >
                {PAN_OPTIONS.map((option) => (
                  <ToggleButton
                    key={option}
                    value={option}
                    aria-label={`Pan ${PAN_LABELS[option]}`}
                  >
                    {PAN_LABELS[option]}
                  </ToggleButton>
                ))}
              </ToggleButtonGroup>
            </Box>

            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
              <Typography variant="subtitle2">Speed</Typography>
              <Tooltip
                title={
                  speedDisabled ? 'Speed control is unavailable in Safari' : ''
                }
              >
                <ToggleButtonGroup
                  exclusive
                  fullWidth
                  size="small"
                  value={speed}
                  disabled={speedDisabled}
                  onChange={(_, value) => {
                    if (value !== null) setSpeed(value as number);
                  }}
                  sx={{ '& .MuiToggleButton-root': { px: 0.5, fontSize: 12 } }}
                >
                  {SPEED_OPTIONS.map((option) => (
                    <ToggleButton
                      key={option}
                      value={option}
                      aria-label={`Speed ${option}x`}
                    >
                      {option}×
                    </ToggleButton>
                  ))}
                </ToggleButtonGroup>
              </Tooltip>
            </Box>
          </Box>
        </Popover>

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
