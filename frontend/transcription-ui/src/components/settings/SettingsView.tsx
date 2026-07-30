import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link as RouterLink } from 'react-router';

import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import SettingsIcon from '@mui/icons-material/Settings';
import Autocomplete from '@mui/material/Autocomplete';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Divider from '@mui/material/Divider';
import FormControl from '@mui/material/FormControl';
import FormControlLabel from '@mui/material/FormControlLabel';
import IconButton from '@mui/material/IconButton';
import Link from '@mui/material/Link';
import MenuItem from '@mui/material/MenuItem';
import Paper from '@mui/material/Paper';
import Radio from '@mui/material/Radio';
import RadioGroup from '@mui/material/RadioGroup';
import Select from '@mui/material/Select';
import Slider from '@mui/material/Slider';
import Stack from '@mui/material/Stack';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';
import type { Feed } from '@transcription/common';

import {
  DEFAULT_PAN,
  DEFAULT_SPEED,
  DEFAULT_VOLUME_DB,
  PAN_OPTIONS,
  SPEED_OPTIONS,
  STORAGE_KEYS,
  VOLUME_MAX_DB,
  VOLUME_MIN_DB,
  feedKey,
} from '../../audio/audioSettings';
import { useAuth } from '../../context/AuthContext';
import { useFeeds } from '../../hooks/useFeeds';
import { handleListboxScroll } from '../../utils/scrollUtils';

export interface SettingsViewProps {
  triggerSnackbar?: (message: string) => void;
  onError?: (error: Error, titleMessage?: string) => void;
}

export type ThemeMode = 'system' | 'light' | 'dark';

export interface FeedAudioOverride {
  feedId: string;
  volumeDb: number;
  pan: number;
  speed: number;
}

function readStoredNumber(key: string): number | null {
  const raw = localStorage.getItem(key);
  if (raw === null) return null;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Scans localStorage for per-feed custom audio settings (volume, pan, speed).
 * Returns an array of FeedAudioOverride objects containing all feeds with stored overrides.
 */
function getStoredFeedOverrides(): FeedAudioOverride[] {
  const feedIdsSet = new Set<string>();

  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (!key) continue;

    const baseKeys = [
      STORAGE_KEYS.volumeDb,
      STORAGE_KEYS.pan,
      STORAGE_KEYS.speed,
    ];
    for (const base of baseKeys) {
      const prefix = `${base}.`;
      if (key.startsWith(prefix)) {
        const feedId = key.slice(prefix.length);
        if (feedId) {
          feedIdsSet.add(feedId);
        }
      }
    }
  }

  const globalVol =
    readStoredNumber(STORAGE_KEYS.volumeDb) ?? DEFAULT_VOLUME_DB;
  const globalSpeed = readStoredNumber(STORAGE_KEYS.speed) ?? DEFAULT_SPEED;

  const overrides: FeedAudioOverride[] = [];

  feedIdsSet.forEach((feedId) => {
    const vol =
      readStoredNumber(feedKey(STORAGE_KEYS.volumeDb, feedId)) ?? globalVol;
    const pan =
      readStoredNumber(feedKey(STORAGE_KEYS.pan, feedId)) ?? DEFAULT_PAN;
    const speed =
      readStoredNumber(feedKey(STORAGE_KEYS.speed, feedId)) ?? globalSpeed;

    overrides.push({
      feedId,
      volumeDb: Math.min(
        VOLUME_MAX_DB,
        Math.max(VOLUME_MIN_DB, Math.round(vol))
      ),
      pan: (PAN_OPTIONS as readonly number[]).includes(pan) ? pan : DEFAULT_PAN,
      speed: (SPEED_OPTIONS as readonly number[]).includes(speed)
        ? speed
        : DEFAULT_SPEED,
    });
  });

  return overrides.sort((a, b) => a.feedId.localeCompare(b.feedId));
}

export function SettingsView({ triggerSnackbar, onError }: SettingsViewProps) {
  const { token } = useAuth();

  // Mode state
  const [themeMode, setThemeModeState] = useState<ThemeMode>(() => {
    const stored = localStorage.getItem(STORAGE_KEYS.themeMode);
    if (stored === 'light' || stored === 'dark' || stored === 'system') {
      return stored;
    }
    return 'system';
  });

  // Global default audio settings state
  const [defaultVolumeDb, setDefaultVolumeDbState] = useState<number>(() => {
    return readStoredNumber(STORAGE_KEYS.volumeDb) ?? DEFAULT_VOLUME_DB;
  });

  const [defaultSpeed, setDefaultSpeedState] = useState<number>(() => {
    return readStoredNumber(STORAGE_KEYS.speed) ?? DEFAULT_SPEED;
  });

  // Per-feed audio overrides state
  const [feedOverrides, setFeedOverrides] = useState<FeedAudioOverride[]>(() =>
    getStoredFeedOverrides()
  );

  const [selectedFeedToAdd, setSelectedFeedToAdd] = useState<Feed | null>(null);
  const [feedSearchQuery, setFeedSearchQuery] = useState('');

  const {
    feeds,
    error: feedsError,
    hasNextPage,
    isFetchingNextPage,
    fetchNextPage,
  } = useFeeds({
    token,
    searchQuery: feedSearchQuery,
  });

  useEffect(() => {
    if (feedsError && onError) {
      onError(feedsError, 'Loading Feeds for Settings');
    }
  }, [feedsError, onError]);

  const feedNameMap = useMemo(() => {
    const map = new Map<string, string>();
    feeds.forEach((feed) => {
      map.set(feed.id, feed.name);
    });
    return map;
  }, [feeds]);

  const availableFeedsForNewOverride = useMemo(() => {
    const existingFeedIds = new Set(feedOverrides.map((o) => o.feedId));
    return feeds
      .filter((f) => !existingFeedIds.has(f.id))
      .sort((a, b) => (a.name || a.id).localeCompare(b.name || b.id));
  }, [feeds, feedOverrides]);

  // Handler for Theme Mode Change
  const handleThemeModeChange = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    const newMode = event.target.value as ThemeMode;
    setThemeModeState(newMode);
    localStorage.setItem(STORAGE_KEYS.themeMode, newMode);
    window.dispatchEvent(
      new CustomEvent('radio-theme-changed', { detail: newMode })
    );
    if (triggerSnackbar) {
      triggerSnackbar(`Theme mode set to ${newMode}`);
    }
  };

  // Handler for Global Default Volume
  const handleDefaultVolumeChange = (newVol: number) => {
    const clamped = Math.min(
      VOLUME_MAX_DB,
      Math.max(VOLUME_MIN_DB, Math.round(newVol))
    );
    setDefaultVolumeDbState(clamped);
    localStorage.setItem(STORAGE_KEYS.volumeDb, String(clamped));
  };

  const handleResetDefaultVolume = () => {
    setDefaultVolumeDbState(DEFAULT_VOLUME_DB);
    localStorage.removeItem(STORAGE_KEYS.volumeDb);
    if (triggerSnackbar) {
      triggerSnackbar('Default volume reset to 0 dB');
    }
  };

  // Handler for Global Default Speed
  const handleDefaultSpeedChange = (newSpeed: number) => {
    setDefaultSpeedState(newSpeed);
    localStorage.setItem(STORAGE_KEYS.speed, String(newSpeed));
  };

  const handleResetDefaultSpeed = () => {
    setDefaultSpeedState(DEFAULT_SPEED);
    localStorage.removeItem(STORAGE_KEYS.speed);
    if (triggerSnackbar) {
      triggerSnackbar('Default playback speed reset to 1×');
    }
  };

  // Handlers for Per-Feed Overrides
  const handleFeedVolumeChange = useCallback((feedId: string, vol: number) => {
    const clamped = Math.min(
      VOLUME_MAX_DB,
      Math.max(VOLUME_MIN_DB, Math.round(vol))
    );
    localStorage.setItem(
      feedKey(STORAGE_KEYS.volumeDb, feedId),
      String(clamped)
    );
    setFeedOverrides((prev) =>
      prev.map((item) =>
        item.feedId === feedId ? { ...item, volumeDb: clamped } : item
      )
    );
  }, []);

  const handleFeedPanChange = useCallback((feedId: string, pan: number) => {
    localStorage.setItem(feedKey(STORAGE_KEYS.pan, feedId), String(pan));
    setFeedOverrides((prev) =>
      prev.map((item) => (item.feedId === feedId ? { ...item, pan } : item))
    );
  }, []);

  const handleFeedSpeedChange = useCallback((feedId: string, speed: number) => {
    localStorage.setItem(feedKey(STORAGE_KEYS.speed, feedId), String(speed));
    setFeedOverrides((prev) =>
      prev.map((item) => (item.feedId === feedId ? { ...item, speed } : item))
    );
  }, []);

  const handleRemoveFeedOverride = useCallback(
    (feedId: string) => {
      localStorage.removeItem(feedKey(STORAGE_KEYS.volumeDb, feedId));
      localStorage.removeItem(feedKey(STORAGE_KEYS.pan, feedId));
      localStorage.removeItem(feedKey(STORAGE_KEYS.speed, feedId));
      setFeedOverrides(getStoredFeedOverrides());
      if (triggerSnackbar) {
        const name = feedNameMap.get(feedId) || feedId;
        triggerSnackbar(`Removed custom audio settings for ${name}`);
      }
    },
    [feedNameMap, triggerSnackbar]
  );

  const handleAddFeedOverride = useCallback(() => {
    if (!selectedFeedToAdd) return;
    const feedId = selectedFeedToAdd.id;
    localStorage.setItem(
      feedKey(STORAGE_KEYS.volumeDb, feedId),
      String(defaultVolumeDb)
    );
    localStorage.setItem(
      feedKey(STORAGE_KEYS.pan, feedId),
      String(DEFAULT_PAN)
    );
    localStorage.setItem(
      feedKey(STORAGE_KEYS.speed, feedId),
      String(defaultSpeed)
    );
    setFeedOverrides(getStoredFeedOverrides());
    if (triggerSnackbar) {
      triggerSnackbar(
        `Added custom audio override for ${selectedFeedToAdd.name}`
      );
    }
    setSelectedFeedToAdd(null);
  }, [selectedFeedToAdd, defaultVolumeDb, defaultSpeed, triggerSnackbar]);

  return (
    <Box
      sx={{
        width: '100%',
        height: '100%',
        textAlign: 'left',
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
      }}
    >
      <Box
        sx={{
          display: 'flex',
          flexDirection: 'row',
          alignItems: 'center',
          gap: 1,
          pb: 2,
          bgcolor: 'background.default',
          position: 'sticky',
          top: 0,
          zIndex: 10,
          flexShrink: 0,
        }}
      >
        <SettingsIcon
          sx={{
            fontSize: 32,
            color: 'primary.main',
          }}
        />
        <Typography variant="h4" sx={{ fontWeight: 600 }}>
          Settings
        </Typography>
      </Box>

      <Box
        sx={{
          flexGrow: 1,
          overflowY: 'auto',
          display: 'flex',
          flexDirection: 'column',
          gap: 3,
          pb: 6,
        }}
      >
        {/* Section 1: Theme Mode */}
        <Box>
          <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
            Theme Mode
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Select your preferred interface theme mode.
          </Typography>
          <FormControl component="fieldset">
            <RadioGroup
              aria-labelledby="theme-mode-label"
              name="theme-mode-group"
              value={themeMode}
              onChange={handleThemeModeChange}
              row
            >
              <FormControlLabel
                value="system"
                control={<Radio />}
                label="System Default"
              />
              <FormControlLabel
                value="light"
                control={<Radio />}
                label="Light"
              />
              <FormControlLabel value="dark" control={<Radio />} label="Dark" />
            </RadioGroup>
          </FormControl>
        </Box>

        <Divider />

        {/* Section 2: Audio */}
        <Box>
          <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
            Audio
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
            Set the default audio volume and playback speed applied to all
            feeds.
          </Typography>

          <Stack spacing={3} sx={{ maxWidth: 600 }}>
            <Box>
              <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>
                Default Volume (
                {defaultVolumeDb > 0 ? `+${defaultVolumeDb}` : defaultVolumeDb}{' '}
                dB)
              </Typography>
              <Stack direction="row" spacing={2} sx={{ alignItems: 'center' }}>
                <Slider
                  value={defaultVolumeDb}
                  min={VOLUME_MIN_DB}
                  max={VOLUME_MAX_DB}
                  step={1}
                  valueLabelDisplay="auto"
                  valueLabelFormat={(val) => `${val > 0 ? '+' : ''}${val} dB`}
                  onChange={(_, val) =>
                    handleDefaultVolumeChange(val as number)
                  }
                  aria-label="Default Volume"
                  sx={{ flexGrow: 1 }}
                />
                <Button
                  size="small"
                  variant="outlined"
                  onClick={handleResetDefaultVolume}
                  disabled={defaultVolumeDb === DEFAULT_VOLUME_DB}
                  sx={{ textTransform: 'none' }}
                >
                  Reset
                </Button>
              </Stack>
            </Box>

            <Box>
              <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>
                Default Playback Speed
              </Typography>
              <Stack direction="row" spacing={2} sx={{ alignItems: 'center' }}>
                <FormControl size="small" sx={{ minWidth: 160 }}>
                  <Select
                    value={defaultSpeed}
                    onChange={(e) =>
                      handleDefaultSpeedChange(Number(e.target.value))
                    }
                    inputProps={{ 'aria-label': 'Default Playback Speed' }}
                  >
                    {SPEED_OPTIONS.map((speed) => (
                      <MenuItem key={speed} value={speed}>
                        {speed}×
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <Button
                  size="small"
                  variant="outlined"
                  onClick={handleResetDefaultSpeed}
                  disabled={defaultSpeed === DEFAULT_SPEED}
                  sx={{ textTransform: 'none' }}
                >
                  Reset
                </Button>
              </Stack>
            </Box>
          </Stack>
        </Box>

        <Divider />

        {/* Section 3: Feeds */}
        <Box>
          <Typography variant="h6" sx={{ fontWeight: 600, mb: 1 }}>
            Feeds
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
            View and edit custom volume, stereo panning, and playback speed
            configured for specific feeds.
          </Typography>

          {availableFeedsForNewOverride.length > 0 && (
            <Stack
              direction={{ xs: 'column', sm: 'row' }}
              spacing={2}
              sx={{ alignItems: 'center', mb: 3 }}
            >
              <Autocomplete
                options={availableFeedsForNewOverride}
                filterOptions={(x) => x}
                getOptionLabel={(option) =>
                  option.name ? `${option.name} (${option.id})` : option.id
                }
                value={selectedFeedToAdd}
                inputValue={feedSearchQuery}
                onInputChange={(_, newInputValue) =>
                  setFeedSearchQuery(newInputValue)
                }
                onChange={(_, val) => setSelectedFeedToAdd(val)}
                loading={isFetchingNextPage}
                slotProps={{
                  listbox: {
                    onScroll: (event) =>
                      handleListboxScroll(
                        event,
                        fetchNextPage,
                        hasNextPage,
                        isFetchingNextPage
                      ),
                  },
                }}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    label="Select feed to override audio settings"
                    size="small"
                  />
                )}
                sx={{ minWidth: 300, flexGrow: 1 }}
              />
              <Button
                variant="contained"
                startIcon={<AddIcon />}
                onClick={handleAddFeedOverride}
                disabled={!selectedFeedToAdd}
                sx={{ textTransform: 'none', whitespace: 'nowrap' }}
              >
                Add override
              </Button>
            </Stack>
          )}

          {feedOverrides.length === 0 ? (
            <Typography
              variant="body2"
              color="text.secondary"
              sx={{ fontStyle: 'italic', py: 2 }}
            >
              No custom per-feed audio settings stored in localStorage.
            </Typography>
          ) : (
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell sx={{ fontWeight: 600 }}>Feed</TableCell>
                    <TableCell sx={{ fontWeight: 600 }}>Volume (dB)</TableCell>
                    <TableCell sx={{ fontWeight: 600 }}>Stereo Pan</TableCell>
                    <TableCell sx={{ fontWeight: 600 }}>Speed</TableCell>
                    <TableCell sx={{ fontWeight: 600 }} align="right">
                      Actions
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {feedOverrides.map((override) => {
                    const feedName =
                      feedNameMap.get(override.feedId) || override.feedId;
                    return (
                      <TableRow key={override.feedId}>
                        <TableCell>
                          <Link
                            component={RouterLink}
                            to={`/transcripts?feedId=${override.feedId}`}
                            variant="body2"
                            sx={{
                              fontWeight: 500,
                              textDecoration: 'none',
                              color: 'primary.main',
                              '&:hover': { textDecoration: 'underline' },
                            }}
                          >
                            {feedName}
                          </Link>
                        </TableCell>
                        <TableCell style={{ width: 220 }}>
                          <Stack
                            direction="row"
                            spacing={1}
                            sx={{ alignItems: 'center' }}
                          >
                            <Slider
                              size="small"
                              value={override.volumeDb}
                              min={VOLUME_MIN_DB}
                              max={VOLUME_MAX_DB}
                              step={1}
                              onChange={(_, val) =>
                                handleFeedVolumeChange(
                                  override.feedId,
                                  val as number
                                )
                              }
                              valueLabelDisplay="auto"
                              valueLabelFormat={(val) =>
                                `${val > 0 ? '+' : ''}${val} dB`
                              }
                              aria-label={`Volume for ${feedName}`}
                            />
                            <Typography variant="caption" sx={{ minWidth: 45 }}>
                              {override.volumeDb > 0
                                ? `+${override.volumeDb}`
                                : override.volumeDb}{' '}
                              dB
                            </Typography>
                          </Stack>
                        </TableCell>
                        <TableCell style={{ width: 140 }}>
                          <Select
                            size="small"
                            value={override.pan}
                            onChange={(e) =>
                              handleFeedPanChange(
                                override.feedId,
                                Number(e.target.value)
                              )
                            }
                            inputProps={{
                              'aria-label': `Pan for ${feedName}`,
                            }}
                          >
                            {PAN_OPTIONS.map((pan) => (
                              <MenuItem key={pan} value={pan}>
                                {pan === -1
                                  ? 'Left'
                                  : pan === 1
                                    ? 'Right'
                                    : 'Center'}
                              </MenuItem>
                            ))}
                          </Select>
                        </TableCell>
                        <TableCell style={{ width: 120 }}>
                          <Select
                            size="small"
                            value={override.speed}
                            onChange={(e) =>
                              handleFeedSpeedChange(
                                override.feedId,
                                Number(e.target.value)
                              )
                            }
                            inputProps={{
                              'aria-label': `Speed for ${feedName}`,
                            }}
                          >
                            {SPEED_OPTIONS.map((speed) => (
                              <MenuItem key={speed} value={speed}>
                                {speed}×
                              </MenuItem>
                            ))}
                          </Select>
                        </TableCell>
                        <TableCell align="right">
                          <IconButton
                            size="small"
                            color="error"
                            onClick={() =>
                              handleRemoveFeedOverride(override.feedId)
                            }
                            aria-label={`Remove override for ${feedName}`}
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </Box>
      </Box>
    </Box>
  );
}

export default SettingsView;
