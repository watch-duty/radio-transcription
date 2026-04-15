import { Fragment, useCallback, useEffect, useRef, useState } from 'react';

import RefreshIcon from '@mui/icons-material/Refresh';
import ThumbDownIcon from '@mui/icons-material/ThumbDown';
import ThumbUpIcon from '@mui/icons-material/ThumbUp';
import type { AlertProps } from '@mui/material/Alert';
import Autocomplete from '@mui/material/Autocomplete';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import CircularProgress from '@mui/material/CircularProgress';
import IconButton from '@mui/material/IconButton';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import Paper from '@mui/material/Paper';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';
import type { Feed, Transcript } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { listFeeds } from '../../service/listFeeds';
import { listTranscripts } from '../../service/listTranscripts';
import AudioPlayer from '../audio/AudioPlayer';

interface TranscriptViewProps {
  addAlert: (alert: AlertProps) => void;
}

export function TranscriptView({ addAlert }: TranscriptViewProps) {
  const { token } = useAuth();

  const initialLoadFeedsCalled = useRef(false);
  const [feeds, setFeeds] = useState<Feed[]>([]);
  const [feedsLoading, setFeedsLoading] = useState<boolean>(false);
  const [, setFeedsError] = useState<string | null>(null);

  const [feedId, setFeedId] = useState<string>('');

  const [transcripts, setTranscripts] = useState<Transcript[]>([]);
  const [transcriptsLoading, setTranscriptsLoading] = useState(false);
  const [transcriptsError, setTranscriptsError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);

  const [currentlyPlayingTransmissionId, setCurrentlyPlayingTransmissionId] =
    useState<string | null>(null);

  /**
   * A callback that loads all the available feeds for the authenticated user.
   */
  const loadFeeds = useCallback(async () => {
    setFeeds([]);
    setFeedsLoading(true);
    setFeedsError(null);

    try {
      const feeds = await listFeeds(token!);
      setFeeds(feeds);
    } catch (err: unknown) {
      if (err instanceof Error) {
        const message = `An unexpected error occurred while trying to load feeds. You can still manually enter a feed ID. Error: ${err.message}`;
        setFeedsError(err.message);
        addAlert({
          severity: 'error',
          children: message,
        });
      } else {
        setFeedsError('Unknwon error');
        addAlert({
          severity: 'error',
          children:
            'An unknown error occurred while trying to load feeds. You can still manually enter a feed ID',
        });
      }
    } finally {
      setFeedsLoading(false);
    }
  }, [token, addAlert]);

  /**
   * A callback that loads the transcripts for the specified feed ID.
   */
  const handleFetch = async () => {
    if (!feedId.trim()) return;
    setHasSearched(true);
    setTranscripts([]);
    setTranscriptsLoading(true);
    setTranscriptsError(null);

    try {
      const transcripts = await listTranscripts(feedId, token!);
      setTranscripts(transcripts);
    } catch (err: unknown) {
      if (err instanceof Error) {
        const message = `An error occurred while trying to load transcripts for feed ${feedId}. Error: ${err.message}`;
        setTranscriptsError(message);
        addAlert({
          severity: 'error',
          children: message,
        });
      } else {
        const message = `An error occurred while trying to load transcripts for feed ${feedId}.`;
        setTranscriptsError(message);
        addAlert({
          severity: 'error',
          children: message,
        });
      }
    } finally {
      setTranscriptsLoading(false);
    }
  };

  const onPlay = (transmissionId: string | null) => {
    setCurrentlyPlayingTransmissionId(transmissionId);
  };

  /**
   * This effect only loads the feeds once when the token is available.
   * We utilize the `initialLoadFeedsCalled` ref to ensure we don't initialize more than once.
   */
  useEffect(() => {
    if (token && !initialLoadFeedsCalled.current) {
      initialLoadFeedsCalled.current = true;
      loadFeeds();
    }
  }, [token, loadFeeds]);

  return (
    <Box
      sx={{
        width: '100%',
        textAlign: 'left',
        height: 'calc(100vh - 112px)',
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <Box sx={{ display: 'flex', gap: 2, mb: 3, alignItems: 'center' }}>
        <Autocomplete
          disablePortal
          options={feeds.sort((a, b) => a.name.localeCompare(b.name))}
          getOptionLabel={(option) =>
            typeof option === 'string' ? option : option.id
          }
          size="small"
          sx={{ width: '25%' }}
          onInputChange={(_, value) => setFeedId(value)}
          onChange={(_, option) =>
            setFeedId(
              option ? (typeof option === 'string' ? option : option.id) : ''
            )
          }
          freeSolo={true}
          loading={feedsLoading}
          filterOptions={(options, { inputValue }) => {
            const filtered = options.filter((option) => {
              return (
                option.name.toLowerCase().includes(inputValue.toLowerCase()) ||
                option.id.includes(inputValue)
              );
            });
            return filtered;
          }}
          renderInput={(params) => (
            <TextField
              {...params}
              label="Select a registered feed or enter a feed ID"
            />
          )}
          renderOption={(props, option) => {
            const { key, ...optionProps } = props;
            return (
              <Box key={key} component="li" {...optionProps}>
                <Typography noWrap>
                  {option.name} ({option.id})
                </Typography>
              </Box>
            );
          }}
        />
        <IconButton
          onClick={loadFeeds}
          disabled={feedsLoading}
          size="small"
          sx={{ ml: -1 }}
          aria-label="refresh feeds"
        >
          {feedsLoading ? (
            <CircularProgress size={24} color="inherit" />
          ) : (
            <RefreshIcon />
          )}
        </IconButton>
        <Button
          variant="contained"
          onClick={handleFetch}
          disabled={transcriptsLoading || !feedId.trim()}
          sx={{ minWidth: '100px' }}
        >
          {transcriptsLoading ? (
            <CircularProgress size={24} color="inherit" />
          ) : (
            'Fetch'
          )}
        </Button>
      </Box>

      <Box sx={{ flexGrow: 1, overflowY: 'auto' }}>
        {transcripts.length > 0 ? (
          <List component={Paper} variant="outlined" sx={{ p: 0 }}>
            {transcripts.map((t, index) => {
              const currentDate = new Date(t.startTimestamp);
              const prevDate =
                index > 0
                  ? new Date(transcripts[index - 1].startTimestamp)
                  : null;
              const showHeader =
                !prevDate ||
                currentDate.toDateString() !== prevDate.toDateString();

              return (
                <Fragment key={t.transmissionId}>
                  {showHeader && (
                    <ListItem sx={{ py: 0.5, bgcolor: 'action.hover' }}>
                      <Typography
                        variant="caption"
                        color="text.secondary"
                        sx={{ fontWeight: 'bold' }}
                      >
                        {currentDate.toLocaleDateString([], {
                          weekday: 'long',
                          month: 'long',
                          day: 'numeric',
                          year: 'numeric',
                        })}
                      </Typography>
                    </ListItem>
                  )}
                  <ListItem
                    divider={index < transcripts.length - 1}
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 2,
                      py: 1.5,
                    }}
                  >
                    <Typography
                      variant="caption"
                      color="text.secondary"
                      sx={{ minWidth: 'max-content' }}
                    >
                      {currentDate.toLocaleTimeString([], {
                        hour: '2-digit',
                        minute: '2-digit',
                        second: '2-digit',
                        timeZoneName: 'short',
                        hour12: false,
                      })}
                    </Typography>
                    <AudioPlayer
                      audioUri={t.canonicalAudioUri}
                      transmissionId={t.transmissionId}
                      onPlay={onPlay}
                      currentlyPlayingTransmissionId={
                        currentlyPlayingTransmissionId
                      }
                    />
                    <Typography
                      variant="body1"
                      sx={{ flexGrow: 1, whiteSpace: 'pre-wrap' }}
                    >
                      {t.transcript}
                    </Typography>
                    <Box sx={{ display: 'flex', gap: 1 }}>
                      <IconButton size="small" aria-label="thumbs up" disabled>
                        <ThumbUpIcon fontSize="small" />
                      </IconButton>
                      <IconButton
                        size="small"
                        aria-label="thumbs down"
                        disabled
                      >
                        <ThumbDownIcon fontSize="small" />
                      </IconButton>
                    </Box>
                  </ListItem>
                </Fragment>
              );
            })}
          </List>
        ) : transcriptsLoading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
            <CircularProgress />
          </Box>
        ) : hasSearched && !transcriptsError ? (
          <Typography color="text.secondary" align="center" sx={{ mt: 4 }}>
            No transcripts found.
          </Typography>
        ) : null}
      </Box>
    </Box>
  );
}

export default TranscriptView;
