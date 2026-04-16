import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { useSearchParams } from 'react-router';

import LinkIcon from '@mui/icons-material/Link';
import RefreshIcon from '@mui/icons-material/Refresh';
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
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import { useQuery } from '@tanstack/react-query';
import type { Feed, Transcript } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { listTranscripts } from '../../service/listTranscripts';
import TranscriptRow from './TranscriptRow';

interface TranscriptViewProps {
  addAlert: (alert: AlertProps) => void;
  triggerSnackbar: (message: string) => void;
}

export function TranscriptView({
  addAlert,
  triggerSnackbar,
}: TranscriptViewProps) {
  const [searchParams, setSearchParams] = useSearchParams();
  const theme = useTheme();
  const { token } = useAuth();

  const initialLoadCalled = useRef(false);
  const autoFetchCalled = useRef(false);
  const [feeds, setFeeds] = useState<Feed[]>([]);
  const [feedsLoading, setFeedsLoading] = useState<boolean>(false);
  // TODO: determine best way to render errors, leaving this empty for now
  const [, setFeedsError] = useState<string | null>(null);

  const [feedId, setFeedId] = useState<string>('');

  // TODO: call transcripts using react-query
  // https://linear.app/watchduty/issue/GOO-313/load-feeds-using-react-query
  const [transcripts, setTranscripts] = useState<Transcript[]>([]);
  const [transcriptsLoading, setTranscriptsLoading] = useState(false);
  const [transcriptsError, setTranscriptsError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);
  const [transcriptNextToken, setTranscriptNextToken] = useState<
    string | undefined
  >(undefined);
  const [loadingMoreTranscripts, setLoadingMoreTranscripts] = useState(false);

  const [currentlyPlayingTransmissionId, setCurrentlyPlayingTransmissionId] =
    useState<string | null>(null);

  const {
    data: rules,
    isError: rulesError,
    isLoading: rulesLoading,
  } = useQuery({
    queryKey: ['listRules', token],
    queryFn: () => listRules(token!),
    enabled: !!token,
  });

  // Memoizing the rule ID to name map so we don't have to recreate it on every render.
  const ruleIdToNameMap: Map<string, string> = useMemo(() => {
    if (!rules) {
      return new Map<string, string>();
    }
    return new Map(rules.map((rule) => [rule.ruleId, rule.ruleName]));
  }, [rules]);

  /**
   * Effect for handling rules errors.
   */
  useEffect(() => {
    if (rulesError) {
      addAlert({
        severity: 'error',
        children: `An error occurred while trying to load rules: ${rulesError}`,
      });
    }
  }, [rulesError, addAlert]);

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

      // Preset the feed ID if provided in the URL
      const feedIdFromUrl = searchParams.get('feedId');
      if (feedIdFromUrl) {
        setFeedId(feedIdFromUrl);
      }
    } catch (err: unknown) {
      if (err instanceof Error) {
        const message = `An unexpected error occurred while trying to load feeds. You can still manually enter a feed ID. Error: ${err.message}`;
        setFeedsError(err.message);
        addAlert({
          severity: 'error',
          children: message,
        });
      } else {
        setFeedsError('Unknown error');
        addAlert({
          severity: 'error',
          children:
            'An unknown error occurred while trying to load feeds. You can still manually enter a feed ID',
        });
      }
    } finally {
      setFeedsLoading(false);
    }
  }, [token, addAlert, searchParams]);

  /**
   * A callback that loads the transcripts for the specified feed ID.
   */
  const handleFetch = useCallback(
    async (targetFeedId?: string) => {
      const currentFeedId = targetFeedId !== undefined ? targetFeedId : feedId;
      if (!currentFeedId.trim()) return;
      setSearchParams({ feedId: currentFeedId.trim() });
      setHasSearched(true);
      setTranscripts([]);
      setTranscriptsLoading(true);
      setTranscriptsError(null);
      setTranscriptNextToken(undefined);

      try {
        const response = await listTranscripts(currentFeedId, token!);
        setTranscripts(response.transcripts);
        setTranscriptNextToken(response.nextToken);
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
    },
    [feedId, token, addAlert, setSearchParams, feedId]
  );

  const handleLoadMore = async () => {
    if (!transcriptNextToken || !feedId.trim()) return;
    setLoadingMoreTranscripts(true);
    setTranscriptsError(null);

    try {
      const response = await listTranscripts(
        feedId,
        token!,
        undefined,
        transcriptNextToken
      );
      setTranscripts((prev) => [...prev, ...response.transcripts]);
      setTranscriptNextToken(response.nextToken);
    } catch (err: unknown) {
      if (err instanceof Error) {
        setTranscriptsError(err.message);
      } else {
        setTranscriptsError('An unknown error occurred');
      }
    } finally {
      setLoadingMoreTranscripts(false);
    }
  };

  const onPlay = (transmissionId: string | null) => {
    setCurrentlyPlayingTransmissionId(transmissionId);
  };

  /**
   * This effect only loads the feeds once when the token is available.
   * We utilize the `initialLoadFeedsCalled` ref to ensure we don't initialize more than once.
   * TODO: remove this after https://linear.app/watchduty/issue/GOO-313/load-feeds-using-react-query
   */
  useEffect(() => {
    if (token && !initialLoadCalled.current) {
      initialLoadCalled.current = true;
      loadFeeds();
    }
  }, [token, loadFeeds]);

  /**
   * This effect handles loading the transcripts if a `feedId` is provided in the URL params.
   * We utilize the `autoFetchCalled` ref to ensure we don't initialize more than once.
   */
  useEffect(() => {
    if (!autoFetchCalled.current && token) {
      autoFetchCalled.current = true;
      const feedIdFromUrl = searchParams.get('feedId');
      if (feedIdFromUrl) {
        handleFetch(feedIdFromUrl);
      }
    }
  }, [token, searchParams, handleFetch]);

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
          value={feedId}
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
              label="Select a registered feed or enter a feed ID/name"
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
          onClick={() => handleFetch()}
          disabled={transcriptsLoading || !feedId.trim()}
          sx={{ minWidth: '100px' }}
        >
          {transcriptsLoading ? (
            <CircularProgress size={24} color="inherit" />
          ) : (
            'Fetch'
          )}
        </Button>
        <Box sx={{ flexGrow: 1 }} />
        <Tooltip title="Copy link to feed">
          <span>
            <Button
              variant="outlined"
              size="small"
              disabled={!feedId.trim()}
              onClick={() => {
                const url = new URL(
                  window.location.origin + window.location.pathname
                );
                url.searchParams.set('feedId', feedId.trim());
                navigator.clipboard.writeText(url.toString());
                triggerSnackbar('Link copied');
              }}
              sx={{ minWidth: 0, px: 1.5 }}
              aria-label="copy feed deeplink"
            >
              <LinkIcon fontSize="small" />
            </Button>
          </span>
        </Tooltip>
      </Box>

      <Box sx={{ flexGrow: 1, overflowY: 'auto' }}>
        {transcripts.length > 0 ? (
          <List component={Paper} variant="outlined" sx={{ p: 0 }}>
            {transcripts.map((transcript, index) => {
              const currentDate = new Date(transcript.startTimestamp);
              const prevDate =
                index > 0
                  ? new Date(transcripts[index - 1].startTimestamp)
                  : null;
              const showHeader =
                !prevDate ||
                currentDate.toDateString() !== prevDate.toDateString();

              return (
                <TranscriptRow
                  key={transcript.transmissionId}
                  transcript={transcript}
                  index={index}
                  totalTranscripts={transcripts.length}
                  ruleIdToNameMap={ruleIdToNameMap}
                  rulesLoading={rulesLoading}
                  onPlay={onPlay}
                  currentlyPlayingTransmissionId={
                    currentlyPlayingTransmissionId
                  }
                  triggerSnackbar={triggerSnackbar}
                  showHeader={showHeader}
                />
              );
            })}
            {transcriptNextToken && (
              <ListItem sx={{ justifyContent: 'center', py: theme.spacing(2) }}>
                <Button
                  variant="outlined"
                  onClick={handleLoadMore}
                  disabled={loadingMoreTranscripts}
                  sx={{ minWidth: '160px' }}
                >
                  {loadingMoreTranscripts ? (
                    <CircularProgress size={24} color="inherit" />
                  ) : (
                    'Load More'
                  )}
                </Button>
              </ListItem>
            )}
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
