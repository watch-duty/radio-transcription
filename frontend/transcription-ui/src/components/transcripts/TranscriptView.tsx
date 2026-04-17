import {
  useEffect,
  useMemo,
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
import {
  useInfiniteQuery,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';

import { useAuth } from '../../context/AuthContext';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { listTranscripts } from '../../service/listTranscripts';
import TranscriptRow from './TranscriptRow';
import DateTimePicker from './DateTimePicker';

interface TranscriptViewProps {
  addAlert: (alert: AlertProps) => void;
  triggerSnackbar: (message: string) => void;
}

export function TranscriptView({
  addAlert,
  triggerSnackbar,
}: TranscriptViewProps) {
  const theme = useTheme();
  const { token } = useAuth();
  const queryClient = useQueryClient();

  const [searchParams, setSearchParams] = useSearchParams();
  const [hasLoadedFromSearchParams, setHasLoadedFromSearchParams] = useState<boolean>(false);

  const [feedId, setFeedId] = useState<string>('');
  const [startTime, setStartTime] = useState<Date | null>(null);
  const [endTime, setEndTime] = useState<Date | null>(null);

  const [searchedFeedId, setSearchedFeedId] = useState<string>('');
  const [searchedStartTime, setSearchedStartTime] = useState<Date | null>(null);
  const [searchedEndTime, setSearchedEndTime] = useState<Date | null>(null);

  const areDatesValid = !startTime || !endTime || startTime.getTime() < endTime.getTime();

  const [currentlyPlayingTransmissionId, setCurrentlyPlayingTransmissionId] =
    useState<string | null>(null);

  /**
   * Effect which preloads the feed selection and transcripts based on the search params.
   */
  useEffect(() => {
    if (!hasLoadedFromSearchParams && (searchParams.get('feedId') || searchParams.get('startTimestamp') || searchParams.get('endTimestamp'))) {
      setHasLoadedFromSearchParams(true);
      
      const feedId = searchParams.get('feedId');
      if (feedId) {
        setFeedId(feedId);
        setSearchedFeedId(feedId);
      }

      const startParam = searchParams.get('startTimestamp');
      if (startParam) {
        const parsedStart = new Date(Number(startParam));
        setStartTime(parsedStart);
        setSearchedStartTime(parsedStart);
      }

      const endParam = searchParams.get('endTimestamp');
      if (endParam) {
        const parsedEnd = new Date(Number(endParam));
        setEndTime(parsedEnd);
        setSearchedEndTime(parsedEnd);
      }
    }
  }, [hasLoadedFromSearchParams, searchParams]);

  const {
    data: feeds,
    error: feedsError,
    isFetching: feedsFetching,
  } = useQuery({
    queryKey: ['listFeeds', token],
    queryFn: () => listFeeds(token!),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  /**
   * Effect for handling feeds errors.
   */
  useEffect(() => {
    if (feedsError) {
      addAlert({
        severity: 'error',
        children: `An error occurred while trying to load feeds: ${feedsError}`,
      });
    }
  }, [feedsError, addAlert]);

  const {
    data: listTranscriptsResponse,
    fetchNextPage: fetchNextTranscripts,
    hasNextPage: hasNextTranscripts,
    error: transcriptsError,
    isLoading: transcriptsLoading,
    isFetching: transcriptsFetching,
    isSuccess: isTranscriptsSuccess,
  } = useInfiniteQuery({
    queryKey: ['listTranscripts', token, searchedFeedId, searchedStartTime?.getTime(), searchedEndTime?.getTime()],
    queryFn: ({ pageParam }) =>
      listTranscripts(
        searchedFeedId, 
        token!, 
        undefined, 
        pageParam, 
        searchedStartTime ? searchedStartTime.getTime() : undefined,
        searchedEndTime ? searchedEndTime.getTime() : undefined
      ),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.nextToken,
    enabled: !!searchedFeedId,
    refetchOnWindowFocus: false,
  });

  const transcripts = useMemo(() => {
    return (
      listTranscriptsResponse?.pages.flatMap((page) => page.transcripts) ?? []
    );
  }, [listTranscriptsResponse]);

  const {
    data: rules,
    error: rulesError,
    isLoading: rulesLoading,
  } = useQuery({
    queryKey: ['listRules', token],
    queryFn: () => listRules(token!),
    enabled: !!token,
    refetchOnWindowFocus: false,
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

  const onPlay = (transmissionId: string | null) => {
    setCurrentlyPlayingTransmissionId(transmissionId);
  };

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
      <Box sx={{ display: 'flex', gap: 2, mb: 1, alignItems: 'center' }}>
        <Autocomplete
          disablePortal
          options={(feeds ?? []).sort((a, b) => a.name.localeCompare(b.name))}
          getOptionLabel={(option) =>
            typeof option === 'string' ? option : option.id
          }
          size="small"
          sx={{ width: '40%' }}
          value={feedId}
          onInputChange={(_, value) => setFeedId(value)}
          onChange={(_, option) =>
            setFeedId(
              option ? (typeof option === 'string' ? option : option.id) : ''
            )
          }
          freeSolo={true}
          loading={feedsFetching}
          disabled={feedsFetching}
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
          onClick={() => {
            // Invalidate and refresh feeds.
            queryClient.invalidateQueries({ queryKey: ['listFeeds', token] });
          }}
          disabled={feedsFetching}
          size="small"
          sx={{ ml: -1 }}
          aria-label="refresh feeds"
        >
          {feedsFetching ? (
            <CircularProgress size={24} color="inherit" />
          ) : (
            <RefreshIcon />
          )}
        </IconButton>
        <Button
          variant="contained"
          onClick={() => {
            setSearchedStartTime(startTime);
            setSearchedEndTime(endTime);
            
            const newParams: Record<string, string> = { feedId: feedId.trim() };
            if (startTime) newParams.startTimestamp = startTime.getTime().toString();
            if (endTime) newParams.endTimestamp = endTime.getTime().toString();
            setSearchParams(newParams);

            if (searchedFeedId === feedId && searchedStartTime?.getTime() === startTime?.getTime() && searchedEndTime?.getTime() === endTime?.getTime()) {
              queryClient.resetQueries({
                queryKey: ['listTranscripts', token, searchedFeedId, startTime?.getTime(), endTime?.getTime()],
              });
            } else {
              setSearchedFeedId(feedId);
            }
          }}
          disabled={feedsFetching || transcriptsLoading || !feedId.trim() || !areDatesValid}
          sx={{ minWidth: '100px', height: '40px' }}
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
                if (startTime) url.searchParams.set('startTimestamp', startTime.getTime().toString());
                if (endTime) url.searchParams.set('endTimestamp', endTime.getTime().toString());
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
      <Box sx={{ display: 'flex', gap: 2, mb: 3, width: '40%' }}>
        <DateTimePicker
          label="Start time (Optional)"
          dateTime={startTime}
          setDateTime={setStartTime}
          error={!areDatesValid}
          helperText={!areDatesValid ? "Must be before end time" : undefined}
          width="100%"
        />
        <DateTimePicker
          label="End time (Optional)"
          dateTime={endTime}
          setDateTime={setEndTime}
          error={!areDatesValid}
          helperText={!areDatesValid ? "Must be after start time" : undefined}
          width="100%"
        />
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
            {hasNextTranscripts && (
              <ListItem sx={{ justifyContent: 'center', py: theme.spacing(2) }}>
                <Button
                  variant="outlined"
                  onClick={() => fetchNextTranscripts()}
                  disabled={transcriptsFetching}
                  sx={{ minWidth: '160px' }}
                >
                  {transcriptsFetching ? (
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
        ) : transcriptsError ? (
          <Typography color="error" align="center" sx={{ mt: 4 }}>
            Error loading transcripts.
          </Typography>
        ) : isTranscriptsSuccess ? (
          <Typography color="textSecondary" align="center" sx={{ mt: 4 }}>
            No transcripts found.
          </Typography>
        ) : null}
      </Box>
    </Box>
  );
}

export default TranscriptView;
