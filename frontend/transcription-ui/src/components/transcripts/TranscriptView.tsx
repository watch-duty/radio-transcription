import { useEffect, useMemo, useRef, useState } from 'react';
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
import DateTimePicker from '../common/DateTimePicker';

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

  const [feedId, setFeedId] = useState<string>(() => searchParams.get('feedId') || '');
  const [timestamp, setTimestamp] = useState<Date | null>(() => {
    const param = searchParams.get('timestamp');
    if (param) return new Date(Number(param));
    const start = searchParams.get('startTimestamp');
    const end = searchParams.get('endTimestamp');
    if (start && end) {
      return new Date((Number(start) + Number(end)) / 2);
    }
    return null;
  });
  const [duration, setDuration] = useState<string>(() => {
    const param = searchParams.get('duration');
    if (param !== null) return param;
    const start = searchParams.get('startTimestamp');
    const end = searchParams.get('endTimestamp');
    if (start && end) {
      const diffMs = Number(end) - Number(start);
      return String(Math.round(diffMs / (2 * 60000)));
    }
    return '';
  });

  const [searchedFeedId, setSearchedFeedId] = useState<string>(() => searchParams.get('feedId') || '');
  const [searchedStartTime, setSearchedStartTime] = useState<Date | null>(() => {
    const ts = searchParams.get('timestamp');
    const dur = searchParams.get('duration') || '0';
    if (ts) {
      return new Date(Number(ts) - Number(dur) * 60000);
    }
    const start = searchParams.get('startTimestamp');
    return start ? new Date(Number(start)) : null;
  });
  const [searchedEndTime, setSearchedEndTime] = useState<Date | null>(() => {
    const ts = searchParams.get('timestamp');
    const dur = searchParams.get('duration') || '0';
    if (ts) {
      return new Date(Number(ts) + Number(dur) * 60000);
    }
    const end = searchParams.get('endTimestamp');
    return end ? new Date(Number(end)) : null;
  });

  const isDurationValid = !duration || (!isNaN(Number(duration)) && Number(duration) >= 0);

  const [currentlyPlayingTransmissionId, setCurrentlyPlayingTransmissionId] =
    useState<string | null>(null);

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
    isLoading: isTranscriptsInitialLoading, // isLoading is the first load, which we use to show the main loading spinner
    isFetching: isTranscriptsFetching, // isFetching is any load, which we use to show that we're loading additional data
    isSuccess: isTranscriptsSuccess,
  } = useInfiniteQuery({
    queryKey: [
      'listTranscripts',
      token,
      searchedFeedId,
      searchedStartTime?.getTime(),
      searchedEndTime?.getTime(),
    ],
    queryFn: ({ pageParam }) =>
      token
        ? listTranscripts(
            searchedFeedId,
            token,
            undefined,
            pageParam === '' ? undefined : pageParam,
            searchedStartTime ? searchedStartTime.getTime() : undefined,
            searchedEndTime ? searchedEndTime.getTime() : undefined
          )
        : Promise.resolve({ transcripts: [] }),
    initialPageParam: '',
    getNextPageParam: (lastPage) => lastPage?.nextToken,
    enabled: !!searchedFeedId,
    refetchOnWindowFocus: false,
  });

  const transcripts = useMemo(
    () =>
      listTranscriptsResponse?.pages.flatMap((page) => page.transcripts) ?? [],
    [listTranscriptsResponse]
  );

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
        flexGrow: 1,
        minHeight: 0,
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
            let calcStart: Date | null = null;
            let calcEnd: Date | null = null;
            if (timestamp) {
              const mins = duration ? Number(duration) : 0;
              const offsetMs = mins * 60000;
              calcStart = new Date(timestamp.getTime() - offsetMs);
              calcEnd = new Date(timestamp.getTime() + offsetMs);
            }

            setSearchedStartTime(calcStart);
            setSearchedEndTime(calcEnd);

            const newParams: Record<string, string> = { feedId: feedId.trim() };
            if (timestamp) newParams.timestamp = timestamp.getTime().toString();
            if (duration) newParams.duration = duration.trim();
            setSearchParams(newParams);

            if (
              searchedFeedId === feedId &&
              searchedStartTime?.getTime() === calcStart?.getTime() &&
              searchedEndTime?.getTime() === calcEnd?.getTime()
            ) {
              queryClient.resetQueries({
                queryKey: [
                  'listTranscripts',
                  token,
                  searchedFeedId,
                  calcStart?.getTime(),
                  calcEnd?.getTime(),
                ],
              });
            } else {
              setSearchedFeedId(feedId);
            }
          }}
          disabled={
            feedsFetching ||
            isTranscriptsInitialLoading ||
            !feedId.trim() ||
            !isDurationValid
          }
          sx={{ minWidth: '100px', height: '40px' }}
        >
          {isTranscriptsInitialLoading ? (
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
                if (timestamp)
                  url.searchParams.set(
                    'timestamp',
                    timestamp.getTime().toString()
                  );
                if (duration)
                  url.searchParams.set(
                    'duration',
                    duration.trim()
                  );
                navigator.clipboard.writeText(url.toString());
                triggerSnackbar('Link copied');
              }}
              sx={{ minWidth: 0, px: theme.spacing(1.5) }}
              aria-label="copy feed deeplink"
            >
              <LinkIcon fontSize="small" />
            </Button>
          </span>
        </Tooltip>
      </Box>
      <Box sx={{ display: 'flex', gap: 2, mb: 3, width: '40%' }}>
        <DateTimePicker
          label="Timestamp"
          dateTime={timestamp}
          setDateTime={setTimestamp}
          width="100%"
          helperText="(Optional) Pick a date and time to search around"
        />
        <TextField
          label="Duration"
          size="small"
          type='number'
          value={duration}
          onChange={(e) => setDuration(e.target.value)}
          error={!isDurationValid}
          helperText={!isDurationValid ? 'Must be a positive number' : "(Optional) Length of time in minutes to search around the timestamp"}
          sx={{ width: '100%' }}
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
                  disabled={isTranscriptsFetching}
                  sx={{ minWidth: '160px' }}
                >
                  {isTranscriptsFetching ? (
                    <CircularProgress size={24} color="inherit" />
                  ) : (
                    'Load More'
                  )}
                </Button>
              </ListItem>
            )}
          </List>
        ) : isTranscriptsInitialLoading ? (
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'center',
              mt: theme.spacing(2),
            }}
          >
            <CircularProgress />
          </Box>
        ) : transcriptsError ? (
          <Typography
            color="error"
            align="center"
            sx={{ mt: theme.spacing(2) }}
          >
            Error loading transcripts.
          </Typography>
        ) : isTranscriptsSuccess ? (
          <Typography
            color="textSecondary"
            align="center"
            sx={{ mt: theme.spacing(2) }}
          >
            No transcripts found.
          </Typography>
        ) : null}
      </Box>
    </Box>
  );
}

export default TranscriptView;
