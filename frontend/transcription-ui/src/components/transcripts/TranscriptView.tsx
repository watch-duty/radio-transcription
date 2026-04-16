import { Fragment, useEffect, useMemo, useState } from 'react';

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
import AudioPlayer from '../audio/AudioPlayer';
import AlertTooltip from './AlertTooltip';

interface TranscriptViewProps {
  addAlert: (alert: AlertProps) => void;
}

export function TranscriptView({ addAlert }: TranscriptViewProps) {
  const theme = useTheme();
  const { token } = useAuth();
  const queryClient = useQueryClient();

  const [feedId, setFeedId] = useState<string>('');
  const [searchedFeedId, setSearchedFeedId] = useState<string>('');
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
    queryKey: ['listTranscripts', token, searchedFeedId],
    queryFn: ({ pageParam }) =>
      listTranscripts(searchedFeedId, token!, undefined, pageParam),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.nextToken,
    enabled: !!searchedFeedId,
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
      <Box sx={{ display: 'flex', gap: 2, mb: 3, alignItems: 'center' }}>
        <Autocomplete
          disablePortal
          options={(feeds ?? []).sort((a, b) => a.name.localeCompare(b.name))}
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
            if (searchedFeedId === feedId) {
              queryClient.resetQueries({
                queryKey: ['listTranscripts', token, searchedFeedId],
              });
            } else {
              setSearchedFeedId(feedId);
            }
          }}
          disabled={feedsFetching || transcriptsLoading || !feedId.trim()}
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
                    <Box
                      sx={{
                        width: '24px',
                        display: 'flex',
                        justifyContent: 'center',
                        flexShrink: 0,
                      }}
                    >
                      <AlertTooltip
                        evaluationDecisions={t.evaluationDecisions}
                        ruleIdToNameMap={ruleIdToNameMap}
                        rulesLoading={rulesLoading}
                      />
                    </Box>
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
                  </ListItem>
                </Fragment>
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
