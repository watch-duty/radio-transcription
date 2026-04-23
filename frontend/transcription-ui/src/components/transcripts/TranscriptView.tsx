import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router';
import { GroupedVirtuoso, type VirtuosoHandle } from 'react-virtuoso';

import InventoryIcon from '@mui/icons-material/Inventory';
import LinkIcon from '@mui/icons-material/Link';
import RefreshIcon from '@mui/icons-material/Refresh';
import type { AlertProps } from '@mui/material/Alert';
import Autocomplete from '@mui/material/Autocomplete';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import CircularProgress from '@mui/material/CircularProgress';
import IconButton from '@mui/material/IconButton';
import Link from '@mui/material/Link';
import Paper from '@mui/material/Paper';
import TextField from '@mui/material/TextField';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import {
  type InfiniteData,
  useInfiniteQuery,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';
import type { Transcript } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { listTranscripts } from '../../service/listTranscripts';
import {
  getInitialTimestamp,
  getSearchedEndTime,
  getSearchedStartTime,
} from '../../utils/timeUtils';
import AudioDisplay from '../audio/AudioDisplay';
import DateTimePicker from '../common/DateTimePicker';
import TranscriptRow from './TranscriptRow';

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
  const targetTransmissionId = searchParams.get('transmissionId');

  const [feedId, setFeedId] = useState<string>(
    () => searchParams.get('feedId') || ''
  );
  const [timestamp, setTimestamp] = useState<Date | null>(() =>
    getInitialTimestamp(searchParams)
  );
  const [searchedFeedId, setSearchedFeedId] = useState<string>(
    () => searchParams.get('feedId') || ''
  );
  const [searchedStartTime, setSearchedStartTime] = useState<Date | null>(() =>
    getSearchedStartTime(searchParams)
  );
  const [searchedEndTime, setSearchedEndTime] = useState<Date | null>(() =>
    getSearchedEndTime(searchParams)
  );

  const [currentlyPlayingTransmissionId, setCurrentlyPlayingTransmissionId] =
    useState<string | null>(null);
  const [highlightedTransmissionId, setHighlightedTransmissionId] = useState<
    string | null
  >(targetTransmissionId);
  const [hideFooterButton, setHideFooterButton] = useState(false);
  const [hideHeaderButton, setHideHeaderButton] = useState(false);
  const [isAtTop, setIsAtTop] = useState(true);
  const [isPolling, setIsPolling] = useState(false);
  const virtuosoRef = useRef<VirtuosoHandle>(null);
  const hasScrolledToTarget = useRef(false);

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

  // Memoizing the feed ID to feed map so we don't have to recreate it on every render.
  const feedIdToFeedMap = useMemo(() => {
    if (!feeds) {
      return new Map<string, NonNullable<typeof feeds>[number]>();
    }
    return new Map(feeds.map((f) => [f.id, f]));
  }, [feeds]);

  // Memoizing the selected feed object derived from the searchedFeedId state.
  const selectedFeed = useMemo(() => {
    return feedIdToFeedMap.get(feedId) || null;
  }, [feedIdToFeedMap, feedId]);

  const searchedFeed = useMemo(() => {
    return feedIdToFeedMap.get(searchedFeedId) || null;
  }, [feedIdToFeedMap, searchedFeedId]);

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
    fetchNextPage: fetchOlderTranscripts,
    hasNextPage: hasOlderTranscripts,
    fetchPreviousPage: fetchNewerTrnscripts,
    hasPreviousPage: hasNewerTranscripts,
    isFetchingNextPage: isFetchingOlderTranscripts,
    isFetchingPreviousPage: isFetchingNewerTranscripts,
    error: transcriptsError,
    isLoading: isTranscriptsInitialLoading, // isLoading is the first load, which we use to show the main loading spinner
    isFetching: isTranscriptsFetching, // isFetching is any load, which we use to show that we're loading additional data
    isSuccess: isTranscriptsSuccess,
  } = useInfiniteQuery<
    {
      transcripts: Transcript[];
      nextToken?: string;
      startTime?: number;
      endTime?: number;
      order?: 'asc' | 'desc';
    },
    Error,
    InfiniteData<{
      transcripts: Transcript[];
      nextToken?: string;
      startTime?: number;
      endTime?: number;
      order?: 'asc' | 'desc';
    }>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    any[],
    {
      startTime?: number;
      endTime?: number;
      nextToken?: string;
      order?: 'asc' | 'desc';
    }
  >({
    queryKey: [
      'listTranscripts',
      token,
      searchedFeedId,
      searchedStartTime?.getTime(),
      searchedEndTime?.getTime(),
    ],
    queryFn: async ({ pageParam }) => {
      const { startTime, endTime, nextToken, order } = pageParam as {
        startTime?: number;
        endTime?: number;
        nextToken?: string;
        order?: 'asc' | 'desc';
      };
      const response = await listTranscripts(
        searchedFeedId,
        token!,
        undefined,
        nextToken,
        startTime,
        endTime,
        order
      );

      // The API returns transcripts in ascending order, meaning that the first transcript in
      // the list is the oldest in time. However, in order to display them in the proper
      // order (newest in time at the top), we need to reverse the transcripts.
      if (order === 'asc' && response.transcripts) {
        response.transcripts.reverse();
      }

      return { ...response, startTime, endTime, order };
    },
    initialPageParam: {
      startTime: searchedStartTime?.getTime() ?? undefined,
      endTime: searchedEndTime?.getTime() ?? undefined,
      order: undefined,
    } as {
      startTime?: number;
      endTime?: number;
      nextToken?: string;
      order?: 'asc' | 'desc';
    },
    // The naming of getNextPageParam is a bit confusing here. What we're actually
    // doing is using this function to fetch the "previous" page in time, or more
    // accurately, the page before the last page returned by the API.
    getNextPageParam: (lastPage) => {
      if (lastPage.nextToken) {
        return {
          startTime: lastPage.startTime,
          endTime: lastPage.endTime,
          nextToken: lastPage.nextToken,
          order: lastPage.order,
        };
      }
      const oldestTranscript =
        lastPage.transcripts?.[lastPage.transcripts.length - 1];
      if (oldestTranscript) {
        return {
          // Substract 1ms to avoid getting the same transcript again
          endTime: new Date(oldestTranscript.endTimestamp).getTime() - 1,
          order: 'desc' as const,
        };
      }
      if (lastPage.startTime !== undefined) {
        return {
          endTime: lastPage.startTime,
          order: 'desc' as const,
        };
      }
      return undefined;
    },
    // In contrast to getNextPageParam, getPreviousPageParam is used to fetch
    // the "next" page in time, or more accurately, the page after the first page
    // returned by the API.
    getPreviousPageParam: (firstPage) => {
      const newestTranscript = firstPage.transcripts?.[0];
      if (newestTranscript) {
        // Add 1ms to get the next transcript, as the current one ends at endTimestamp
        const startTime = new Date(newestTranscript.endTimestamp).getTime() + 1;
        if (startTime > Date.now()) {
          return undefined;
        }
        return {
          startTime,
          order: 'asc' as const,
        };
      }
      if (firstPage.endTime !== undefined) {
        if (firstPage.endTime > Date.now()) {
          return undefined;
        }
        return {
          startTime: firstPage.endTime,
          order: 'asc' as const,
        };
      }
      return undefined;
    },
    enabled: !!searchedFeedId,
    refetchOnWindowFocus: false,
  });

  const transcripts = useMemo(
    () =>
      listTranscriptsResponse?.pages.flatMap((page) => page.transcripts) ?? [],
    [listTranscriptsResponse]
  );

  const { groupCounts, groupTitles } = useMemo(() => {
    const counts: number[] = [];
    const titles: string[] = [];
    let currentTitle = '';
    let currentCount = 0;

    transcripts.forEach((t) => {
      const dateStr = new Date(t.startTimestamp).toLocaleDateString([], {
        weekday: 'long',
        month: 'long',
        day: 'numeric',
        year: 'numeric',
      });

      if (dateStr !== currentTitle) {
        if (currentCount > 0) {
          counts.push(currentCount);
        }
        currentTitle = dateStr;
        titles.push(dateStr);
        currentCount = 1;
      } else {
        currentCount++;
      }
    });

    if (currentCount > 0) {
      counts.push(currentCount);
    }

    return { groupCounts: counts, groupTitles: titles };
  }, [transcripts]);

  const newestTimestamp = transcripts[0]?.startTimestamp;

  const pollNewerTranscripts = useCallback(async () => {
    if (!newestTimestamp || !searchedFeedId) return [];
    const response = await listTranscripts(
      searchedFeedId,
      token!,
      undefined,
      undefined,
      new Date(newestTimestamp).getTime()
    );
    return response.transcripts;
  }, [newestTimestamp, searchedFeedId, token]);

  const updateCacheWithNewTranscripts = useCallback(
    (newTranscripts: Transcript[]) => {
      queryClient.setQueryData<
        InfiniteData<{
          transcripts: Transcript[];
          nextToken?: string;
          startTime?: number;
          endTime?: number;
        }>
      >(
        ['listTranscripts', token, searchedFeedId, undefined, undefined],
        (oldData) => {
          if (!oldData) return oldData;

          const existingIds = new Set(
            oldData.pages.flatMap((p) =>
              p.transcripts.map((t) => t.transmissionId)
            )
          );
          const filteredNew = newTranscripts.filter(
            (t) => !existingIds.has(t.transmissionId)
          );

          if (filteredNew.length === 0) return oldData;

          const newPages = [...oldData.pages];
          newPages[0] = {
            ...newPages[0],
            transcripts: [...filteredNew, ...newPages[0].transcripts],
          };
          return { ...oldData, pages: newPages };
        }
      );
    },
    [token, searchedFeedId, queryClient]
  );

  useEffect(() => {
    if (
      import.meta.env.MODE === 'test' ||
      searchedStartTime ||
      !isAtTop ||
      !newestTimestamp ||
      !searchedFeedId
    )
      return;

    const interval = setInterval(async () => {
      try {
        setIsPolling(true);
        const newTranscripts = await pollNewerTranscripts();
        if (newTranscripts.length > 0) {
          updateCacheWithNewTranscripts(newTranscripts);
        }
      } catch (error) {
        console.error('Polling error:', error);
      } finally {
        setIsPolling(false);
      }
    }, 15000);

    return () => clearInterval(interval);
  }, [
    searchedStartTime,
    isAtTop,
    newestTimestamp,
    searchedFeedId,
    pollNewerTranscripts,
    updateCacheWithNewTranscripts,
  ]);

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

  useEffect(() => {
    hasScrolledToTarget.current = false;
  }, [targetTransmissionId]);

  useEffect(() => {
    if (
      isTranscriptsSuccess &&
      targetTransmissionId &&
      transcripts.length > 0 &&
      !hasScrolledToTarget.current
    ) {
      const index = transcripts.findIndex(
        (t) => t.transmissionId === targetTransmissionId
      );
      if (index !== -1) {
        const timer = setTimeout(() => {
          virtuosoRef.current?.scrollToIndex({
            index,
            align: 'center',
            behavior: 'smooth',
          });
          hasScrolledToTarget.current = true;
        }, 100);
        return () => clearTimeout(timer);
      }
    }
  }, [isTranscriptsSuccess, targetTransmissionId, transcripts]);

  const onPlay = (transmissionId: string | null) => {
    setCurrentlyPlayingTransmissionId(transmissionId);
  };

  const handleClipClick = (transmissionId: string) => {
    const index = transcripts.findIndex(
      (t) => t.transmissionId === transmissionId
    );
    if (index !== -1) {
      virtuosoRef.current?.scrollToIndex({
        index,
        align: 'center',
        behavior: 'smooth',
      });
    }
    setHighlightedTransmissionId(transmissionId);
  };

  return (
    <Box
      sx={{
        width: '100%',
        textAlign: 'left',
        display: 'flex',
        flexDirection: 'column',
        height: 'calc(100vh)',
      }}
    >
      <Box
        sx={{
          display: 'flex',
          gap: 2,
          mb: 4,
          alignItems: 'center',
          width: '100%',
        }}
      >
        <Autocomplete
          disablePortal
          options={(feeds ?? []).sort((a, b) => a.name.localeCompare(b.name))}
          getOptionLabel={(option) => option.name}
          size="small"
          sx={{ width: '20%' }}
          value={selectedFeed}
          onChange={(_, option) => option && setFeedId(option.id)}
          // Explicitly disallowing custom input - the user should always pick from registered feeds
          freeSolo={false}
          loading={feedsFetching}
          disabled={feedsFetching}
          filterOptions={(options, { inputValue }) =>
            options.filter((option) =>
              option.name.toLowerCase().includes(inputValue.toLowerCase())
            )
          }
          renderInput={(params) => (
            <TextField {...params} label="Select a registered feed" />
          )}
          renderOption={(props, option) => {
            const { key, ...optionProps } = props;
            return (
              <Box key={key} component="li" {...optionProps}>
                <Typography noWrap>{option.name}</Typography>
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

        <DateTimePicker
          label="Timestamp (optional)"
          dateTime={timestamp}
          setDateTime={setTimestamp}
          width="15%"
        />

        <Button
          variant="contained"
          onClick={() => {
            let calcStart: Date | null = null;
            let calcEnd: Date | null = null;

            if (timestamp) {
              calcStart = new Date(timestamp.getTime() - 15 * 60 * 1000);
              calcEnd = new Date(timestamp.getTime() + 15 * 60 * 1000);
            }

            setSearchedStartTime(calcStart);
            setSearchedEndTime(calcEnd);
            setHideFooterButton(false);
            setHideHeaderButton(false);

            if (!feedId) {
              return;
            }

            const newParams: Record<string, string> = { feedId: feedId.trim() };
            if (timestamp) newParams.timestamp = timestamp.getTime().toString();
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
          disabled={feedsFetching || isTranscriptsInitialLoading || !feedId}
          sx={{ minWidth: '100px', height: '40px' }}
        >
          {isTranscriptsInitialLoading ? (
            <CircularProgress size={24} color="inherit" />
          ) : (
            'Fetch'
          )}
        </Button>

        <Button
          variant="outlined"
          color="primary"
          onClick={() => {
            setTimestamp(null);
            // Remove timestamp from search params to reset
            const nextParams = new URLSearchParams(searchParams);
            nextParams.delete('timestamp');
            setSearchParams(nextParams);
          }}
          disabled={!timestamp}
          sx={{ height: '40px', minWidth: '100px' }}
        >
          Clear
        </Button>

        <Box sx={{ flexGrow: 1 }} />

        <Tooltip title="Copy link to feed">
          <Box component="span">
            <Button
              variant="outlined"
              size="small"
              disabled={!feedId}
              onClick={() => {
                if (!feedId) {
                  return;
                }

                const url = new URL(
                  window.location.origin + window.location.pathname
                );
                url.searchParams.set('feedId', feedId);
                if (timestamp)
                  url.searchParams.set(
                    'timestamp',
                    timestamp.getTime().toString()
                  );
                navigator.clipboard.writeText(url.toString());
                triggerSnackbar('Link copied');
              }}
              sx={{ minWidth: 0, px: theme.spacing(1.5) }}
              aria-label="copy feed deeplink"
            >
              <LinkIcon fontSize="small" />
            </Button>
          </Box>
        </Tooltip>
      </Box>

      <AudioDisplay
        transcripts={transcripts}
        currentlyPlayingTransmissionId={currentlyPlayingTransmissionId}
        onClipClick={handleClipClick}
      />

      <Box
        sx={{
          flexGrow: 1,
          minHeight: 0,
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {transcripts.length > 0 ? (
          <>
            <Box
              sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}
            >
              {searchedFeed?.sourceUrl || searchedFeed?.archiveUrl ? (
                <Box
                  sx={{ mb: 2, display: 'flex', alignItems: 'center', gap: 2 }}
                >
                  {searchedFeed.sourceUrl && (
                    <Link
                      href={searchedFeed.sourceUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      variant="body2"
                      sx={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 0.5,
                      }}
                    >
                      <LinkIcon fontSize="small" />
                      Original source link
                    </Link>
                  )}
                  {searchedFeed.archiveUrl && (
                    <Link
                      href={searchedFeed.archiveUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      variant="body2"
                      sx={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 0.5,
                      }}
                    >
                      <InventoryIcon fontSize="small" />
                      Archives
                    </Link>
                  )}
                </Box>
              ) : (
                <Box />
              )}
              {(!hasNewerTranscripts || hideHeaderButton) && (
                <Button
                  size="small"
                  variant="text"
                  onClick={async () => {
                    setIsPolling(true);
                    try {
                      const newTranscripts = await pollNewerTranscripts();
                      if (newTranscripts.length > 0) {
                        updateCacheWithNewTranscripts(newTranscripts);
                      } else {
                        triggerSnackbar('No newer transcripts found');
                      }
                    } catch (error) {
                      console.error('Manual refresh error:', error);
                    } finally {
                      setIsPolling(false);
                    }
                  }}
                  disabled={isTranscriptsFetching || isPolling}
                  startIcon={
                    isPolling ? (
                      <CircularProgress size={16} color="inherit" />
                    ) : (
                      <RefreshIcon />
                    )
                  }
                  sx={{ textTransform: 'none' }}
                >
                  {isPolling ? 'Refreshing...' : 'Refresh (15s)'}
                </Button>
              )}
            </Box>
            <Paper
              variant="outlined"
              sx={{
                display: 'flex',
                flexDirection: 'column',
                flexGrow: 1,
                minHeight: 0,
                overflow: 'hidden',
              }}
            >
              <GroupedVirtuoso
                ref={virtuosoRef}
                groupCounts={groupCounts}
                data={transcripts}
                atTopStateChange={(atTop) => setIsAtTop(atTop)}
                groupContent={(index) => {
                  const title = groupTitles[index];
                  return (
                    <Box
                      sx={{
                        width: '100%',
                        bgcolor: 'background.paper',
                        position: 'sticky',
                        top: 0,
                        zIndex: 1,
                      }}
                    >
                      <Box
                        sx={{
                          width: '100%',
                          py: 0.5,
                          px: 2,
                          bgcolor: 'action.hover',
                        }}
                      >
                        <Typography
                          variant="caption"
                          color="text.secondary"
                          sx={{ fontWeight: 'bold' }}
                        >
                          {title}
                        </Typography>
                      </Box>
                    </Box>
                  );
                }}
                itemContent={(index, groupIndex, transcript) => {
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
                      showHeader={false}
                      isHighlighted={
                        transcript.transmissionId === highlightedTransmissionId
                      }
                    />
                  );
                }}
                components={{
                  Header: () =>
                    hasNewerTranscripts && !hideHeaderButton ? (
                      <Box
                        sx={{
                          display: 'flex',
                          justifyContent: 'center',
                          py: 1,
                        }}
                      >
                        {isFetchingNewerTranscripts ? (
                          <CircularProgress size={40} />
                        ) : (
                          <Button
                            variant="text"
                            onClick={async () => {
                              const result = await fetchNewerTrnscripts();
                              if (
                                result.data &&
                                (
                                  result.data.pages[0] as {
                                    transcripts: Transcript[];
                                  }
                                )?.transcripts.length === 0
                              ) {
                                triggerSnackbar('No newer transcripts found');
                                setHideHeaderButton(true);
                              }
                            }}
                            disabled={isTranscriptsFetching}
                            sx={{ minWidth: '160px' }}
                          >
                            Load newer transcripts
                          </Button>
                        )}
                      </Box>
                    ) : null,
                  Footer: () => {
                    if (hasOlderTranscripts && !hideFooterButton) {
                      return (
                        <Box
                          sx={{
                            display: 'flex',
                            justifyContent: 'center',
                            py: 1,
                          }}
                        >
                          {isFetchingOlderTranscripts ? (
                            <CircularProgress size={40} />
                          ) : (
                            <Button
                              variant="text"
                              onClick={async () => {
                                const result = await fetchOlderTranscripts();
                                if (result.data) {
                                  const lastPage = result.data.pages[
                                    result.data.pages.length - 1
                                  ] as { transcripts: Transcript[] };
                                  if (lastPage?.transcripts.length === 0) {
                                    triggerSnackbar(
                                      'No older transcripts found'
                                    );
                                    setHideFooterButton(true);
                                  }
                                }
                              }}
                              disabled={isTranscriptsFetching}
                              sx={{ minWidth: '160px' }}
                            >
                              Load previous transcripts
                            </Button>
                          )}
                        </Box>
                      );
                    }
                    if (!hasOlderTranscripts || hideFooterButton) {
                      return (
                        <Box
                          sx={{
                            display: 'flex',
                            justifyContent: 'center',
                            py: 2,
                          }}
                        >
                          <Typography variant="caption" color="text.secondary">
                            No more transcripts found
                          </Typography>
                        </Box>
                      );
                    }
                    return null;
                  },
                }}
              />
            </Paper>
          </>
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
          <Box sx={{ mt: theme.spacing(2), textAlign: 'center' }}>
            <Typography color="textSecondary" align="center">
              No transcripts found.
            </Typography>
          </Box>
        ) : null}
      </Box>
    </Box>
  );
}

export default TranscriptView;
