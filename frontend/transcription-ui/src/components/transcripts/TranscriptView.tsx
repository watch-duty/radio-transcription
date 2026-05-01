import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router';
import type { VirtuosoHandle } from 'react-virtuoso';

import LinkIcon from '@mui/icons-material/Link';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import CircularProgress from '@mui/material/CircularProgress';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import {
  type InfiniteData,
  type QueryKey,
  useInfiniteQuery,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query';
import { type Transcript } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { listTranscripts } from '../../service/listTranscripts';
import {
  getInitialTimestamp,
  roundUpToNearestMinute,
} from '../../utils/timeUtils';
import AudioDisplay from '../audio/AudioDisplay';
import DateTimePicker from '../common/DateTimePicker';
import FeedSearch from './FeedSearch';
import TranscriptActionsBar from './TranscriptActionsBar';
import TranscriptDisplay from './TranscriptDisplay';

interface TranscriptViewProps {
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

export type ListTranscriptsPage = {
  nextToken?: string;
  order: 'asc' | 'desc';
};

export type ListTranscriptsData = {
  transcripts: Transcript[];
} & ListTranscriptsPage;

const TRANSCRIPTS_POLLING_INTERVAL_MS = 15000; // 15 seconds
const TRANSCRIPTS_POLLING_INTERVAL_DISPLAY_STRING = `${TRANSCRIPTS_POLLING_INTERVAL_MS / 1000}s`;
const MAX_TRANSCRIPTS_POLLING_ITERATIONS = 10;

export function TranscriptView({
  triggerSnackbar,
  onError,
}: TranscriptViewProps) {
  const theme = useTheme();
  const { token } = useAuth();

  const queryClient = useQueryClient();

  const [searchParams, setSearchParams] = useSearchParams();
  const targetTransmissionId = searchParams.get('transmissionId');

  const [feedId, setFeedId] = useState<string>(
    () => searchParams.get('feedId') || ''
  );
  const [searchedFeedId, setSearchedFeedId] = useState<string>(
    () => searchParams.get('feedId') || ''
  );
  const [timestamp, setTimestamp] = useState<Date | null>(() =>
    getInitialTimestamp(searchParams)
  );
  const [searchedTimestamp, setSearchedTimestamp] = useState<Date | null>(() =>
    getInitialTimestamp(searchParams)
  );

  const [currentlyPlayingTransmissionId, setCurrentlyPlayingTransmissionId] =
    useState<string | null>(null);
  const [highlightedTransmissionId, setHighlightedTransmissionId] = useState<
    string | null
  >(targetTransmissionId);
  const [isViewAtTopOfTranscripts, setIsViewAtTopOfTranscripts] =
    useState(true);
  const [isTranscriptsPolling, setIsTranscriptsPolling] = useState(false);

  const virtuosoRef = useRef<VirtuosoHandle>(null);
  const hasScrolledToTarget = useRef(false);

  const {
    data: feeds,
    error: feedsError,
    isFetching: feedsFetching,
    isSuccess: isFeedsSuccess,
  } = useQuery({
    queryKey: ['listFeeds', token],
    queryFn: () => listFeeds(token!),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (feedsError) {
      onError(feedsError, 'Loading Feeds');
    }
  }, [feedsError, onError]);

  // Memoizing the feed ID to feed map so we don't have to recreate it on every render.
  const feedIdToFeedMap = useMemo(() => {
    if (!feeds) {
      return new Map<string, NonNullable<typeof feeds>[number]>();
    }
    return new Map(feeds.map((f) => [f.id, f]));
  }, [feeds]);

  // Memoizing the selected feed object derived from the feedId state.
  const selectedFeed = useMemo(() => {
    return feedIdToFeedMap.get(feedId) || null;
  }, [feedIdToFeedMap, feedId]);

  const searchedFeed = feedIdToFeedMap.get(searchedFeedId) || null;

  const {
    data: listTranscriptsResponse,
    fetchNextPage: fetchOlderTranscripts,
    hasNextPage: hasOlderTranscripts,
    fetchPreviousPage: fetchNewerTranscripts,
    hasPreviousPage: hasNewerTranscripts,
    isFetchingNextPage: isFetchingOlderTranscripts,
    isFetchingPreviousPage: isFetchingNewerTranscripts,
    error: transcriptsError,
    isLoading: isTranscriptsInitialLoading, // isLoading is the first load, which we use to show the main loading spinner
    isFetching: isTranscriptsFetching, // isFetching is any load, which we use to show that we're loading additional data
    isSuccess: isTranscriptsSuccess,
  } = useInfiniteQuery<
    ListTranscriptsData,
    Error,
    InfiniteData<ListTranscriptsData>,
    QueryKey,
    ListTranscriptsPage
  >({
    queryKey: ['listTranscripts', token, searchedFeedId, searchedTimestamp],
    queryFn: async ({ pageParam }) => {
      const { nextToken, order } = pageParam;

      // We only fetch the timestamp on the initial load. On subsequent loads,
      // the cursor-based positioning of the database in nextToken handles the rest.
      const originalTimestampMs =
        !nextToken && searchedTimestamp
          ? roundUpToNearestMinute(searchedTimestamp).getTime()
          : undefined;

      const response = await listTranscripts(
        searchedFeedId,
        token ?? '',
        /*limit=*/ undefined,
        nextToken,
        /*startTime=*/ order === 'asc' ? originalTimestampMs : undefined,
        /*endTime=*/ order === 'desc' ? originalTimestampMs : undefined,
        order
      );

      // The API returns transcripts in ascending order, meaning that the first transcript in
      // the list is the oldest in time. However, in order to display them in the proper
      // order (newest in time at the top), we need to reverse the transcripts.
      if (order === 'asc' && response.transcripts) {
        response.transcripts.reverse();
      }

      return { ...response, order };
    },
    initialPageParam: {
      order: 'desc',
    },
    // Note: TanStack Query automatically manages the bidirectional pagination state for us.
    // - `getNextPageParam` is always passed the LAST page in the cache (oldest) to continue scanning backward.
    // - `getPreviousPageParam` is always passed the FIRST page in the cache (newest) to continue scanning forward.
    // Because each page stores its own `nextToken` and `order`, the framework naturally isolates the
    // forward and backward pagination bookmarks without us needing to maintain separate local state for them.
    getNextPageParam: (lastPage) => {
      // We only fetch older transcripts (descending order). Because the initial
      // page is in descending order, we can just use the nextToken.
      if (lastPage.order !== 'desc') return undefined;
      return lastPage.nextToken
        ? { order: 'desc', nextToken: lastPage.nextToken }
        : undefined;
    },
    getPreviousPageParam: (firstPage) => {
      // 1. If no timestamp was searched, we are at the "live" head. No newer transcripts exist.
      if (!searchedTimestamp) return undefined;
      // 2. If we are already fetching newer transcripts ('asc') and hit the end, stop.
      if (firstPage.order === 'asc') {
        return firstPage.nextToken
          ? { order: 'asc', nextToken: firstPage.nextToken }
          : undefined;
      }
      // 3. Initial load (order === 'desc'): Start fetching newer transcripts from the base timestamp.
      return {
        order: 'asc',
        nextToken: undefined,
      };
    },
    enabled: !!token && !!searchedFeedId && isFeedsSuccess,
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (transcriptsError) {
      onError(transcriptsError, 'Loading transcripts');
    }
  }, [transcriptsError, onError]);

  const transcripts = useMemo(
    () =>
      listTranscriptsResponse?.pages.flatMap((page) => page.transcripts) ?? [],
    [listTranscriptsResponse]
  );

  // This is used to group transcripts by date and display them in the UI.
  // groupCounts is an array of numbers representing the number of transcripts in each group.
  // groupTitles is an array of strings representing the title of each group.
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

  // The timestamp of the newest transcript currently loaded in the UI.
  // Used as a reference point when polling for newly arriving transcripts.
  const newestTimestamp = transcripts[0]?.startTimestamp;

  /**
   * Fetches transcripts that have arrived after the current newest loaded transcript.
   * Used by both the automatic background polling and the manual "Refresh" button.
   * Handles pagination in case there are multiple pages of new transcripts.
   */
  const pollNewerTranscripts = useCallback(async () => {
    if (!newestTimestamp || !searchedFeedId || !token) return [];

    const allNewTranscripts: Transcript[] = [];
    let currentNextToken: string | undefined = undefined;
    let hasMore = true;
    let iterations = 0;

    try {
      // Fetch all pages of new transcripts moving forward in time.
      while (hasMore) {
        if (iterations > MAX_TRANSCRIPTS_POLLING_ITERATIONS) {
          console.warn(
            'pollNewerTranscripts has more than 10 pages of new transcripts. This is unexpected. If this message continues, please report a bug.'
          );
        }

        iterations++;
        const response = await listTranscripts(
          searchedFeedId,
          token,
          /*limit=*/ undefined,
          currentNextToken,
          // Query for transcripts with a start time greater than our current newest
          /*startTime=*/ new Date(newestTimestamp).getTime(),
          /*endTime=*/ undefined,
          /*order=*/ 'asc'
        );

        if (response.transcripts && response.transcripts.length > 0) {
          allNewTranscripts.push(...response.transcripts);
        }

        currentNextToken = response.nextToken;
        hasMore = !!currentNextToken;
      }
    } catch (error) {
      console.error('Error polling for new transcripts:', error);
    }

    // Reverse the array so the newest transcripts are at index 0 for prepending
    return allNewTranscripts.reverse();
  }, [newestTimestamp, searchedFeedId, token]);

  /**
   * Merges newly polled transcripts into the top of the infinite query cache.
   * This updates the active view without triggering a full refetch of all loaded pages.
   */
  const updateCacheWithNewTranscripts = useCallback(
    (newTranscripts: Transcript[]) => {
      if (!token) return;
      queryClient.setQueryData<InfiniteData<ListTranscriptsData>>(
        ['listTranscripts', token, searchedFeedId, searchedTimestamp],
        (oldData) => {
          if (!oldData) return oldData;

          // Filter out duplicates to prevent rendering issues if a transcript
          // was caught in both the initial fetch and the poll.
          const existingIds = new Set(
            oldData.pages.flatMap((p) =>
              p.transcripts.map((t) => t.transmissionId)
            )
          );
          const filteredNew = newTranscripts.filter(
            (t) => !existingIds.has(t.transmissionId)
          );

          if (filteredNew.length === 0) return oldData;

          // Prepend the new transcripts to the first (newest) page of the query cache.
          const newPages = [...oldData.pages];
          newPages[0] = {
            ...newPages[0],
            transcripts: [...filteredNew, ...newPages[0].transcripts],
          };
          return { ...oldData, pages: newPages };
        }
      );
    },
    [token, searchedFeedId, searchedTimestamp, queryClient]
  );

  /**
   * Background polling effect.
   * Automatically fetches new transcripts every 15 seconds, provided the user is:
   * 1. Scrolled to the top of the view.
   * 2. Looking at the "live" head of the stream (no more un-fetched newer pages available).
   */
  useEffect(() => {
    if (
      // Skip polling if not viewing at the top of the transcripts to prevent fetching data when the user would not see it.
      // User can always click refresh button if they want to.
      !isViewAtTopOfTranscripts ||
      // Skip polling if there are older historical pages ahead of us to load.
      hasNewerTranscripts ||
      !newestTimestamp ||
      !searchedFeedId
    ) {
      return;
    }

    const interval = setInterval(async () => {
      try {
        setIsTranscriptsPolling(true);
        const newTranscripts = await pollNewerTranscripts();
        if (newTranscripts.length > 0) {
          updateCacheWithNewTranscripts(newTranscripts);
        }
      } catch (error) {
        console.error('Polling error:', error);
      } finally {
        setIsTranscriptsPolling(false);
      }
    }, TRANSCRIPTS_POLLING_INTERVAL_MS);

    return () => clearInterval(interval);
  }, [
    isViewAtTopOfTranscripts,
    hasNewerTranscripts,
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
    queryFn: () => listRules(token ?? ''),
    enabled: !!token && isFeedsSuccess,
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (rulesError) {
      onError(rulesError, 'Loading rules');
    }
  }, [rulesError, onError]);

  // Memoizing the rule ID to name map so we don't have to recreate it on every render.
  const ruleIdToNameMap: Map<string, string> = useMemo(() => {
    if (!rules) {
      return new Map<string, string>();
    }
    return new Map(rules.map((rule) => [rule.ruleId, rule.ruleName]));
  }, [rules]);

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

  const handleManualRefresh = useCallback(async () => {
    setIsTranscriptsPolling(true);
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
      setIsTranscriptsPolling(false);
    }
  }, [pollNewerTranscripts, updateCacheWithNewTranscripts, triggerSnackbar]);

  if (!token) {
    return null;
  }

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
        <FeedSearch
          feeds={feeds ?? []}
          selectedFeed={selectedFeed}
          onFeedSelect={setFeedId}
          isFetching={feedsFetching}
        />

        <DateTimePicker
          label="Timestamp (optional)"
          dateTime={timestamp}
          setDateTime={setTimestamp}
          width="15%"
        />

        <Button
          variant="contained"
          onClick={() => {
            if (!feedId) {
              return;
            }

            const newParams: Record<string, string> = { feedId: feedId.trim() };
            if (timestamp) {
              newParams.timestamp = timestamp.getTime().toString();
            } else {
              setSearchedTimestamp(timestamp);
            }
            setSearchParams(newParams);

            if (searchedFeedId === feedId) {
              queryClient.resetQueries({
                queryKey: [
                  'listTranscripts',
                  token,
                  searchedFeedId,
                  searchedTimestamp,
                ],
              });
            } else {
              setSearchedFeedId(feedId);
              setSearchedTimestamp(timestamp);
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
            <TranscriptActionsBar
              sourceUrl={searchedFeed?.sourceUrl}
              archiveUrl={searchedFeed?.archiveUrl}
              hasNewerTranscripts={hasNewerTranscripts}
              isTranscriptsFetching={isTranscriptsFetching}
              isTranscriptsPolling={isTranscriptsPolling}
              pollingIntervalDisplay={
                TRANSCRIPTS_POLLING_INTERVAL_DISPLAY_STRING
              }
              onRefresh={handleManualRefresh}
            />
            <TranscriptDisplay
              ref={virtuosoRef}
              transcripts={transcripts}
              groupCounts={groupCounts}
              groupTitles={groupTitles}
              setIsViewAtTopOfTranscripts={setIsViewAtTopOfTranscripts}
              hasNewerTranscripts={hasNewerTranscripts}
              isFetchingNewerTranscripts={isFetchingNewerTranscripts}
              fetchNewerTranscripts={fetchNewerTranscripts}
              isTranscriptsFetching={isTranscriptsFetching}
              hasOlderTranscripts={hasOlderTranscripts}
              isFetchingOlderTranscripts={isFetchingOlderTranscripts}
              fetchOlderTranscripts={fetchOlderTranscripts}
              triggerSnackbar={triggerSnackbar}
              ruleIdToNameMap={ruleIdToNameMap}
              rulesLoading={rulesLoading}
              onPlay={onPlay}
              currentlyPlayingTransmissionId={currentlyPlayingTransmissionId}
              highlightedTransmissionId={highlightedTransmissionId}
            />
          </>
        ) : feedsFetching || isTranscriptsInitialLoading ? (
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'center',
              mt: theme.spacing(2),
            }}
          >
            <CircularProgress />
          </Box>
        ) : isTranscriptsError ? (
          <Typography
            color="error"
            align="center"
            sx={{ mt: theme.spacing(2) }}
          >
            Error loading transcripts
          </Typography>
        ) : isTranscriptsSuccess ? (
          <Box sx={{ mt: theme.spacing(2), textAlign: 'center' }}>
            <Typography color="textSecondary" align="center">
              No transcripts found
            </Typography>
          </Box>
        ) : null}
      </Box>
    </Box>
  );
}

export default TranscriptView;
