import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router';
import type { VirtuosoHandle } from 'react-virtuoso';

import { Howl } from 'howler';

import Box from '@mui/material/Box';
import Checkbox from '@mui/material/Checkbox';
import CircularProgress from '@mui/material/CircularProgress';
import FormControlLabel from '@mui/material/FormControlLabel';
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
import { getFeed } from '../../service/getFeed';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { listTranscripts } from '../../service/listTranscripts';
import { getAudioUrl } from '../../utils/audioUtils';
import AudioDisplay from '../audio/AudioDisplay';
import FeedSearchView from '../feeds/FeedSearchView';
import FeedHeader from './FeedHeader';
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

export type AlertFilter = 'all' | 'alerts';

const DEFAULT_REFRESH_INTERVAL = 10000;
const MAX_TRANSCRIPTS_POLLING_ITERATIONS = 10;
const FEED_POLLING_INTERVAL_MS = 15000; // 15 seconds

export function TranscriptView({
  triggerSnackbar,
  onError,
}: TranscriptViewProps) {
  const theme = useTheme();
  const { token } = useAuth();

  const queryClient = useQueryClient();

  const [searchParams, setSearchParams] = useSearchParams();
  const targetFeedId = searchParams.get('feedId');
  const targetTransmissionId = searchParams.get('transmissionId');
  const targetTimestampParam = searchParams.get('timestamp');

  // Need to memoize the timestamp since Dates are compared by object reference.
  const targetTimestamp = useMemo(
    () =>
      targetTimestampParam ? new Date(Number(targetTimestampParam)) : null,
    [targetTimestampParam]
  );

  const [searchedFeedId, setSearchedFeedId] = useState<string>(
    targetFeedId || ''
  );
  const [searchedTimestamp, setSearchedTimestamp] = useState<Date | null>(
    targetTimestamp
  );

  const [newMessageCount, setNewMessageCount] = useState(0);
  const [playLatestAudio, setPlayLatestAudio] = useState(true);

  // Effect which sets the searched feed ID based on the search params changing.
  useEffect(() => {
    if (targetFeedId) {
      setSearchedFeedId(targetFeedId);
    } else {
      setSearchedFeedId('');
    }
  }, [targetFeedId]);

  // Effect which sets the searched timestamp based on the search params changing.
  useEffect(() => {
    if (targetTimestamp) {
      setSearchedTimestamp(targetTimestamp);
    } else {
      setSearchedTimestamp(null);
    }
  }, [targetTimestamp]);

  const [redactTranscripts, setRedactTranscripts] = useState(false);
  const [alertFilter, setAlertFilter] = useState<AlertFilter>('all');

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

  const currentAudio = useRef<Howl>(null);
  const [playbackEndedForId, setPlaybackEndedForId] = useState<string | null>(
    null
  );
  const [isAudioPlaying, setIsAudioPlaying] = useState(false);

  // A mutable reference to the latest list of transcripts. This prevents stale closures
  // inside the Howl audio lifecycle callbacks (like onend), ensuring continuous playback logic
  // always evaluates against the most up-to-date transcript list even if it updates mid-playback.
  const transcriptsRef = useRef<Transcript[]>([]);

  // Cleanup effect to ensure audio is unloaded when component unmounts
  useEffect(() => {
    return () => {
      currentAudio.current?.unload();
    };
  }, []);

  // Play and pause audio from a URL.
  const toggleAudio = useCallback(
    (transmissionId: string, audioUri: string) => {
      const newAudio = currentlyPlayingTransmissionId !== transmissionId;

      if (newAudio) {
        if (currentAudio.current) {
          currentAudio.current.off();
          currentAudio.current.unload();
          currentAudio.current = null;
        }
        setCurrentlyPlayingTransmissionId(transmissionId);
        setHighlightedTransmissionId(transmissionId);
      }

      if (!currentAudio.current) {
        const sound = new Howl({
          src: [getAudioUrl(audioUri)],
          html5: true,
          preload: true,
          onplay: () => setIsAudioPlaying(true),
          onpause: () => setIsAudioPlaying(false),
          onend: () => {
            const currentTranscripts = transcriptsRef.current;
            const currentIndex = currentTranscripts.findIndex(
              (t) => t.transmissionId === transmissionId
            );
            const hasNext = currentIndex > 0;

            if (!hasNext) {
              setIsAudioPlaying(false);
            }

            setPlaybackEndedForId(transmissionId);
            sound.unload();
            if (currentAudio.current === sound) {
              currentAudio.current = null;
            }
          },
        });
        currentAudio.current = sound;
      }

      // Play is no current audio or changing audio
      if (!isAudioPlaying || newAudio) {
        currentAudio.current.play();
      } else {
        currentAudio.current.pause();
      }
    },
    [currentlyPlayingTransmissionId, isAudioPlaying]
  );

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

  const { data: activeFeedData } = useQuery({
    queryKey: ['getFeed', token, searchedFeedId],
    queryFn: () => getFeed(searchedFeedId, token!),
    enabled: !!token && !!searchedFeedId,
    refetchInterval: FEED_POLLING_INTERVAL_MS,
    refetchOnWindowFocus: true,
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

  const searchedFeed = feedIdToFeedMap.get(searchedFeedId) || null;

  useEffect(() => {
    if (!searchedFeed) return;

    let pageTitle = `${searchedFeed.name} - Radio Transcription`;
    if (newMessageCount > 0) {
      pageTitle = `(${newMessageCount}) ${pageTitle}`;
    }
    if (document.title !== pageTitle) {
      document.title = pageTitle;
    }
  }, [searchedFeed, newMessageCount]);

  // Clear the unread message indicator when the user focuses back on the page
  useEffect(() => {
    const handleFocus = () => {
      setNewMessageCount(0);
    };

    window.addEventListener('focus', handleFocus);
    return () => {
      window.removeEventListener('focus', handleFocus);
    };
  }, []);

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
    dataUpdatedAt: transcriptsDataUpdatedAt,
  } = useInfiniteQuery<
    ListTranscriptsData,
    Error,
    InfiniteData<ListTranscriptsData>,
    QueryKey,
    ListTranscriptsPage
  >({
    queryKey: [
      'listTranscripts',
      token,
      searchedFeedId,
      searchedTimestamp,
      alertFilter,
    ],
    queryFn: async ({ pageParam }) => {
      const { nextToken, order } = pageParam;

      // We only fetch the timestamp on the initial load. On subsequent loads,
      // the cursor-based positioning of the database in nextToken handles the rest.
      const originalTimestampMs =
        !nextToken && searchedTimestamp
          ? searchedTimestamp.getTime()
          : undefined;

      const response = await listTranscripts(
        searchedFeedId,
        token ?? '',
        /*limit=*/ undefined,
        nextToken,
        /*startTime=*/ order === 'asc' ? originalTimestampMs : undefined,
        /*endTime=*/ order === 'desc' ? originalTimestampMs : undefined,
        order,
        alertFilter === 'alerts' ? true : undefined
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
      order: searchedTimestamp ? 'asc' : 'desc',
    },
    // Note: TanStack Query automatically manages the bidirectional pagination state for us.
    // - `getNextPageParam` is always passed the LAST page in the cache (oldest) to continue scanning backward.
    // - `getPreviousPageParam` is always passed the FIRST page in the cache (newest) to continue scanning forward.
    // Because each page stores its own `nextToken` and `order`, the framework naturally isolates the
    // forward and backward pagination bookmarks without us needing to maintain separate local state for them.
    getNextPageParam: (lastPage) => {
      // 1. If we are already fetching older transcripts ('desc'), continue scanning backward.
      if (lastPage.order === 'desc') {
        return lastPage.nextToken
          ? { order: 'desc', nextToken: lastPage.nextToken }
          : undefined;
      }
      // 2. If the initial load was 'asc' (searching from a timestamp), and the user scrolls down
      // to load older transcripts, we start fetching them in 'desc' order starting from the searched timestamp.
      return searchedTimestamp
        ? { order: 'desc', nextToken: undefined }
        : undefined;
    },
    getPreviousPageParam: (firstPage) => {
      // 1. If no timestamp was searched, we are at the "live" head. No newer transcripts exist.
      if (!searchedTimestamp) return undefined;
      // 2. If we are fetching newer transcripts ('asc') and hit the end, stop.
      if (firstPage.order === 'asc') {
        return firstPage.nextToken
          ? { order: 'asc', nextToken: firstPage.nextToken }
          : undefined;
      }
      // 3. If we are in a descending page load, we cannot fetch newer transcripts directly from it.
      return undefined;
    },
    enabled: !!token && !!searchedFeedId && isFeedsSuccess,
    refetchOnWindowFocus: false,
  });

  useEffect(() => {
    if (transcriptsError) {
      onError(transcriptsError, 'Loading transcripts');
    }
  }, [transcriptsError, onError]);

  const transcriptsLastUpdated =
    transcriptsDataUpdatedAt && transcriptsDataUpdatedAt > 0
      ? transcriptsDataUpdatedAt
      : null;

  const transcripts = useMemo(() => {
    const allTranscripts =
      listTranscriptsResponse?.pages.flatMap((page) => page.transcripts) ?? [];
    const seenIds = new Set<string>();
    const uniqueTranscripts = allTranscripts.filter((transcript) => {
      if (seenIds.has(transcript.transmissionId)) {
        return false;
      }
      seenIds.add(transcript.transmissionId);
      return true;
    });
    return uniqueTranscripts.sort(
      (a, b) =>
        new Date(b.startTimestamp).getTime() -
        new Date(a.startTimestamp).getTime()
    );
  }, [listTranscriptsResponse]);

  // Keep the ref in sync with the transcripts so that audio lifecycle callbacks can access the latest list.
  useEffect(() => {
    transcriptsRef.current = transcripts;
  }, [transcripts]);

  // Handles continuous auto-play by advancing to the next newer transcript when the current audio finishes.
  // Since the transcript list is sorted newest-first, the next transmission in time is at `currentIndex - 1`.
  useEffect(() => {
    if (!playbackEndedForId) return;

    const currentIndex = transcripts.findIndex(
      (t) => t.transmissionId === playbackEndedForId
    );

    if (currentIndex > 0) {
      const nextTranscript = transcripts[currentIndex - 1];
      toggleAudio(
        nextTranscript.transmissionId,
        nextTranscript.playbackAudioUri
      );
    }

    setPlaybackEndedForId(null);
  }, [playbackEndedForId, transcripts, toggleAudio]);

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
          /*order=*/ 'asc',
          alertFilter === 'alerts' ? true : undefined
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
  }, [newestTimestamp, searchedFeedId, token, alertFilter]);

  /**
   * Merges newly polled transcripts into the top of the infinite query cache.
   * This updates the active view without triggering a full refetch of all loaded pages.
   */
  const updateCacheWithNewTranscripts = useCallback(
    (newTranscripts: Transcript[]): Transcript[] => {
      if (!token) return [];
      let updatedTranscripts: Transcript[] = [];
      queryClient.setQueryData<InfiniteData<ListTranscriptsData>>(
        [
          'listTranscripts',
          token,
          searchedFeedId,
          searchedTimestamp,
          alertFilter,
        ],
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
          updatedTranscripts = filteredNew;

          // Prepend the new transcripts to the first (newest) page of the query cache.
          const newPages = [...oldData.pages];
          newPages[0] = {
            ...newPages[0],
            transcripts: [...filteredNew, ...newPages[0].transcripts],
          };
          return { ...oldData, pages: newPages };
        }
      );
      return updatedTranscripts;
    },
    [token, searchedFeedId, searchedTimestamp, alertFilter, queryClient]
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
        if (newTranscripts.length === 0) {
          return;
        }

        // Add the transcript to cache
        const cachedTranscripts = updateCacheWithNewTranscripts(newTranscripts);
        if (cachedTranscripts.length === 0) {
          return;
        }

        // Display snackbar indicator that new transcripts were received
        const message =
          cachedTranscripts.length === 1
            ? 'New transcript received'
            : `${cachedTranscripts.length} new transcripts received`;
        triggerSnackbar(message);

        // Update the new message count if the user is not viewing the screen
        if (!document.hasFocus()) {
          setNewMessageCount(
            (prevCount) => prevCount + cachedTranscripts.length
          );
        }

        // Trigger the new audio to play if no audio is currently playing
        if (!isAudioPlaying && playLatestAudio) {
          const audioToPlay = cachedTranscripts[cachedTranscripts.length - 1];
          toggleAudio(audioToPlay.transmissionId, audioToPlay.playbackAudioUri);
        }
      } catch (error) {
        console.error('Polling error:', error);
      } finally {
        setIsTranscriptsPolling(false);
      }
    }, DEFAULT_REFRESH_INTERVAL);

    return () => clearInterval(interval);
  }, [
    isViewAtTopOfTranscripts,
    hasNewerTranscripts,
    newestTimestamp,
    searchedFeedId,
    pollNewerTranscripts,
    updateCacheWithNewTranscripts,
    triggerSnackbar,
    toggleAudio,
    isAudioPlaying,
    playLatestAudio,
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
            behavior: 'auto',
          });
          hasScrolledToTarget.current = true;
        }, 100);
        return () => clearTimeout(timer);
      }
    }
  }, [isTranscriptsSuccess, targetTransmissionId, transcripts]);

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

  const handleTogglePlayPause = () => {
    const targetId = isAudioPlaying
      ? currentlyPlayingTransmissionId || highlightedTransmissionId
      : highlightedTransmissionId ||
        currentlyPlayingTransmissionId ||
        transcripts[0]?.transmissionId;
    if (!targetId) return;

    const transcript = transcripts.find((t) => t.transmissionId === targetId);
    if (transcript) {
      toggleAudio(transcript.transmissionId, transcript.playbackAudioUri);
    }
  };

  const handleRowClick = (transmissionId: string) => {
    setHighlightedTransmissionId(transmissionId);
  };

  const handleFilterByDateTime = (date: Date | null) => {
    setSearchedTimestamp(date);
    setSearchParams((prev) => {
      if (date) {
        prev.set('timestamp', date.getTime().toString());
      } else {
        prev.delete('timestamp');
      }
      return prev;
    });

    // Given that clearing the date effectively jumps to live, we will
    // navigate to the top of the table in case the user is scrolled
    // down in the table.
    if (date === null) {
      setTimeout(() => {
        virtuosoRef.current?.scrollToIndex({
          index: 0,
          align: 'center',
          behavior: 'auto',
        });
      }, 100);
      hasScrolledToTarget.current = false;
    }
  };

  const handleFeedSelect = (feedId: string) => {
    setSearchedFeedId(feedId);
    // Stop audio
    currentAudio.current?.stop();
    currentAudio.current?.unload();
    // Reset all state
    handleFilterByDateTime(null);
    setNewMessageCount(0);
    setCurrentlyPlayingTransmissionId(null);
    setHighlightedTransmissionId(null);
    setIsViewAtTopOfTranscripts(true);
    setPlaybackEndedForId(null);
    setIsAudioPlaying(false);
    // Update URL params
    setSearchParams((prev) => {
      prev.set('feedId', feedId);
      prev.delete('transmissionId');
      return prev;
    });
  };

  if (!token) {
    return null;
  }

  if (!searchedFeedId) {
    return (
      <FeedSearchView
        title="Select a feed to view transcripts"
        triggerSnackbar={triggerSnackbar}
        onError={onError}
      />
    );
  }

  const customSourceUrl = searchedFeed?.tags?.find(
    (t) => t.key === 'source_url'
  )?.value;
  const customArchiveUrl = searchedFeed?.tags?.find(
    (t) => t.key === 'archive_url'
  )?.value;
  const sourceUrl = customSourceUrl || searchedFeed?.sourceUrl;
  const archiveUrl = customArchiveUrl || searchedFeed?.archiveUrl;

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
      <FeedHeader
        searchedFeed={searchedFeed}
        onSelectFeed={handleFeedSelect}
        sourceUrl={sourceUrl}
        archiveUrl={archiveUrl}
        status={activeFeedData?.status ?? searchedFeed?.status}
        lastHeartbeat={
          activeFeedData?.lastHeartbeat ?? searchedFeed?.lastHeartbeat
        }
        triggerSnackbar={triggerSnackbar}
        onError={onError}
      />

      <Box
        sx={{
          display: 'flex',
          justifyContent: 'flex-end',
          alignItems: 'center',
          gap: 2,
          // This space allows room for the alert icon which hovers above the AudioDisplay.
          mb: 2.5,
        }}
      >
        <FormControlLabel
          control={
            <Checkbox
              checked={playLatestAudio}
              onChange={(e) => setPlayLatestAudio(e.target.checked)}
              disabled={!searchedFeed}
            />
          }
          label="Always play latest audio"
          slotProps={{ typography: { variant: 'body2' } }}
        />
      </Box>

      <AudioDisplay
        transcripts={transcripts}
        currentlyPlayingTransmissionId={currentlyPlayingTransmissionId}
        highlightedTransmissionId={highlightedTransmissionId}
        onClipClick={handleClipClick}
        isAudioPlaying={isAudioPlaying}
        onTogglePlayPause={handleTogglePlayPause}
      />

      <Box
        sx={{
          flexGrow: 1,
          minHeight: 0,
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        <TranscriptActionsBar
          searchedTimestamp={searchedTimestamp}
          hasNewerTranscripts={hasNewerTranscripts}
          redactTranscripts={redactTranscripts}
          setRedactTranscripts={setRedactTranscripts}
          dateTime={searchedTimestamp}
          setDateTime={handleFilterByDateTime}
          alertFilter={alertFilter}
          setAlertFilter={setAlertFilter}
          onClickViewLatest={() => handleFilterByDateTime(null)}
        />
        {transcripts.length > 0 ? (
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
            isTranscriptsPolling={isTranscriptsPolling}
            hasOlderTranscripts={hasOlderTranscripts}
            isFetchingOlderTranscripts={isFetchingOlderTranscripts}
            fetchOlderTranscripts={fetchOlderTranscripts}
            transcriptsLastUpdated={transcriptsLastUpdated}
            triggerSnackbar={triggerSnackbar}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={rulesLoading}
            onToggleAudio={toggleAudio}
            isAudioPlaying={isAudioPlaying}
            currentlyPlayingTransmissionId={currentlyPlayingTransmissionId}
            highlightedTransmissionId={highlightedTransmissionId}
            redactTranscripts={redactTranscripts}
            onRowClick={handleRowClick}
          />
        ) : feedsFetching || isTranscriptsInitialLoading ? (
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'center',
              mt: theme.spacing(2),
            }}
          >
            <CircularProgress data-testid="loading-spinner" />
          </Box>
        ) : transcriptsError ? (
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
