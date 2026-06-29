import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { useSearchParams } from 'react-router';
import type { VirtuosoHandle } from 'react-virtuoso';

import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Typography from '@mui/material/Typography';
import { useTheme } from '@mui/material/styles';
import { useQuery } from '@tanstack/react-query';
import { AudioClassification, type AudioSegment } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { useAudioPlayback } from '../../hooks/useAudioPlayback';
import {
  type AlertFilter,
  useAudioSegments,
} from '../../hooks/useAudioSegments';
import { useAudioSettings } from '../../hooks/useAudioSettings';
import {
  type RenderableAudioSegment,
  useConsolidatedAudioSegments,
} from '../../hooks/useConsolidatedAudioSegments';
import { useTranscriptPlayback } from '../../hooks/useTranscriptPlayback';
import { getFeed } from '../../service/getFeed';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import {
  getNextContinuousSegment,
  isWithinSegment,
} from '../../utils/playbackUtils';
import { AudioControl } from '../audio/AudioControl';
import AudioDisplay from '../audio/AudioDisplay';
import FeedSearchView from '../feeds/FeedSearchView';
import AudioSettingsButton from './AudioSettingsButton';
import FeedHeader from './FeedHeader';
import TranscriptActionsBar from './TranscriptActionsBar';
import TranscriptDisplay from './TranscriptDisplay';

interface TranscriptViewProps {
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const FEED_POLLING_INTERVAL_MS = 15000; // 15 seconds

// Base index for Virtuoso's `firstItemIndex`. When newer segments are prepended
// to the top of the list we decrease this value by the number of prepended
// items, which lets Virtuoso preserve the user's scroll position instead of
// jumping to the top. Starts high so it stays positive across many prepends.
const VIRTUOSO_START_INDEX = 1_000_000;

export function TranscriptView({
  triggerSnackbar,
  onError,
}: TranscriptViewProps) {
  const theme = useTheme();
  const { token } = useAuth();

  const [searchParams, setSearchParams] = useSearchParams();
  const targetFeedId = searchParams.get('feedId');
  const targetSegmentId = searchParams.get('segmentId');
  const targetTimestampParam = searchParams.get('timestamp');

  // Need to memoize the timestamp since Dates are compared by object reference.
  const targetTimestamp = useMemo(
    () =>
      targetTimestampParam ? new Date(Number(targetTimestampParam)) : null,
    [targetTimestampParam]
  );

  const searchedFeedId = targetFeedId || '';
  const searchedTimestamp = targetTimestamp;

  const [newMessageCount, setNewMessageCount] = useState(0);
  const [playLatestAudio, setPlayLatestAudio] = useState(true);
  const [playbackIntent, setPlaybackIntent] = useState<'playing' | 'paused'>(
    'playing'
  );

  const playLatestAudioRef = useRef(playLatestAudio);
  useEffect(() => {
    playLatestAudioRef.current = playLatestAudio;
  }, [playLatestAudio]);

  const [redactTranscripts, setRedactTranscripts] = useState(false);
  const [alertFilter, setAlertFilter] = useState<AlertFilter>('all');

  const [highlightedSegmentId, setHighlightedSegmentId] = useState<
    string | null
  >(targetSegmentId);
  const [isViewAtTopOfAudioSegments, setIsViewAtTopOfAudioSegments] =
    useState(true);

  const virtuosoRef = useRef<VirtuosoHandle>(null);
  const hasScrolledToTarget = useRef(false);
  // Tracks whether the user has scrolled away from the top at least once, so we
  // only auto-load newer segments when they deliberately scroll back up to the
  // top (not on the initial render, which starts at the top).
  const hasScrolledAwayFromTop = useRef(false);

  // Virtuoso scroll-anchoring for prepended (newer) segments. When a newer load
  // is triggered we remember the id of the current top item; once the prepend
  // lands we lower firstItemIndex by however many items appeared above it, so
  // the scroll position stays put. Only newer loads anchor this way — live
  // polling intentionally leaves firstItemIndex alone so new items show at top.
  const [firstItemIndex, setFirstItemIndex] = useState(VIRTUOSO_START_INDEX);
  const newerLoadAnchorId = useRef<string | null>(null);
  const wasFetchingNewer = useRef(false);

  const { volumeDb, setVolumeDb, pan, setPan, speed, setSpeed, reset } =
    useAudioSettings(searchedFeedId);

  // Passed to useAudioPlayback so its `onEnd` callback reads the current list
  // rather than a stale closure when deciding whether to auto-advance.
  const audioSegmentsRef = useRef<RenderableAudioSegment[]>([]);
  const rawAudioSegmentsRef = useRef<AudioSegment[]>([]);

  const {
    isAudioPlaying,
    currentlyPlayingSegmentId,
    currentAudioRef,
    togglePlay,
    stop: stopPlayback,
  } = useAudioPlayback({
    audioSegmentsRef,
    rawAudioSegmentsRef,
    onPlaySegment: setHighlightedSegmentId,
    volumeDb,
    pan,
    speed,
    onPlaybackEnded: () => {
      if (!playLatestAudioRef.current) {
        setPlaybackIntent('paused');
      }
    },
  });

  // Side effects for segments that arrive from a live poll: notify, bump the
  // unread badge when backgrounded, and optionally autoplay the latest.
  const handleNewAudioSegments = useCallback(
    (newAudioSegments: AudioSegment[]) => {
      const speechSegments = newAudioSegments.filter(
        (t) => t.classification === AudioClassification.SPEECH
      );

      if (speechSegments.length > 0) {
        triggerSnackbar(
          speechSegments.length === 1
            ? 'New transcript received'
            : `${speechSegments.length} new transcripts received`
        );

        if (!document.hasFocus()) {
          setNewMessageCount((prevCount) => prevCount + speechSegments.length);
        }
      }

      if (playbackIntent === 'playing' && !isAudioPlaying && playLatestAudio) {
        const audioToPlay = newAudioSegments[newAudioSegments.length - 1];
        if (audioToPlay.playbackAudioUri) {
          togglePlay(audioToPlay.id, audioToPlay.playbackAudioUri);
        }
      }
    },
    [
      triggerSnackbar,
      isAudioPlaying,
      playLatestAudio,
      playbackIntent,
      togglePlay,
    ]
  );

  const {
    data: feedsData,
    error: feedsError,
    isFetching: feedsFetching,
    isSuccess: isFeedsSuccess,
  } = useQuery({
    queryKey: ['listFeeds', token],
    queryFn: () => listFeeds(token!),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });

  const feeds = useMemo(() => feedsData?.feeds || [], [feedsData]);

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
    rawAudioSegments,
    loadOlderAudioSegments: fetchOlderAudioSegments,
    loadNewerAudioSegments: fetchNewerAudioSegments,
    hasOlderAudioSegments,
    hasNewerAudioSegments,
    isAudioSegmentsSuccess,
    audioSegmentsError,
    isFetchingNewerAudioSegments,
    isFetchingOlderAudioSegments,
    isAudioSegmentsPolling,
    audioSegmentsLastUpdated,
    isLoading: isAudioSegmentsInitialLoading,
  } = useAudioSegments({
    token,
    searchedFeedId,
    searchedTimestamp,
    alertFilter,
    isFeedsSuccess,
    pollingEnabled: isViewAtTopOfAudioSegments,
    onNewSegments: handleNewAudioSegments,
  });

  const audioSegments = useConsolidatedAudioSegments(rawAudioSegments);

  // Keep the refs in sync with the audio segments so that audio lifecycle callbacks can access the latest list.
  useEffect(() => {
    audioSegmentsRef.current = audioSegments;
  }, [audioSegments]);

  const handleToggleAudio = useCallback(
    (segmentId: string, audioUri: string) => {
      togglePlay(segmentId, audioUri);

      if (currentlyPlayingSegmentId === segmentId && isAudioPlaying) {
        setPlaybackIntent('paused');
      } else {
        setPlaybackIntent('playing');
      }
    },
    [currentlyPlayingSegmentId, isAudioPlaying, togglePlay]
  );

  // Automatically play the highlighted/selected segment, or the most recent segment, if play mode is active.
  useEffect(() => {
    if (playbackIntent !== 'playing' || audioSegments.length === 0) return;

    const hasSelectionChange =
      highlightedSegmentId &&
      highlightedSegmentId !== currentlyPlayingSegmentId;
    const shouldStartPlaying = !currentlyPlayingSegmentId || hasSelectionChange;

    if (shouldStartPlaying) {
      const targetId = highlightedSegmentId || audioSegments[0].id;
      const segment = rawAudioSegments.find((s) => s.id === targetId);
      if (segment && segment.playbackAudioUri) {
        togglePlay(segment.id, segment.playbackAudioUri);
      } else {
        const audioSegment = audioSegments.find((t) =>
          isWithinSegment(t, targetId)
        );
        if (audioSegment && audioSegment.playbackAudioUri) {
          togglePlay(audioSegment.id, audioSegment.playbackAudioUri);
        }
      }
    }
  }, [
    playbackIntent,
    audioSegments,
    rawAudioSegments,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
    togglePlay,
  ]);

  const [seekTrigger, setSeekTrigger] = useState(0);
  const handleSeek = useCallback(() => {
    setSeekTrigger((prev) => prev + 1);
  }, []);

  const {
    skipToNext,
    skipToPrevious,
    skipToNextSpeech,
    skipToPreviousSpeech,
    skipTime,
  } = useTranscriptPlayback({
    rawAudioSegments,
    audioSegments,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
    isAudioPlaying,
    currentAudioRef,
    virtuosoRef,
    toggleAudio: handleToggleAudio,
    onSeek: handleSeek,
  });

  useEffect(() => {
    rawAudioSegmentsRef.current = rawAudioSegments;
  }, [rawAudioSegments]);

  // This is used to group audio segments by date and display them in the UI.
  // groupCounts is an array of numbers representing the number of audio segments in each group.
  // groupTitles is an array of strings representing the title of each group.
  const { groupCounts, groupTitles } = useMemo(() => {
    const counts: number[] = [];
    const titles: string[] = [];
    let currentTitle = '';
    let currentCount = 0;

    audioSegments.forEach((t) => {
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
  }, [audioSegments]);

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
  }, [targetSegmentId]);

  useEffect(() => {
    if (
      isAudioSegmentsSuccess &&
      targetSegmentId &&
      audioSegments.length > 0 &&
      !hasScrolledToTarget.current
    ) {
      const index = audioSegments.findIndex((t) =>
        isWithinSegment(t, targetSegmentId)
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
  }, [isAudioSegmentsSuccess, targetSegmentId, audioSegments]);

  const handleClipClick = (segmentId: string) => {
    const index = audioSegments.findIndex((t) => isWithinSegment(t, segmentId));
    if (index !== -1) {
      virtuosoRef.current?.scrollToIndex({
        index,
        align: 'center',
        behavior: 'smooth',
      });
    }
    setHighlightedSegmentId(segmentId);
  };

  const handleTogglePlayPause = () => {
    if (playbackIntent === 'playing') {
      setPlaybackIntent('paused');
      if (isAudioPlaying && currentlyPlayingSegmentId) {
        const segment = rawAudioSegments.find(
          (s) => s.id === currentlyPlayingSegmentId
        );
        if (segment && segment.playbackAudioUri) {
          togglePlay(segment.id, segment.playbackAudioUri);
        }
      }
    } else {
      setPlaybackIntent('playing');

      const targetId =
        highlightedSegmentId ||
        currentlyPlayingSegmentId ||
        audioSegments[0]?.id;
      if (!targetId) return;

      const shouldPlayNext =
        currentlyPlayingSegmentId === targetId &&
        currentAudioRef.current === null;

      if (shouldPlayNext) {
        const next = getNextContinuousSegment(
          audioSegments,
          rawAudioSegments,
          targetId
        );
        if (next) {
          togglePlay(next.id, next.uri);
          return;
        }
      }

      // Default fallback: play targetId directly
      const segment = rawAudioSegments.find((s) => s.id === targetId);
      if (segment && segment.playbackAudioUri) {
        togglePlay(segment.id, segment.playbackAudioUri);
      } else {
        const audioSegment = audioSegments.find((t) =>
          isWithinSegment(t, targetId)
        );
        if (audioSegment && audioSegment.playbackAudioUri) {
          togglePlay(audioSegment.id, audioSegment.playbackAudioUri);
        }
      }
    }
  };

  const handleRowClick = (segmentId: string) => {
    setHighlightedSegmentId(segmentId);
  };

  // Reaching the top of the list loads newer segments (the prepend direction).
  // We drive this off the reliable "at top" state rather than Virtuoso's
  // startReached, which does not fire dependably after prepends. Only trigger
  // once the user has scrolled away and come back, so the initial render (which
  // starts at the top) doesn't auto-load. fetchNewerAudioSegments self-guards
  // against missing pages / in-flight requests.
  const handleAtTopStateChange = useCallback(
    (atTop: boolean) => {
      setIsViewAtTopOfAudioSegments(atTop);
      if (!atTop) {
        hasScrolledAwayFromTop.current = true;
      } else if (hasScrolledAwayFromTop.current) {
        // Remember the current top item so we can preserve the scroll position
        // once the newer segments are prepended above it. Read from the ref so
        // this callback stays stable across data updates.
        newerLoadAnchorId.current = audioSegmentsRef.current[0]?.id ?? null;
        fetchNewerAudioSegments();
      }
    },
    [fetchNewerAudioSegments]
  );

  // Once a newer load settles, lower firstItemIndex by the number of items that
  // were prepended above the anchored top item, keeping the scroll position
  // stable. react-query updates the data and clears isFetching in the same
  // commit, so by the time fetching flips to false audioSegments is current.
  // Runs before paint so the corrected offset is applied without a visible jump.
  useLayoutEffect(() => {
    const justSettled =
      wasFetchingNewer.current && !isFetchingNewerAudioSegments;
    wasFetchingNewer.current = isFetchingNewerAudioSegments;
    if (!justSettled) return;

    const anchorId = newerLoadAnchorId.current;
    newerLoadAnchorId.current = null;
    if (!anchorId) return;

    const prependedCount = audioSegments.findIndex((s) =>
      isWithinSegment(s, anchorId)
    );
    if (prependedCount > 0) {
      setFirstItemIndex((prev) => prev - prependedCount);
    }
  }, [isFetchingNewerAudioSegments, audioSegments]);

  // A different feed / timestamp / alert filter replaces the list wholesale
  // rather than prepending, so reset the anchoring baseline.
  const [prevFeedId, setPrevFeedId] = useState(searchedFeedId);
  const [prevTimestamp, setPrevTimestamp] = useState(searchedTimestamp);
  const [prevAlertFilter, setPrevAlertFilter] = useState(alertFilter);

  if (
    searchedFeedId !== prevFeedId ||
    searchedTimestamp?.getTime() !== prevTimestamp?.getTime() ||
    alertFilter !== prevAlertFilter
  ) {
    setPrevFeedId(searchedFeedId);
    setPrevTimestamp(searchedTimestamp);
    setPrevAlertFilter(alertFilter);
    setFirstItemIndex(VIRTUOSO_START_INDEX);
  }

  // Reset refs inside an effect since they should not be modified during render
  useEffect(() => {
    newerLoadAnchorId.current = null;
    hasScrolledAwayFromTop.current = false;
  }, [searchedFeedId, searchedTimestamp, alertFilter]);

  const handleFilterByDateTime = (date: Date | null) => {
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
    hasScrolledAwayFromTop.current = false;
  };

  const handleFeedSelect = (feedId: string) => {
    stopPlayback();
    // Reset all state
    handleFilterByDateTime(null);
    setNewMessageCount(0);
    setHighlightedSegmentId(null);
    setIsViewAtTopOfAudioSegments(true);
    setPlaybackIntent('playing');
    // Update URL params
    setSearchParams((prev) => {
      prev.set('feedId', feedId);
      prev.delete('segmentId');
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
        lastSpeechSegmentTimestamp={activeFeedData?.lastSpeechSegmentTimestamp}
        triggerSnackbar={triggerSnackbar}
        onError={onError}
      />

      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 1,
          mt: 1,
          // Space for the alert icon that hovers above the AudioDisplay.
          mb: 2.5,
        }}
      >
        <AudioControl
          sx={{ flex: 1, mb: 0 }}
          isAudioPlaying={playbackIntent === 'playing'}
          onTogglePlayPause={handleTogglePlayPause}
          onSkipToNext={skipToNext}
          onSkipToPrevious={skipToPrevious}
          onFastForward={skipToNextSpeech}
          onFastRewind={skipToPreviousSpeech}
          onSkipTime={skipTime}
          playLatestAudio={playLatestAudio}
          togglePlayLatestAudio={setPlayLatestAudio}
          disableControls={rawAudioSegments.length === 0}
          disableCheckbox={!searchedFeed}
        />
        <AudioSettingsButton
          volumeDb={volumeDb}
          setVolumeDb={setVolumeDb}
          pan={pan}
          setPan={setPan}
          speed={speed}
          setSpeed={setSpeed}
          onReset={reset}
          disableControls={rawAudioSegments.length === 0}
        />
      </Box>

      <AudioDisplay
        audioSegments={audioSegments}
        currentlyPlayingSegmentId={currentlyPlayingSegmentId}
        highlightedSegmentId={highlightedSegmentId}
        onClipClick={handleClipClick}
        isAudioPlaying={isAudioPlaying}
        currentAudioRef={currentAudioRef}
        seekTrigger={seekTrigger}
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
          hasNewerAudioSegments={hasNewerAudioSegments}
          redactTranscripts={redactTranscripts}
          setRedactTranscripts={setRedactTranscripts}
          dateTime={searchedTimestamp}
          setDateTime={handleFilterByDateTime}
          alertFilter={alertFilter}
          setAlertFilter={setAlertFilter}
          onClickViewLatest={() => handleFilterByDateTime(null)}
        />
        {audioSegments.length > 0 ? (
          <TranscriptDisplay
            ref={virtuosoRef}
            audioSegments={audioSegments}
            firstItemIndex={firstItemIndex}
            groupCounts={groupCounts}
            groupTitles={groupTitles}
            setIsViewAtTopOfAudioSegments={handleAtTopStateChange}
            hasNewerAudioSegments={hasNewerAudioSegments}
            isFetchingNewerAudioSegments={isFetchingNewerAudioSegments}
            isAudioSegmentsPolling={isAudioSegmentsPolling}
            hasOlderAudioSegments={hasOlderAudioSegments}
            isFetchingOlderAudioSegments={isFetchingOlderAudioSegments}
            fetchOlderAudioSegments={fetchOlderAudioSegments}
            audioSegmentsLastUpdated={audioSegmentsLastUpdated}
            triggerSnackbar={triggerSnackbar}
            ruleIdToNameMap={ruleIdToNameMap}
            rulesLoading={rulesLoading}
            onToggleAudio={handleToggleAudio}
            isAudioPlaying={isAudioPlaying}
            currentlyPlayingSegmentId={currentlyPlayingSegmentId}
            highlightedSegmentId={highlightedSegmentId}
            redactTranscripts={redactTranscripts}
            onRowClick={handleRowClick}
          />
        ) : feedsFetching || isAudioSegmentsInitialLoading ? (
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'center',
              mt: theme.spacing(2),
            }}
          >
            <CircularProgress data-testid="loading-spinner" />
          </Box>
        ) : audioSegmentsError ? (
          <Typography
            color="error"
            align="center"
            sx={{ mt: theme.spacing(2) }}
          >
            Error loading transcripts
          </Typography>
        ) : isAudioSegmentsSuccess ? (
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
