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
import { useConsolidatedAudioSegments } from '../../hooks/useConsolidatedAudioSegments';
import { useTranscriptPlayback } from '../../hooks/useTranscriptPlayback';
import { getFeed } from '../../service/getFeed';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { isWithinSegment } from '../../utils/playbackUtils';
import { AudioControl } from '../audio/AudioControl';
import AudioDisplay from '../audio/AudioDisplay';
import {
  isSegmentOutsideWindow,
  useAudioTimelineWindow,
} from '../audio/useAudioTimelineWindow';
import FeedSearchView from '../feeds/FeedSearchView';
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

  // Passed to useAudioPlayback so its `onEnd` callback reads the current list
  // rather than a stale closure when deciding whether to auto-advance.
  const audioSegmentsRef = useRef<AudioSegment[]>([]);

  // Read inside the poll callback below; only autoplay incoming clips while the
  // window is at the live edge, so viewing the past isn't yanked forward.
  const isLatestTimeWindowRef = useRef(true);

  const {
    isAudioPlaying,
    currentlyPlayingSegmentId,
    playbackEndedForId,
    setPlaybackEndedForId,
    currentAudioRef,
    togglePlay: toggleAudio,
    stop: stopPlayback,
  } = useAudioPlayback({
    audioSegmentsRef,
    onPlaySegment: setHighlightedSegmentId,
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

      if (!isAudioPlaying && playLatestAudio && isLatestTimeWindowRef.current) {
        const audioToPlay = newAudioSegments[newAudioSegments.length - 1];
        if (audioToPlay.playbackAudioUri) {
          toggleAudio(audioToPlay.id, audioToPlay.playbackAudioUri);
        }
      }
    },
    [triggerSnackbar, isAudioPlaying, playLatestAudio, toggleAudio]
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

  // Single source of truth for the audio timeline's visible window, shared by
  // the waveform display and the date/time chip / jump-to-live control.
  const { windowEndTime, windowDurationMs, isLatestTimeWindow, jumpToLive } =
    useAudioTimelineWindow({
      audioSegments: rawAudioSegments,
      currentlyPlayingSegmentId,
      highlightedSegmentId,
    });
  // Keep the ref the poll callback reads in sync with the latest window state.
  useEffect(() => {
    isLatestTimeWindowRef.current = isLatestTimeWindow;
  }, [isLatestTimeWindow]);

  // A user pick pauses playback only when it will move the display window — i.e.
  // the picked segment isn't already visible. Resolves by exact id (mirroring
  // the window hook's recenter); a bundle id resolves to its first raw segment.
  const willMoveWindowTo = (segmentId: string): boolean => {
    const target = rawAudioSegments.find((s) => s.id === segmentId);
    if (!target) return false;
    const liveEnd = rawAudioSegments[0]
      ? new Date(rawAudioSegments[0].endTimestamp).getTime()
      : 0;
    return isSegmentOutsideWindow(
      new Date(target.startTimestamp).getTime(),
      new Date(target.endTimestamp).getTime(),
      windowEndTime ?? liveEnd,
      windowDurationMs
    );
  };

  // Keep the ref in sync with the audio segments so that audio lifecycle callbacks can access the latest list.
  useEffect(() => {
    audioSegmentsRef.current = audioSegments;
  }, [audioSegments]);

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
    toggleAudio,
  });

  // Handles continuous auto-play by advancing to the next newer audio segment when the current audio finishes.
  // Since the audio segment list is sorted newest-first, the next transmission in time is at `currentIndex - 1`.
  useEffect(() => {
    if (!playbackEndedForId) return;

    // 1. First check if the ended segment was part of a silence bundle, and if there is a next newer segment in that same bundle!
    const parentBundle = audioSegments.find(
      (t) =>
        t.isSilenceBundle && t.bundledSegmentIds?.includes(playbackEndedForId)
    );

    if (parentBundle && parentBundle.bundledSegmentIds) {
      const endedIdx =
        parentBundle.bundledSegmentIds.indexOf(playbackEndedForId);
      if (
        endedIdx !== -1 &&
        endedIdx < parentBundle.bundledSegmentIds.length - 1
      ) {
        const nextSegmentId = parentBundle.bundledSegmentIds[endedIdx + 1];
        const nextSegment = rawAudioSegments.find(
          (s) => s.id === nextSegmentId
        );
        if (nextSegment && nextSegment.playbackAudioUri) {
          toggleAudio(nextSegment.id, nextSegment.playbackAudioUri);
          setPlaybackEndedForId(null);
          return;
        }
      }
    }

    // 2. If it was a Speech segment, or the last segment in a silence bundle, advance to the next newer audio segment row
    const currentIndex = audioSegments.findIndex((t) =>
      isWithinSegment(t, playbackEndedForId)
    );

    if (currentIndex > 0) {
      const nextAudioSegment = audioSegments[currentIndex - 1];
      if (nextAudioSegment.playbackAudioUri) {
        // If the next audio segment is a silence bundle, play its first segment
        if (
          nextAudioSegment.isSilenceBundle &&
          nextAudioSegment.bundledSegmentIds &&
          nextAudioSegment.bundledSegmentIds.length > 0
        ) {
          const firstId = nextAudioSegment.bundledSegmentIds[0];
          const firstSegment = rawAudioSegments.find((s) => s.id === firstId);
          if (firstSegment && firstSegment.playbackAudioUri) {
            toggleAudio(firstSegment.id, firstSegment.playbackAudioUri);
            setPlaybackEndedForId(null);
            return;
          }
        }
        toggleAudio(nextAudioSegment.id, nextAudioSegment.playbackAudioUri);
      }
    }

    setPlaybackEndedForId(null);
  }, [
    playbackEndedForId,
    audioSegments,
    rawAudioSegments,
    toggleAudio,
    setPlaybackEndedForId,
  ]);

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
    if (willMoveWindowTo(segmentId)) stopPlayback();
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
    const targetId = isAudioPlaying
      ? currentlyPlayingSegmentId || highlightedSegmentId
      : highlightedSegmentId ||
        currentlyPlayingSegmentId ||
        audioSegments[0]?.id;
    if (!targetId) return;

    const specificSegment = rawAudioSegments.find((s) => s.id === targetId);
    if (specificSegment && specificSegment.playbackAudioUri) {
      toggleAudio(specificSegment.id, specificSegment.playbackAudioUri);
      return;
    }

    const audioSegment = audioSegments.find((t) =>
      isWithinSegment(t, targetId)
    );
    if (audioSegment && audioSegment.playbackAudioUri) {
      toggleAudio(audioSegment.id, audioSegment.playbackAudioUri);
    }
  };

  const handleRowClick = (segmentId: string) => {
    if (willMoveWindowTo(segmentId)) stopPlayback();
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
    // Navigating the window (filtering / jumping to live) pauses playback and
    // drops the selection so playback doesn't drag the view back.
    stopPlayback();
    setHighlightedSegmentId(null);
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

  // Jump to live: move the window to live and clear any date filter;
  // handleFilterByDateTime stops playback and clears the selection.
  const handleJumpToLive = () => {
    jumpToLive();
    handleFilterByDateTime(null);
  };

  const handleFeedSelect = (feedId: string) => {
    // Resets to live: stops playback and clears the selection + date filter.
    handleFilterByDateTime(null);
    setNewMessageCount(0);
    setIsViewAtTopOfAudioSegments(true);
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

      <AudioControl
        sx={{ mt: 1 }}
        isAudioPlaying={isAudioPlaying}
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

      <AudioDisplay
        audioSegments={rawAudioSegments}
        currentlyPlayingSegmentId={currentlyPlayingSegmentId}
        highlightedSegmentId={highlightedSegmentId}
        onClipClick={handleClipClick}
        windowEndTime={windowEndTime}
        windowDurationMs={windowDurationMs}
        isAudioPlaying={isAudioPlaying}
        currentAudioRef={currentAudioRef}
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
          hasNewerAudioSegments={hasNewerAudioSegments}
          isLatestTimeWindow={isLatestTimeWindow}
          activeWindowTime={isLatestTimeWindow ? null : windowEndTime}
          redactTranscripts={redactTranscripts}
          setRedactTranscripts={setRedactTranscripts}
          dateTime={searchedTimestamp}
          setDateTime={handleFilterByDateTime}
          alertFilter={alertFilter}
          setAlertFilter={setAlertFilter}
          onClickViewLatest={handleJumpToLive}
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
            onToggleAudio={toggleAudio}
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
