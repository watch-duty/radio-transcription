import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { VirtuosoHandle } from 'react-virtuoso';

import { useQuery } from '@tanstack/react-query';
import {
  AudioClassification,
  type AudioSegment,
  type Feed,
} from '@transcription/common';

import { deriveTimelineState } from '../components/audio/deriveTimelineState';
import {
  feedPlaybackView,
  useScannerPlaybackCoordinator,
  useScannerPlaybackState,
} from '../context/ScannerPlaybackContext';
import { listFeedHistory } from '../service/listFeedHistory';
import {
  getNextContinuousSegment,
  isWithinSegment,
} from '../utils/playbackUtils';
import { TIMELINE_RANGE_DURATION_MS } from '../utils/timeUtils';
import { useAudioPlayback } from './useAudioPlayback';
import { type AlertFilter, useAudioSegments } from './useAudioSegments';
import { useAudioSettings } from './useAudioSettings';
import { useAudioTimelineWindow } from './useAudioTimelineWindow';
import {
  type RenderableAudioSegment,
  useConsolidatedAudioSegments,
} from './useConsolidatedAudioSegments';
import { useScrollAnchor } from './useScrollAnchor';
import { useTimelineHistogram } from './useTimelineHistogram';
import { useTranscriptPlayback } from './useTranscriptPlayback';

// Effective volume applied while another feed's solo mutes this one.
const MUTED_VOLUME_DB = -100;

interface UseFeedViewParams {
  feed: Feed | null;
  feedId: string;
  token: string;
  triggerSnackbar: (message: string) => void;
  // Controlled date filter — the feed page persists it in the URL, the scanner
  // keeps it in local state. Omit to disable date filtering.
  dateTime?: Date | null;
  onDateTimeChange?: (date: Date | null) => void;
  // Deep-link target: scroll to and highlight this segment on load.
  targetSegmentId?: string | null;
  // Gate for enabling the segments query (page: feed list loaded; scanner: true).
  isReady?: boolean;
  // Fired with the count of newly-arrived speech segments so the caller can
  // drive its unread badge / document title.
  onNewSpeechSegments?: (count: number) => void;
  // Overview preload window for the mini-map; omit to skip the 24h preload.
  preloadWindowMs?: number;
}

/**
 * Owns everything about viewing a single feed's live audio + transcripts: the
 * segment/playback/timeline hook stack plus the orchestration that wires them
 * together (playback intent, once-per-feed autoplay, window/scroll navigation,
 * date filtering). Consumed by both the feed page and the scanner panels so the
 * behavior lives in exactly one place; callers only render the returned values.
 */
export function useFeedView({
  feed,
  feedId,
  token,
  triggerSnackbar,
  dateTime = null,
  onDateTimeChange,
  targetSegmentId = null,
  isReady = true,
  onNewSpeechSegments,
  preloadWindowMs = TIMELINE_RANGE_DURATION_MS,
}: UseFeedViewParams) {
  const searchedFeedId = feedId;
  const searchedTimestamp = dateTime;

  // Cross-panel playback coordination (scanner only; null on the feed page).
  const coordinator = useScannerPlaybackCoordinator();
  const scannerState = useScannerPlaybackState();
  const muted = feedPlaybackView(scannerState, feedId).muted;
  const acquirePlayback = useCallback(
    (userInitiated: boolean) =>
      !coordinator || coordinator.requestPlayback(feedId, userInitiated),
    [coordinator, feedId]
  );

  const [playbackIntent, setPlaybackIntent] = useState<'playing' | 'paused'>(
    'playing'
  );
  const [redactTranscripts, setRedactTranscripts] = useState(false);
  const [alertFilter, setAlertFilter] = useState<AlertFilter>('all');
  const [searchQuery, setSearchQuery] = useState('');

  const [highlightedSegmentId, setHighlightedSegmentId] = useState<
    string | null
  >(targetSegmentId);
  const [isViewAtTopOfAudioSegments, setIsViewAtTopOfAudioSegments] =
    useState(true);

  const virtuosoRef = useRef<VirtuosoHandle>(null);
  const hasScrolledToTarget = useRef(false);
  // Whether the once-per-feed auto-play of the latest segment has run. Reset on
  // feed change so a new feed auto-plays, but not on a stop within the feed.
  const hasAutoStartedRef = useRef(false);
  // Tracks whether the user has scrolled away from the top at least once, so we
  // only auto-load newer segments when they deliberately scroll back up to the
  // top (not on the initial render, which starts at the top).
  const hasScrolledAwayFromTop = useRef(false);

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
    volumeDb: muted ? MUTED_VOLUME_DB : volumeDb,
    pan,
    speed,
  });

  // Side effects for segments that arrive from a live poll: notify, surface the
  // count to the caller (unread badge), and optionally autoplay the latest.
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
        onNewSpeechSegments?.(speechSegments.length);
      }

      // Autoplay is always-on while playing at the live edge, but only for
      // incoming SPEECH — stay idle in "listening" through silence rather than
      // auto-playing dead-air clips (which stream continuously on scanner feeds).
      if (
        playbackIntent === 'playing' &&
        !isAudioPlaying &&
        speechSegments.length > 0
      ) {
        const audioToPlay = speechSegments[speechSegments.length - 1];
        // Autoplay is not user-initiated: if another feed holds the lock (or a
        // priority feed is playing) this feed queues rather than taking over.
        if (audioToPlay.playbackAudioUri && acquirePlayback(false)) {
          togglePlay(audioToPlay.id, audioToPlay.playbackAudioUri);
        }
      }
    },
    [
      triggerSnackbar,
      onNewSpeechSegments,
      isAudioPlaying,
      playbackIntent,
      togglePlay,
      acquirePlayback,
    ]
  );

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
    isFeedsSuccess: isReady,
    pollingEnabled: isViewAtTopOfAudioSegments,
    onNewSegments: handleNewAudioSegments,
    searchQuery: searchQuery,
    // Eagerly preload the 24h overview window so the mini-map and in-window
    // navigation need no separate fetch.
    preloadWindowMs,
  });

  const { data: feedHistoryData } = useQuery({
    queryKey: ['feedHistory', searchedFeedId, token],
    queryFn: () => listFeedHistory(searchedFeedId, token, 100),
    enabled: !!searchedFeedId && !!token,
  });

  const audioSegments = useConsolidatedAudioSegments(
    rawAudioSegments,
    feed?.sourceType,
    feedHistoryData?.historyEvents
  );

  // Identity of the current query; a change replaces the list wholesale, so the
  // window and scroll anchor both reset off it.
  const audioWindowResetKey = `${searchedFeedId}|${searchedTimestamp?.getTime() ?? ''}|${alertFilter}|${searchQuery}`;

  // Single source of truth for the audio timeline's visible window.
  const {
    windowEndTime,
    windowDurationMs,
    isLatestTimeWindow,
    jumpToLive,
    centerWindowOn,
  } = useAudioTimelineWindow({
    audioSegments,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
    resetKey: audioWindowResetKey,
  });

  // Playhead color/label + jump-to-live enablement, derived from window +
  // playback state (see deriveTimelineState for the truth table).
  const { playbackState, isViewingLive } = deriveTimelineState({
    isAudioPlaying,
    playbackIntent,
    isLatestTimeWindow,
    hasDateFilter: searchedTimestamp != null,
    currentlyPlayingSegmentId,
    audioSegments,
    hasNewerAudioSegments,
  });

  // 24h overview density for the mini-map, derived from the preloaded segments.
  const {
    marks: histogramMarks,
    rangeStartMs,
    rangeEndMs,
  } = useTimelineHistogram({
    segments: rawAudioSegments,
    anchorTimestamp: searchedTimestamp,
  });

  // Keep the refs in sync with the audio segments so that audio lifecycle callbacks can access the latest list.
  useEffect(() => {
    audioSegmentsRef.current = audioSegments;
  }, [audioSegments]);

  const handleToggleAudio = useCallback(
    (segmentId: string, audioUri: string) => {
      // A click is user intent: take the lock (evicting any current holder). If
      // a priority feed is ducking this one, record the intent so it resumes.
      if (currentlyPlayingSegmentId === segmentId && isAudioPlaying) {
        togglePlay(segmentId, audioUri);
        setPlaybackIntent('paused');
        return;
      }
      setPlaybackIntent('playing');
      if (acquirePlayback(true)) {
        togglePlay(segmentId, audioUri);
      }
    },
    [currentlyPlayingSegmentId, isAudioPlaying, togglePlay, acquirePlayback]
  );

  // Resolve a target id to a playable segment — the raw segment by id, else the
  // consolidated entry containing it (a raw id inside a silence bundle) — and play it.
  const playSegmentById = useCallback(
    (targetId: string) => {
      const raw = rawAudioSegments.find((s) => s.id === targetId);
      if (raw?.playbackAudioUri) {
        togglePlay(raw.id, raw.playbackAudioUri);
        return;
      }
      const consolidated = audioSegments.find((t) =>
        isWithinSegment(t, targetId)
      );
      if (consolidated?.playbackAudioUri) {
        togglePlay(consolidated.id, consolidated.playbackAudioUri);
      }
    },
    [rawAudioSegments, audioSegments, togglePlay]
  );

  // Play the latest/selected clip when the coordinator grants this feed the
  // lock (queue advance or priority resume). Read through a ref so the stable
  // registration always sees the current segments. Relinquish if we can't play
  // (e.g. the user paused while queued) so the lock passes to the next feed.
  const playLatestRef = useRef<() => void>(() => {});
  playLatestRef.current = () => {
    if (playbackIntent !== 'playing' || audioSegments.length === 0) {
      coordinator?.relinquish(feedId);
      return;
    }
    playSegmentById(highlightedSegmentId || audioSegments[0].id);
  };

  // Bulk "Play all": express intent and attempt a gated start (queues under
  // sequential mode / ducks under priority). Read through a ref so the stable
  // registration sees the current segments.
  const bulkPlayRef = useRef<() => void>(() => {});
  bulkPlayRef.current = () => {
    setPlaybackIntent('playing');
    const target = highlightedSegmentId || audioSegments[0]?.id;
    if (target && acquirePlayback(false)) playSegmentById(target);
  };

  const bulkPauseRef = useRef<() => void>(() => {});
  bulkPauseRef.current = () => {
    // Freeze in place like the single-feed pause: pause the current clip
    // without clearing it. stopPlayback() would null currentlyPlayingSegmentId,
    // which snaps the playhead/window to the live edge.
    setPlaybackIntent('paused');
    if (isAudioPlaying && currentlyPlayingSegmentId) {
      playSegmentById(currentlyPlayingSegmentId);
    }
  };

  useEffect(() => {
    if (!coordinator) return;
    return coordinator.register(feedId, {
      onGranted: () => playLatestRef.current(),
      onEvict: () => stopPlayback(),
      play: () => bulkPlayRef.current(),
      pause: () => bulkPauseRef.current(),
    });
  }, [coordinator, feedId, stopPlayback]);

  // Report audible transitions so the coordinator can advance its queue / resume
  // interrupted feeds when this one stops.
  const wasAudibleRef = useRef(false);
  useEffect(() => {
    if (!coordinator) return;
    if (isAudioPlaying !== wasAudibleRef.current) {
      wasAudibleRef.current = isAudioPlaying;
      coordinator.notifyAudible(feedId, isAudioPlaying);
    }
  }, [coordinator, feedId, isAudioPlaying]);

  // Auto-play the latest segment once per feed load, and thereafter play the
  // highlighted segment when the user selects one. Crucially it does NOT re-start
  // just because nothing is playing after the initial load, so stopping playback
  // (jump-to-live, mini-map navigation) lands in the idle "listening" state at
  // the live edge rather than re-grabbing the backlog.
  useEffect(() => {
    if (playbackIntent !== 'playing' || audioSegments.length === 0) return;

    const hasSelectionChange =
      highlightedSegmentId != null &&
      highlightedSegmentId !== currentlyPlayingSegmentId;
    const shouldAutoStart =
      !currentlyPlayingSegmentId && !hasAutoStartedRef.current;
    if (!hasSelectionChange && !shouldAutoStart) return;
    if (shouldAutoStart) hasAutoStartedRef.current = true;

    // A user selecting a segment takes the lock; a background auto-start queues.
    if (acquirePlayback(hasSelectionChange)) {
      playSegmentById(highlightedSegmentId || audioSegments[0].id);
    }
  }, [
    playbackIntent,
    audioSegments,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
    playSegmentById,
    acquirePlayback,
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

  const scrollListToNearestTime = useCallback(
    (centerMs: number) => {
      if (audioSegments.length === 0) return;
      let nearest = 0;
      let nearestDist = Infinity;
      audioSegments.forEach((segment, i) => {
        const mid =
          (new Date(segment.startTimestamp).getTime() +
            new Date(segment.endTimestamp).getTime()) /
          2;
        const dist = Math.abs(mid - centerMs);
        if (dist < nearestDist) {
          nearestDist = dist;
          nearest = i;
        }
      });
      virtuosoRef.current?.scrollToIndex({
        index: nearest,
        align: 'center',
        // Jump rather than animate — mini-map navigation can span a long
        // distance, where a smooth scroll would be slow and disorienting.
        behavior: 'auto',
      });
    },
    [audioSegments]
  );

  // Mini-map navigation: move the window and scroll the list to that time.
  // Stops the current clip and drops the selection so it neither plays off-window
  // nor re-triggers the autoplay-on-selection effect (which would recenter away
  // from the navigated spot). Play intent is unchanged.
  const handleCenterWindow = (centerMs: number) => {
    stopPlayback();
    setHighlightedSegmentId(null);
    centerWindowOn(centerMs);
    scrollListToNearestTime(centerMs);
  };

  const handleTogglePlayPause = () => {
    if (playbackIntent === 'playing') {
      setPlaybackIntent('paused');
      if (isAudioPlaying && currentlyPlayingSegmentId) {
        playSegmentById(currentlyPlayingSegmentId);
      }
    } else {
      setPlaybackIntent('playing');
      // User-initiated resume takes the lock; bail if a priority feed ducks us.
      if (!acquirePlayback(true)) return;

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

      // Default fallback: play targetId directly.
      playSegmentById(targetId);
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
        fetchNewerAudioSegments();
      }
    },
    [fetchNewerAudioSegments]
  );

  const firstItemIndex = useScrollAnchor({
    headId: rawAudioSegments[0]?.id ?? null,
    renderedSegments: audioSegments,
    // At the top of an unfiltered list — i.e. pinned to live, not viewing the past.
    followingLiveEdge: isViewAtTopOfAudioSegments && !searchedTimestamp,
    resetKey: audioWindowResetKey,
  });

  // Reset the scroll-trigger guard when the query identity changes.
  useEffect(() => {
    hasScrolledAwayFromTop.current = false;
  }, [searchedFeedId, searchedTimestamp, alertFilter, searchQuery]);

  // A new feed re-arms the once-per-feed auto-play (see the autoplay effect).
  useEffect(() => {
    hasAutoStartedRef.current = false;
  }, [searchedFeedId]);

  const handleFilterByDateTime = (date: Date | null) => {
    onDateTimeChange?.(date);

    if (date === null) {
      // Clearing the date effectively jumps to live; return to the top.
      setTimeout(() => {
        virtuosoRef.current?.scrollToIndex({
          index: 0,
          align: 'center',
          behavior: 'auto',
        });
      }, 100);
      hasScrolledToTarget.current = false;
    } else {
      // Applying a date filter navigates the window to that time — same as a
      // mini-map click: stop playback and drop the selection so it parks there
      // (amber) rather than playing on, and the refetched list doesn't autoplay.
      stopPlayback();
      setHighlightedSegmentId(null);
    }
    hasScrolledAwayFromTop.current = false;
  };

  // Jump to live: return the window to the live edge and clear any date filter,
  // landing in the "listening" state — stop the current clip, drop the selection,
  // and set play intent, so we idle at the live edge (green) and play new audio as
  // it arrives rather than resuming the clip that was playing.
  const handleJumpToLive = () => {
    stopPlayback();
    setHighlightedSegmentId(null);
    setPlaybackIntent('playing');
    jumpToLive();
    handleFilterByDateTime(null);
  };

  return {
    // Segment data + query status
    audioSegments,
    rawAudioSegments,
    isReady,
    isAudioSegmentsInitialLoading,
    isAudioSegmentsSuccess,
    audioSegmentsError,
    hasNewerAudioSegments,
    isFetchingNewerAudioSegments,
    isAudioSegmentsPolling,
    hasOlderAudioSegments,
    isFetchingOlderAudioSegments,
    fetchOlderAudioSegments,
    audioSegmentsLastUpdated,
    // Grouping + list plumbing
    virtuosoRef,
    firstItemIndex,
    groupCounts,
    groupTitles,
    handleAtTopStateChange,
    // Playback
    isAudioPlaying,
    playbackIntent,
    currentlyPlayingSegmentId,
    currentAudioRef,
    handleToggleAudio,
    handleTogglePlayPause,
    skipToNext,
    skipToPrevious,
    skipToNextSpeech,
    skipToPreviousSpeech,
    skipTime,
    // Audio settings
    volumeDb,
    setVolumeDb,
    pan,
    setPan,
    speed,
    setSpeed,
    reset,
    muted,
    // Timeline / mini-map
    windowEndTime,
    windowDurationMs,
    isLatestTimeWindow,
    histogramMarks,
    rangeStartMs,
    rangeEndMs,
    playbackState,
    seekTrigger,
    handleClipClick,
    handleCenterWindow,
    // Selection + filters
    highlightedSegmentId,
    handleRowClick,
    redactTranscripts,
    setRedactTranscripts,
    dateTime: searchedTimestamp,
    handleFilterByDateTime,
    alertFilter,
    setAlertFilter,
    searchQuery,
    setSearchQuery,
    isViewingLive,
    handleJumpToLive,
  };
}
