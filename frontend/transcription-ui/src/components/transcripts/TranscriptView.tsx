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
import { useQuery } from '@tanstack/react-query';
import { AudioClassification, type AudioSegment } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import { useAudioSegmentHistogram } from '../../hooks/useAudioSegmentHistogram';
import {
  type AlertFilter,
  useAudioSegments,
} from '../../hooks/useAudioSegments';
import { useConsolidatedAudioSegments } from '../../hooks/useConsolidatedAudioSegments';
import { getFeed } from '../../service/getFeed';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { getAudioUrl } from '../../utils/audioUtils';
import { AUDIO_WINDOW_DURATION_MS } from '../../utils/timeUtils';
import AudioDisplay from '../audio/AudioDisplay';
import { useAudioTimelineWindow } from '../audio/useAudioTimelineWindow';
import FeedSearchView from '../feeds/FeedSearchView';
import FeedHeader from './FeedHeader';
import TranscriptActionsBar from './TranscriptActionsBar';
import TranscriptDisplay from './TranscriptDisplay';

interface TranscriptViewProps {
  triggerSnackbar: (message: string) => void;
  onError: (error: Error, titleMessage?: string) => void;
}

const DEFAULT_REFRESH_INTERVAL = 10000;
const FEED_POLLING_INTERVAL_MS = 15000;
// Safety bound: clear the programmatic-scroll flag if Virtuoso never reports the
// scroll settling, so a no-op scroll can't wedge it on (also caps a long smooth one).
const PROGRAMMATIC_SCROLL_MAX_MS = 1500;

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

  const [searchedFeedId, setSearchedFeedId] = useState<string>(
    targetFeedId || ''
  );
  const [searchedTimestamp, setSearchedTimestamp] = useState<Date | null>(
    targetTimestamp
  );

  const [newMessageCount, setNewMessageCount] = useState(0);
  const [playLatestAudio, setPlayLatestAudio] = useState(true);

  useEffect(() => {
    if (targetFeedId) {
      setSearchedFeedId(targetFeedId);
    } else {
      setSearchedFeedId('');
    }
  }, [targetFeedId]);

  useEffect(() => {
    if (targetTimestamp) {
      setSearchedTimestamp(targetTimestamp);
    } else {
      setSearchedTimestamp(null);
    }
  }, [targetTimestamp]);

  const [redactTranscripts, setRedactTranscripts] = useState(false);
  const [alertFilter, setAlertFilter] = useState<AlertFilter>('all');

  const [currentlyPlayingSegmentId, setCurrentlyPlayingSegmentId] = useState<
    string | null
  >(null);
  const [highlightedSegmentId, setHighlightedSegmentId] = useState<
    string | null
  >(targetSegmentId);
  const [isViewAtTopOfAudioSegments, setIsViewAtTopOfAudioSegments] =
    useState(true);
  const [isAudioSegmentsPolling, setIsAudioSegmentsPolling] = useState(false);

  const virtuosoRef = useRef<VirtuosoHandle>(null);
  const hasScrolledToTarget = useRef(false);
  // Auto-scroll the list to the playing clip, suspended once the user scrolls the
  // list away (to read) and resumed on an explicit navigation.
  const followPlaybackRef = useRef(true);
  // True while a scroll we triggered is still animating (incl. a smooth one), so
  // handleListScrolling doesn't mistake its events for the user scrolling away.
  const programmaticScrollRef = useRef(false);
  const programmaticScrollTimerRef =
    useRef<ReturnType<typeof setTimeout>>(undefined);

  const currentAudio = useRef<Howl>(null);
  const [playbackEndedForId, setPlaybackEndedForId] = useState<string | null>(
    null
  );
  const [isAudioPlaying, setIsAudioPlaying] = useState(false);

  // Latest list, read by the Howl onend callback to avoid a stale closure.
  const audioSegmentsRef = useRef<AudioSegment[]>([]);

  useEffect(() => {
    return () => {
      currentAudio.current?.unload();
    };
  }, []);

  // Play and pause audio from a URL. The highlight tracks the playing clip so the
  // list selection follows playback; the timeline ignores playback-driven
  // highlight moves (see useAudioTimelineWindow) so they don't yank a scrub.
  const toggleAudio = useCallback(
    (segmentId: string, audioUri: string) => {
      const newAudio = currentlyPlayingSegmentId !== segmentId;

      if (newAudio) {
        if (currentAudio.current) {
          currentAudio.current.off();
          currentAudio.current.unload();
          currentAudio.current = null;
        }
        setCurrentlyPlayingSegmentId(segmentId);
        setHighlightedSegmentId(segmentId);
      }

      if (!currentAudio.current) {
        const sound = new Howl({
          src: [getAudioUrl(audioUri)],
          html5: true,
          preload: true,
          onplay: () => setIsAudioPlaying(true),
          onpause: () => setIsAudioPlaying(false),
          onend: () => {
            const currentAudioSegments = audioSegmentsRef.current;
            const currentIndex = currentAudioSegments.findIndex(
              (t) => t.id === segmentId
            );
            const hasNext = currentIndex > 0;

            if (!hasNext) {
              setIsAudioPlaying(false);
            }

            setPlaybackEndedForId(segmentId);
            sound.unload();
            if (currentAudio.current === sound) {
              currentAudio.current = null;
            }
          },
        });
        currentAudio.current = sound;
      }

      if (!isAudioPlaying || newAudio) {
        currentAudio.current.play();
      } else {
        currentAudio.current.pause();
      }
    },
    [currentlyPlayingSegmentId, isAudioPlaying]
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

  useEffect(() => {
    const handleFocus = () => {
      setNewMessageCount(0);
    };

    window.addEventListener('focus', handleFocus);
    return () => {
      window.removeEventListener('focus', handleFocus);
    };
  }, []);

  // Bucketed density for the overview; always the live last 24h, regardless of
  // the date filter. The viewport cursor hides when the selection is older.
  const { buckets: histogramBuckets, bucketDurationMs } =
    useAudioSegmentHistogram({
      token,
      searchedFeedId,
      alertFilter,
      isFeedsSuccess,
      anchorTimestamp: null,
    });

  // The list loads forward from its anchor, but the focus time should sit in the
  // middle of the window, so load from half a window earlier.
  const listAnchorTimestamp = useMemo(
    () =>
      searchedTimestamp
        ? new Date(searchedTimestamp.getTime() - AUDIO_WINDOW_DURATION_MS / 2)
        : null,
    [searchedTimestamp]
  );

  const {
    rawAudioSegments,
    loadOlderAudioSegments: fetchOlderAudioSegments,
    loadNewerAudioSegments: fetchNewerAudioSegments,
    hasOlderAudioSegments,
    hasNewerAudioSegments,
    isAudioSegmentsSuccess,
    audioSegmentsError,
    audioSegmentsDataUpdatedAt,
    isFetchingNewerAudioSegments,
    isFetchingOlderAudioSegments,
    pollNewerAudioSegments,
    updateCacheWithNewAudioSegments,
    isLoading: isAudioSegmentsInitialLoading,
    isFetching: isAudioSegmentsFetching,
  } = useAudioSegments({
    token,
    searchedFeedId,
    searchedTimestamp: listAnchorTimestamp,
    alertFilter,
    isFeedsSuccess,
  });

  const audioSegmentsLastUpdated =
    audioSegmentsDataUpdatedAt && audioSegmentsDataUpdatedAt > 0
      ? audioSegmentsDataUpdatedAt
      : null;

  const audioSegments = useConsolidatedAudioSegments(rawAudioSegments);

  const histogramMarks = useMemo(
    () =>
      histogramBuckets.map((b) => {
        const startMs = new Date(b.bucketStart).getTime();
        return {
          startMs,
          endMs: startMs + bucketDurationMs,
          count: b.count,
          hasAlert: b.isAlert,
        };
      }),
    [histogramBuckets, bucketDurationMs]
  );

  const {
    windowEndTime,
    windowDurationMs,
    isScrubbed,
    rangeStartMs,
    maxEnd,
    scrubToCenter,
    jumpToLive,
  } = useAudioTimelineWindow({
    audioSegments: rawAudioSegments,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
    overviewAnchorMs: null,
  });
  const activeWindowTime = isScrubbed ? windowEndTime : null;

  useEffect(() => {
    audioSegmentsRef.current = audioSegments;
  }, [audioSegments]);

  // Continuous auto-play: on finish, advance to the next newer segment. The list
  // is newest-first, so the next transmission in time is at `currentIndex - 1`.
  useEffect(() => {
    if (!playbackEndedForId) return;

    // Within a silence bundle, advance to the next bundled segment first.
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

    // Otherwise advance to the next newer row (speech, or end of a bundle).
    const currentIndex = audioSegments.findIndex(
      (t) =>
        t.id === playbackEndedForId ||
        t.bundledSegmentIds?.includes(playbackEndedForId)
    );

    if (currentIndex > 0) {
      const nextAudioSegment = audioSegments[currentIndex - 1];
      if (nextAudioSegment.playbackAudioUri) {
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
  }, [playbackEndedForId, audioSegments, rawAudioSegments, toggleAudio]);

  // Virtuoso grouped-list inputs: per-date row counts and their titles.
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

  // Poll for new segments only while the user is at the top of the list and at
  // the live head; otherwise the additions wouldn't be visible anyway.
  useEffect(() => {
    if (
      !isAudioSegmentsSuccess ||
      !isViewAtTopOfAudioSegments ||
      hasNewerAudioSegments ||
      !searchedFeedId
    ) {
      return;
    }

    const interval = setInterval(async () => {
      try {
        setIsAudioSegmentsPolling(true);
        const newAudioSegments = await pollNewerAudioSegments();
        if (newAudioSegments.length === 0) {
          return;
        }

        const cachedAudioSegments =
          updateCacheWithNewAudioSegments(newAudioSegments);
        if (cachedAudioSegments.length === 0) {
          return;
        }

        const cachedSpeechAudioSegments = cachedAudioSegments.filter(
          (t) => t.classification === AudioClassification.SPEECH
        );

        if (cachedSpeechAudioSegments.length > 0) {
          const message =
            cachedSpeechAudioSegments.length === 1
              ? 'New transcript received'
              : `${cachedSpeechAudioSegments.length} new transcripts received`;
          triggerSnackbar(message);

          if (!document.hasFocus()) {
            setNewMessageCount(
              (prevCount) => prevCount + cachedSpeechAudioSegments.length
            );
          }
        }

        if (!isAudioPlaying && playLatestAudio) {
          const audioToPlay =
            cachedAudioSegments[cachedAudioSegments.length - 1];
          if (audioToPlay.playbackAudioUri) {
            toggleAudio(audioToPlay.id, audioToPlay.playbackAudioUri);
          }
        }
      } catch (error) {
        console.error('Polling error:', error);
      } finally {
        setIsAudioSegmentsPolling(false);
      }
    }, DEFAULT_REFRESH_INTERVAL);

    return () => clearInterval(interval);
  }, [
    isAudioSegmentsSuccess,
    isViewAtTopOfAudioSegments,
    hasNewerAudioSegments,
    searchedFeedId,
    pollNewerAudioSegments,
    updateCacheWithNewAudioSegments,
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

  const ruleIdToNameMap: Map<string, string> = useMemo(() => {
    if (!rules) {
      return new Map<string, string>();
    }
    return new Map(rules.map((rule) => [rule.ruleId, rule.ruleName]));
  }, [rules]);

  // Center the list on a row index, flagging the scroll (and its smooth
  // animation) as programmatic so handleListScrolling doesn't read it as the user.
  const scrollListToIndex = useCallback(
    (index: number, behavior: 'auto' | 'smooth' = 'auto') => {
      programmaticScrollRef.current = true;
      clearTimeout(programmaticScrollTimerRef.current);
      programmaticScrollTimerRef.current = setTimeout(() => {
        programmaticScrollRef.current = false;
      }, PROGRAMMATIC_SCROLL_MAX_MS);
      virtuosoRef.current?.scrollToIndex({ index, align: 'center', behavior });
    },
    []
  );

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
      const index = audioSegments.findIndex(
        (t) =>
          t.id === targetSegmentId ||
          t.bundledSegmentIds?.includes(targetSegmentId)
      );
      if (index !== -1) {
        const timer = setTimeout(() => {
          scrollListToIndex(index);
          hasScrolledToTarget.current = true;
        }, 100);
        return () => clearTimeout(timer);
      }
    }
  }, [
    isAudioSegmentsSuccess,
    targetSegmentId,
    audioSegments,
    scrollListToIndex,
  ]);

  // Shared list-follow: resolve a raw segment id to its consolidated row and
  // center it. Reads the ref so effect callers don't depend on the segment list.
  const scrollListToSegment = useCallback(
    (segmentId: string, behavior: 'auto' | 'smooth' = 'auto') => {
      const index = audioSegmentsRef.current.findIndex(
        (t) => t.id === segmentId || t.bundledSegmentIds?.includes(segmentId)
      );
      if (index !== -1) scrollListToIndex(index, behavior);
    },
    [scrollListToIndex]
  );

  // A scroll we didn't trigger means the user is reading elsewhere; stop
  // auto-following playback until they navigate explicitly again. The flag stays
  // set through a programmatic scroll's whole animation and clears when it settles.
  const handleListScrolling = (isScrolling: boolean) => {
    if (!isScrolling) {
      programmaticScrollRef.current = false;
      return;
    }
    if (!programmaticScrollRef.current) followPlaybackRef.current = false;
  };

  // Mirror of isScrubbed so the playback-follow effect can read the latest value
  // without it being a dependency (which would re-fire on scrub / jump-to-live).
  const isScrubbedRef = useRef(isScrubbed);
  useEffect(() => {
    isScrubbedRef.current = isScrubbed;
  }, [isScrubbed]);

  // Keep the playing transcript in view as playback auto-advances — unless the
  // user has scrubbed the timeline or scrolled the list away to read. Keyed on
  // the playing id alone so polling new segments doesn't re-fire.
  useEffect(() => {
    if (
      !currentlyPlayingSegmentId ||
      isScrubbedRef.current ||
      !followPlaybackRef.current
    ) {
      return;
    }
    scrollListToSegment(currentlyPlayingSegmentId);
  }, [currentlyPlayingSegmentId, scrollListToSegment]);

  // Bring the transcript list to a segment: scroll if it's already loaded, else
  // load the list around its time (the deep-link scroll effect reveals it once
  // loaded) — for clips from the 24h overview the lazy list hasn't reached.
  const navigateListToSegment = (segmentId: string) => {
    const inList = audioSegmentsRef.current.some(
      (t) => t.id === segmentId || t.bundledSegmentIds?.includes(segmentId)
    );
    if (inList) {
      scrollListToSegment(segmentId, 'smooth');
      return;
    }
    const clip = rawAudioSegments.find((s) => s.id === segmentId);
    if (!clip) return;
    // Re-arm the scroll even when segmentId is unchanged (e.g. re-selecting the
    // same clip after jumping to live), since the [targetSegmentId] reset won't.
    hasScrolledToTarget.current = false;
    setSearchParams((prev) => {
      prev.set('segmentId', segmentId);
      prev.set('timestamp', new Date(clip.startTimestamp).getTime().toString());
      return prev;
    });
  };

  const handleClipClick = (segmentId: string) => {
    followPlaybackRef.current = true;
    setHighlightedSegmentId(segmentId);
    navigateListToSegment(segmentId);
  };

  const handleScrubToCenter = (centerMs: number) => {
    followPlaybackRef.current = true;
    // Sets the list anchor only, not the URL timestamp, so the overview stays put.
    scrubToCenter(centerMs);
    setSearchedTimestamp(new Date(centerMs));
  };

  // The oldest clip with audio overlapping the visible window (its left edge),
  // falling back to the newest loaded clip when the window has none.
  const windowFirstPlayableId = (): string | undefined => {
    const windowEnd = windowEndTime ?? maxEnd;
    if (windowEnd != null) {
      const windowStart = windowEnd - windowDurationMs;
      // rawAudioSegments is newest-first, so scan from the end for the oldest.
      for (let i = rawAudioSegments.length - 1; i >= 0; i--) {
        const s = rawAudioSegments[i];
        if (!s.playbackAudioUri) continue;
        const start = new Date(s.startTimestamp).getTime();
        const end = new Date(s.endTimestamp).getTime();
        if (start < windowEnd && end > windowStart) return s.id;
      }
    }
    return audioSegments[0]?.id;
  };

  const handleTogglePlayPause = () => {
    followPlaybackRef.current = true;
    const startingPlayback = !isAudioPlaying;
    // Fallback (nothing highlighted/playing): start at the window's left edge so
    // playback walks forward through what's shown — live, scrubbed, or filtered.
    const targetId = isAudioPlaying
      ? currentlyPlayingSegmentId || highlightedSegmentId
      : highlightedSegmentId ||
        currentlyPlayingSegmentId ||
        windowFirstPlayableId();
    if (!targetId) return;

    // When starting playback, bring the list to the clip so its row highlights.
    const specificSegment = rawAudioSegments.find((s) => s.id === targetId);
    if (specificSegment && specificSegment.playbackAudioUri) {
      toggleAudio(specificSegment.id, specificSegment.playbackAudioUri);
      if (startingPlayback) navigateListToSegment(specificSegment.id);
      return;
    }

    const audioSegment = audioSegments.find(
      (t) => t.id === targetId || t.bundledSegmentIds?.includes(targetId)
    );
    if (audioSegment && audioSegment.playbackAudioUri) {
      toggleAudio(audioSegment.id, audioSegment.playbackAudioUri);
      if (startingPlayback) navigateListToSegment(audioSegment.id);
    }
  };

  const handleRowClick = (segmentId: string) => {
    setHighlightedSegmentId(segmentId);
  };

  const handleFilterByDateTime = (date: Date | null) => {
    setSearchedTimestamp(date);
    setSearchParams((prev) => {
      if (date) {
        prev.set('timestamp', date.getTime().toString());
      } else {
        prev.delete('timestamp');
        // Drop the stale target so a later re-click of the same clip re-navigates.
        prev.delete('segmentId');
      }
      return prev;
    });

    // Clearing the date jumps to live: snap the timeline and list back to the top.
    if (date === null) {
      jumpToLive();
      followPlaybackRef.current = true;
      setTimeout(() => scrollListToIndex(0), 100);
      hasScrolledToTarget.current = false;
    }
  };

  const handleFeedSelect = (feedId: string) => {
    setSearchedFeedId(feedId);
    currentAudio.current?.stop();
    currentAudio.current?.unload();
    handleFilterByDateTime(null);
    setNewMessageCount(0);
    setCurrentlyPlayingSegmentId(null);
    setHighlightedSegmentId(null);
    setIsViewAtTopOfAudioSegments(true);
    setPlaybackEndedForId(null);
    setIsAudioPlaying(false);
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
        audioSegments={rawAudioSegments}
        currentlyPlayingSegmentId={currentlyPlayingSegmentId}
        highlightedSegmentId={highlightedSegmentId}
        onClipClick={handleClipClick}
        isAudioPlaying={isAudioPlaying}
        onTogglePlayPause={handleTogglePlayPause}
        currentAudioRef={currentAudio}
        windowEndTime={windowEndTime}
        windowDurationMs={windowDurationMs}
        rangeStartMs={rangeStartMs}
        maxEnd={maxEnd}
        histogramMarks={histogramMarks}
        onScrubToCenter={handleScrubToCenter}
        isLoading={isAudioSegmentsInitialLoading}
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
          activeWindowTime={activeWindowTime}
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
            groupCounts={groupCounts}
            groupTitles={groupTitles}
            setIsViewAtTopOfAudioSegments={setIsViewAtTopOfAudioSegments}
            onScrollingChange={handleListScrolling}
            hasNewerAudioSegments={hasNewerAudioSegments}
            isFetchingNewerAudioSegments={isFetchingNewerAudioSegments}
            fetchNewerAudioSegments={fetchNewerAudioSegments}
            isAudioSegmentsFetching={isAudioSegmentsFetching}
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
