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
import { AudioClassification } from '@transcription/common';

import { useAuth } from '../../context/AuthContext';
import {
  type AlertFilter,
  useAudioSegments,
} from '../../hooks/useAudioSegments';
import { useAudioTimelineSummary } from '../../hooks/useAudioTimelineSummary';
import {
  type RenderableAudioSegment,
  useConsolidatedAudioSegments,
} from '../../hooks/useConsolidatedAudioSegments';
import { getFeed } from '../../service/getFeed';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { getAudioUrl } from '../../utils/audioUtils';
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
const FEED_POLLING_INTERVAL_MS = 15000; // 15 seconds
// A list scroll within this of one we triggered is ours, not the user's.
const PROGRAMMATIC_SCROLL_WINDOW_MS = 300;

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
  // When we last scrolled the list ourselves, to tell our scrolls from the user's.
  const programmaticListScrollAtRef = useRef(0);

  const currentAudio = useRef<Howl>(null);
  const [playbackEndedForId, setPlaybackEndedForId] = useState<string | null>(
    null
  );
  const [isAudioPlaying, setIsAudioPlaying] = useState(false);

  // A mutable reference to the latest list of audio segments. This prevents stale closures
  // inside the Howl audio lifecycle callbacks (like onend), ensuring continuous playback logic
  // always evaluates against the most up-to-date audio segments list even if it updates mid-playback.
  const audioSegmentsRef = useRef<RenderableAudioSegment[]>([]);

  // Cleanup effect to ensure audio is unloaded when component unmounts
  useEffect(() => {
    return () => {
      currentAudio.current?.unload();
    };
  }, []);

  // Play and pause audio from a URL. `shouldHighlight` is false for automatic
  // playback (polling latest / auto-advance) so it doesn't read as an explicit
  // navigation that would pull the timeline back to live while the user scrubs.
  const toggleAudio = useCallback(
    (segmentId: string, audioUri: string, shouldHighlight = true) => {
      const newAudio = currentlyPlayingSegmentId !== segmentId;

      if (newAudio) {
        if (currentAudio.current) {
          currentAudio.current.off();
          currentAudio.current.unload();
          currentAudio.current = null;
        }
        setCurrentlyPlayingSegmentId(segmentId);
        if (shouldHighlight) setHighlightedSegmentId(segmentId);
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

      // Play is no current audio or changing audio
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

  // Full last-24h overview for the heatmap, loaded independently of the list's
  // lazy pagination. Kicked off before the list so the mini-map fills first.
  const { summarySegments } = useAudioTimelineSummary({
    token,
    searchedFeedId,
    alertFilter,
    isFeedsSuccess,
  });

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
    searchedTimestamp,
    alertFilter,
    isFeedsSuccess,
  });

  const audioSegmentsLastUpdated =
    audioSegmentsDataUpdatedAt && audioSegmentsDataUpdatedAt > 0
      ? audioSegmentsDataUpdatedAt
      : null;

  const audioSegments = useConsolidatedAudioSegments(rawAudioSegments);

  // The summary query doesn't poll, so union it with the polling list to keep the
  // live edge fresh while the summary backfills history. Newest-first, deduped.
  const timelineSegments = useMemo(() => {
    const seenIds = new Set<string>();
    return [...rawAudioSegments, ...summarySegments]
      .filter((segment) => {
        if (seenIds.has(segment.id)) return false;
        seenIds.add(segment.id);
        return true;
      })
      .sort(
        (a, b) =>
          new Date(b.startTimestamp).getTime() -
          new Date(a.startTimestamp).getTime()
      );
  }, [rawAudioSegments, summarySegments]);

  const {
    windowEndTime,
    windowDurationMs,
    isScrubbed,
    rangeStartMs,
    maxEnd,
    miniMapTimes,
    scrubToCenter,
    jumpToLive,
  } = useAudioTimelineWindow({
    audioSegments: timelineSegments,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
  });
  const activeWindowTime = isScrubbed ? windowEndTime : null;

  // Keep the ref in sync with the audio segments so that audio lifecycle callbacks can access the latest list.
  useEffect(() => {
    audioSegmentsRef.current = audioSegments;
  }, [audioSegments]);

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
          toggleAudio(nextSegment.id, nextSegment.playbackAudioUri, false);
          setPlaybackEndedForId(null);
          return;
        }
      }
    }

    // 2. If it was a Speech segment, or the last segment in a silence bundle, advance to the next newer audio segment row
    const currentIndex = audioSegments.findIndex(
      (t) =>
        t.id === playbackEndedForId ||
        t.bundledSegmentIds?.includes(playbackEndedForId)
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
            toggleAudio(firstSegment.id, firstSegment.playbackAudioUri, false);
            setPlaybackEndedForId(null);
            return;
          }
        }
        toggleAudio(
          nextAudioSegment.id,
          nextAudioSegment.playbackAudioUri,
          false
        );
      }
    }

    setPlaybackEndedForId(null);
  }, [playbackEndedForId, audioSegments, rawAudioSegments, toggleAudio]);

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

  /**
   * Background polling effect.
   * Automatically fetches new audio segments every 15 seconds, provided the user is:
   * 1. Scrolled to the top of the view.
   * 2. Looking at the "live" head of the stream (no more un-fetched newer pages available).
   */
  useEffect(() => {
    if (
      // Skip polling if the initial audio segments load hasn't completed yet
      !isAudioSegmentsSuccess ||
      // Skip polling if not viewing at the top of the audio segments to prevent fetching data when the user would not see it.
      // User can always click refresh button if they want to.
      !isViewAtTopOfAudioSegments ||
      // Skip polling if there are older historical pages ahead of us to load.
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

        // Add the audio segments to cache
        const cachedAudioSegments =
          updateCacheWithNewAudioSegments(newAudioSegments);
        if (cachedAudioSegments.length === 0) {
          return;
        }

        const cachedSpeechAudioSegments = cachedAudioSegments.filter(
          (t) => t.classification === AudioClassification.SPEECH
        );

        if (cachedSpeechAudioSegments.length > 0) {
          // Display snackbar indicator that new audio segments were received
          const message =
            cachedSpeechAudioSegments.length === 1
              ? 'New transcript received'
              : `${cachedSpeechAudioSegments.length} new transcripts received`;
          triggerSnackbar(message);

          // Update the new message count if the user is not viewing the screen
          if (!document.hasFocus()) {
            setNewMessageCount(
              (prevCount) => prevCount + cachedSpeechAudioSegments.length
            );
          }
        }

        // Trigger the new audio to play if no audio is currently playing
        if (!isAudioPlaying && playLatestAudio) {
          const audioToPlay =
            cachedAudioSegments[cachedAudioSegments.length - 1];
          if (audioToPlay.playbackAudioUri) {
            toggleAudio(audioToPlay.id, audioToPlay.playbackAudioUri, false);
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
      const index = audioSegments.findIndex(
        (t) =>
          t.id === targetSegmentId ||
          t.bundledSegmentIds?.includes(targetSegmentId)
      );
      if (index !== -1) {
        const timer = setTimeout(() => {
          programmaticListScrollAtRef.current = Date.now();
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

  // Shared list-follow: resolve a raw segment id to its consolidated row and
  // center it. Reads the ref so effect callers don't depend on the segment list.
  const scrollListToSegment = useCallback(
    (segmentId: string, behavior: 'auto' | 'smooth' = 'auto') => {
      const index = audioSegmentsRef.current.findIndex(
        (t) => t.id === segmentId || t.bundledSegmentIds?.includes(segmentId)
      );
      if (index !== -1) {
        programmaticListScrollAtRef.current = Date.now();
        virtuosoRef.current?.scrollToIndex({
          index,
          align: 'center',
          behavior,
        });
      }
    },
    []
  );

  // A scroll we didn't trigger means the user is reading elsewhere; stop
  // auto-following playback until they navigate explicitly again.
  const handleListScrolling = (isScrolling: boolean) => {
    if (
      isScrolling &&
      Date.now() - programmaticListScrollAtRef.current >
        PROGRAMMATIC_SCROLL_WINDOW_MS
    ) {
      followPlaybackRef.current = false;
    }
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

  const handleClipClick = (segmentId: string) => {
    followPlaybackRef.current = true;
    setHighlightedSegmentId(segmentId);

    const inList = audioSegmentsRef.current.some(
      (t) => t.id === segmentId || t.bundledSegmentIds?.includes(segmentId)
    );
    if (inList) {
      scrollListToSegment(segmentId, 'smooth');
      return;
    }

    // The clip is from the 24h overview, older than the lazily-loaded list.
    // Navigate the list to its time so it loads and the deep-link effect scrolls
    // to it (the timeline already shows it via the summary union).
    const clip = timelineSegments.find((s) => s.id === segmentId);
    if (!clip) return;
    setSearchParams((prev) => {
      prev.set('segmentId', segmentId);
      prev.set('timestamp', new Date(clip.startTimestamp).getTime().toString());
      return prev;
    });
  };

  const handleScrubToCenter = (centerMs: number) => {
    followPlaybackRef.current = true;
    const segmentId = scrubToCenter(centerMs);
    if (segmentId) scrollListToSegment(segmentId);
  };

  const handleTogglePlayPause = () => {
    followPlaybackRef.current = true;
    const targetId = isAudioPlaying
      ? currentlyPlayingSegmentId || highlightedSegmentId
      : highlightedSegmentId ||
        currentlyPlayingSegmentId ||
        audioSegments[0]?.id;
    if (!targetId) return;

    // The timeline can highlight clips from the 24h overview that the lazy list
    // hasn't loaded, so fall back to the union before the consolidated list.
    const specificSegment =
      rawAudioSegments.find((s) => s.id === targetId) ??
      timelineSegments.find((s) => s.id === targetId);
    if (specificSegment && specificSegment.playbackAudioUri) {
      toggleAudio(specificSegment.id, specificSegment.playbackAudioUri);
      return;
    }

    const audioSegment = audioSegments.find(
      (t) => t.id === targetId || t.bundledSegmentIds?.includes(targetId)
    );
    if (audioSegment && audioSegment.playbackAudioUri) {
      toggleAudio(audioSegment.id, audioSegment.playbackAudioUri);
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
      }
      return prev;
    });

    // Given that clearing the date effectively jumps to live, we will
    // navigate to the top of the table in case the user is scrolled
    // down in the table, and snap the audio timeline back to the live edge.
    if (date === null) {
      jumpToLive();
      followPlaybackRef.current = true;
      setTimeout(() => {
        programmaticListScrollAtRef.current = Date.now();
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
    setCurrentlyPlayingSegmentId(null);
    setHighlightedSegmentId(null);
    setIsViewAtTopOfAudioSegments(true);
    setPlaybackEndedForId(null);
    setIsAudioPlaying(false);
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
        audioSegments={timelineSegments}
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
        miniMapTimes={miniMapTimes}
        onScrubToCenter={handleScrubToCenter}
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
