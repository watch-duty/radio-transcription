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

import {
  type PlaybackController,
  WebAudioPlayer,
  createAudioContext,
} from '../../audio/WebAudioPlayer';
import { useAuth } from '../../context/AuthContext';
import {
  type AlertFilter,
  useAudioSegments,
} from '../../hooks/useAudioSegments';
import {
  type RenderableAudioSegment,
  useConsolidatedAudioSegments,
} from '../../hooks/useConsolidatedAudioSegments';
import { getFeed } from '../../service/getFeed';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
import { getAudioUrl } from '../../utils/audioUtils';
import { AudioControl } from '../audio/AudioControl';
import AudioDisplay from '../audio/AudioDisplay';
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

// Matches a consolidated segment by its own id or, for silence bundles, by any
// of the raw segment ids it contains.
function matchesSegmentId(
  segment: RenderableAudioSegment,
  id: string
): boolean {
  return (
    segment.id === id || (segment.bundledSegmentIds?.includes(id) ?? false)
  );
}

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

  const audioContextRef = useRef<AudioContext | null>(null);
  const playerRef = useRef<WebAudioPlayer | null>(null);
  const currentAudio = useRef<PlaybackController | null>(null);
  const [playbackEndedForId, setPlaybackEndedForId] = useState<string | null>(
    null
  );
  const [isAudioPlaying, setIsAudioPlaying] = useState(false);

  // A mutable reference to the latest list of audio segments. This prevents stale closures
  // inside the audio lifecycle callbacks (like onEnd), ensuring continuous playback logic
  // always evaluates against the most up-to-date audio segments list even if it updates mid-playback.
  const audioSegmentsRef = useRef<AudioSegment[]>([]);

  useEffect(() => {
    return () => {
      audioContextRef.current?.close().catch(() => {});
      audioContextRef.current = null;
      playerRef.current = null;
      currentAudio.current = null;
    };
  }, []);

  // Play and pause audio from a URL.
  const toggleAudio = useCallback(
    (segmentId: string, audioUri: string) => {
      // Lazy-build on first play so the AudioContext is created inside a user gesture.
      const context = (audioContextRef.current ??= createAudioContext());
      const player = (playerRef.current ??= new WebAudioPlayer(context));
      player.resume();

      const newAudio = currentlyPlayingSegmentId !== segmentId;

      if (newAudio) {
        currentAudio.current?.unload();
        currentAudio.current = null;
        setCurrentlyPlayingSegmentId(segmentId);
        setHighlightedSegmentId(segmentId);
      }

      if (!currentAudio.current) {
        currentAudio.current = player.load(getAudioUrl(audioUri), {
          onPlay: () => setIsAudioPlaying(true),
          onPause: () => setIsAudioPlaying(false),
          onError: () => setIsAudioPlaying(false),
          onEnd: () => {
            const currentAudioSegments = audioSegmentsRef.current;
            const currentIndex = currentAudioSegments.findIndex(
              (t) => t.id === segmentId
            );
            const hasNext = currentIndex > 0;

            if (!hasNext) {
              setIsAudioPlaying(false);
            }

            setPlaybackEndedForId(segmentId);
            currentAudio.current = null;
          },
        });
      }

      if (!isAudioPlaying || newAudio) {
        currentAudio.current.play();
      } else {
        currentAudio.current.pause();
      }
    },
    [currentlyPlayingSegmentId, isAudioPlaying]
  );

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

      if (!isAudioPlaying && playLatestAudio) {
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
          toggleAudio(nextSegment.id, nextSegment.playbackAudioUri);
          setPlaybackEndedForId(null);
          return;
        }
      }
    }

    // 2. If it was a Speech segment, or the last segment in a silence bundle, advance to the next newer audio segment row
    const currentIndex = audioSegments.findIndex((t) =>
      matchesSegmentId(t, playbackEndedForId)
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
        matchesSegmentId(t, targetSegmentId)
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
    const index = audioSegments.findIndex((t) =>
      matchesSegmentId(t, segmentId)
    );
    if (index !== -1) {
      virtuosoRef.current?.scrollToIndex({
        index,
        align: 'center',
        behavior: 'smooth',
      });
    }
    setHighlightedSegmentId(segmentId);
  };

  const skipToNext = () => {
    const targetId = isAudioPlaying
      ? currentlyPlayingSegmentId || highlightedSegmentId
      : highlightedSegmentId ||
        currentlyPlayingSegmentId ||
        audioSegments[0]?.id;
    if (!targetId) return;

    const specificSegmentIdx = rawAudioSegments.findIndex(
      (s) => s.id === targetId
    );
    if (specificSegmentIdx !== -1) {
      // Find the next newer raw segment (index decreasing) that has a playbackAudioUri
      let nextIdx = specificSegmentIdx - 1;
      while (nextIdx >= 0) {
        const nextSegment = rawAudioSegments[nextIdx];
        if (nextSegment?.playbackAudioUri) {
          toggleAudio(nextSegment.id, nextSegment.playbackAudioUri);

          // Scroll the corresponding consolidated row into view
          const displayIdx = audioSegments.findIndex((t) =>
            matchesSegmentId(t, nextSegment.id)
          );
          if (displayIdx !== -1) {
            virtuosoRef.current?.scrollToIndex({
              index: displayIdx,
              align: 'center',
              behavior: 'smooth',
            });
          }
          return;
        }
        nextIdx--;
      }
    }
  };

  const skipToPrevious = () => {
    const targetId = isAudioPlaying
      ? currentlyPlayingSegmentId || highlightedSegmentId
      : highlightedSegmentId ||
        currentlyPlayingSegmentId ||
        audioSegments[0]?.id;
    if (!targetId) return;

    const specificSegmentIdx = rawAudioSegments.findIndex(
      (s) => s.id === targetId
    );
    if (specificSegmentIdx !== -1) {
      // Find the next older raw segment (index increasing) that has a playbackAudioUri
      let prevIdx = specificSegmentIdx + 1;
      while (prevIdx < rawAudioSegments.length) {
        const previousSegment = rawAudioSegments[prevIdx];
        if (previousSegment?.playbackAudioUri) {
          toggleAudio(previousSegment.id, previousSegment.playbackAudioUri);

          // Scroll the corresponding consolidated row into view
          const displayIdx = audioSegments.findIndex((t) =>
            matchesSegmentId(t, previousSegment.id)
          );
          if (displayIdx !== -1) {
            virtuosoRef.current?.scrollToIndex({
              index: displayIdx,
              align: 'center',
              behavior: 'smooth',
            });
          }
          return;
        }
        prevIdx++;
      }
    }
  };

  const skipToNextSpeech = () => {
    const targetId = isAudioPlaying
      ? currentlyPlayingSegmentId || highlightedSegmentId
      : highlightedSegmentId ||
        currentlyPlayingSegmentId ||
        audioSegments[0]?.id;
    if (!targetId) return;

    const currentConsolidatedIdx = audioSegments.findIndex((t) =>
      matchesSegmentId(t, targetId)
    );
    if (currentConsolidatedIdx !== -1) {
      // Find the next newer consolidated segment (index decreasing) that is a speech segment and has a playbackAudioUri
      let nextIdx = currentConsolidatedIdx - 1;
      while (nextIdx >= 0) {
        const nextSegment = audioSegments[nextIdx];
        if (!nextSegment.isSilenceBundle && nextSegment.playbackAudioUri) {
          toggleAudio(nextSegment.id, nextSegment.playbackAudioUri);

          virtuosoRef.current?.scrollToIndex({
            index: nextIdx,
            align: 'center',
            behavior: 'smooth',
          });
          return;
        }
        nextIdx--;
      }
    }
  };

  const skipToPreviousSpeech = () => {
    const targetId = isAudioPlaying
      ? currentlyPlayingSegmentId || highlightedSegmentId
      : highlightedSegmentId ||
        currentlyPlayingSegmentId ||
        audioSegments[0]?.id;
    if (!targetId) return;

    const currentConsolidatedIdx = audioSegments.findIndex((t) =>
      matchesSegmentId(t, targetId)
    );
    if (currentConsolidatedIdx !== -1) {
      // Find the next older consolidated segment (index increasing) that is a speech segment and has a playbackAudioUri
      let prevIdx = currentConsolidatedIdx + 1;
      while (prevIdx < audioSegments.length) {
        const prevSegment = audioSegments[prevIdx];
        if (!prevSegment.isSilenceBundle && prevSegment.playbackAudioUri) {
          toggleAudio(prevSegment.id, prevSegment.playbackAudioUri);

          virtuosoRef.current?.scrollToIndex({
            index: prevIdx,
            align: 'center',
            behavior: 'smooth',
          });
          return;
        }
        prevIdx++;
      }
    }
  };

  const skipTime = (offsetSeconds: number) => {
    // Determine the active segment (playing or highlighted)
    const activeId = currentlyPlayingSegmentId || highlightedSegmentId;
    if (!activeId) return;

    const currentIdx = rawAudioSegments.findIndex((s) => s.id === activeId);
    if (currentIdx === -1) return;
    const activeSegment = rawAudioSegments[currentIdx];

    // Get current playback time (default to 0 if paused/not loaded)
    const currentTime = currentAudio.current
      ? currentAudio.current.getCurrentTime()
      : 0;

    const getSegmentDuration = (s: typeof activeSegment) =>
      (new Date(s.endTimestamp).getTime() -
        new Date(s.startTimestamp).getTime()) /
      1000;

    const activeDuration = getSegmentDuration(activeSegment);
    const targetTime = currentTime + offsetSeconds;

    // Helper to scroll a segment into view in the transcript
    const scrollSegmentIntoView = (segmentId: string) => {
      const displayIdx = audioSegments.findIndex((t) =>
        matchesSegmentId(t, segmentId)
      );
      if (displayIdx !== -1) {
        virtuosoRef.current?.scrollToIndex({
          index: displayIdx,
          align: 'center',
          behavior: 'smooth',
        });
      }
    };

    // CASE 1: Within the bounds of the current segment
    if (targetTime >= 0 && targetTime <= activeDuration) {
      if (!currentAudio.current && activeSegment.playbackAudioUri) {
        // If paused/not loaded, load it first
        toggleAudio(activeSegment.id, activeSegment.playbackAudioUri);
      }
      currentAudio.current?.setCurrentTime(targetTime);
      return;
    }

    // CASE 2: Overshot backwards (Replay / older segments)
    if (targetTime < 0) {
      let remainingOvershoot = -targetTime;
      let nextIdx = currentIdx + 1; // Older segments have higher indexes

      while (nextIdx < rawAudioSegments.length) {
        const segment = rawAudioSegments[nextIdx];
        if (segment.playbackAudioUri) {
          const duration = getSegmentDuration(segment);
          if (duration >= remainingOvershoot) {
            // Target is inside this older segment, counting from its end
            const seekTime = duration - remainingOvershoot;
            toggleAudio(segment.id, segment.playbackAudioUri);
            currentAudio.current?.setCurrentTime(seekTime);
            scrollSegmentIntoView(segment.id);
            return;
          } else {
            // Consume the entire segment and keep going backward
            remainingOvershoot -= duration;
          }
        }
        nextIdx++;
      }

      // Fallback: If we overshot the oldest segment, play the oldest from the start (0)
      for (let i = rawAudioSegments.length - 1; i >= 0; i--) {
        const segment = rawAudioSegments[i];
        if (segment.playbackAudioUri) {
          toggleAudio(segment.id, segment.playbackAudioUri);
          currentAudio.current?.setCurrentTime(0);
          scrollSegmentIntoView(segment.id);
          return;
        }
      }
    }

    // CASE 3: Overshot forwards (Forward / newer segments)
    if (targetTime > activeDuration) {
      let remainingOvershoot = targetTime - activeDuration;
      let nextIdx = currentIdx - 1; // Newer segments have lower indexes

      while (nextIdx >= 0) {
        const segment = rawAudioSegments[nextIdx];
        if (segment.playbackAudioUri) {
          const duration = getSegmentDuration(segment);
          if (duration >= remainingOvershoot) {
            // Target is inside this newer segment, counting from its start
            const seekTime = remainingOvershoot;
            toggleAudio(segment.id, segment.playbackAudioUri);
            currentAudio.current?.setCurrentTime(seekTime);
            scrollSegmentIntoView(segment.id);
            return;
          } else {
            // Consume the entire segment and keep going forward
            remainingOvershoot -= duration;
          }
        }
        nextIdx--;
      }

      // Fallback: If we overshot the newest segment, seek to the end of the newest segment
      for (let i = 0; i < rawAudioSegments.length; i++) {
        const segment = rawAudioSegments[i];
        if (segment.playbackAudioUri) {
          const duration = getSegmentDuration(segment);
          toggleAudio(segment.id, segment.playbackAudioUri);
          currentAudio.current?.setCurrentTime(duration);
          scrollSegmentIntoView(segment.id);
          return;
        }
      }
    }
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
      matchesSegmentId(t, targetId)
    );
    if (audioSegment && audioSegment.playbackAudioUri) {
      toggleAudio(audioSegment.id, audioSegment.playbackAudioUri);
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
      matchesSegmentId(s, anchorId)
    );
    if (prependedCount > 0) {
      setFirstItemIndex((prev) => prev - prependedCount);
    }
  }, [isFetchingNewerAudioSegments, audioSegments]);

  // A different feed / timestamp / alert filter replaces the list wholesale
  // rather than prepending, so reset the anchoring baseline.
  useEffect(() => {
    setFirstItemIndex(VIRTUOSO_START_INDEX);
    newerLoadAnchorId.current = null;
    hasScrolledAwayFromTop.current = false;
  }, [searchedFeedId, searchedTimestamp, alertFilter]);

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
    hasScrolledAwayFromTop.current = false;
  };

  const handleFeedSelect = (feedId: string) => {
    setSearchedFeedId(feedId);
    playerRef.current?.stop();
    currentAudio.current = null;
    // Reset all state
    handleFilterByDateTime(null);
    setNewMessageCount(0);
    setCurrentlyPlayingSegmentId(null);
    setHighlightedSegmentId(null);
    setIsViewAtTopOfAudioSegments(true);
    setPlaybackEndedForId(null);
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

      <AudioControl
        sx={{ mt: 1 }}
        isAudioPlaying={isAudioPlaying}
        onTogglePlayPause={handleTogglePlayPause}
        onSkipToNext={skipToNext}
        onSkipToPrevious={skipToPrevious}
        onFastForward={skipToNextSpeech}
        onFastRewind={skipToPreviousSpeech}
        onReplay5={() => skipTime(-5)}
        onForward5={() => skipTime(5)}
        playLatestAudio={playLatestAudio}
        onChangePlayLatestAudio={setPlayLatestAudio}
        disableControls={rawAudioSegments.length === 0}
        disableCheckbox={!searchedFeed}
      />

      <AudioDisplay
        audioSegments={rawAudioSegments}
        currentlyPlayingSegmentId={currentlyPlayingSegmentId}
        highlightedSegmentId={highlightedSegmentId}
        onClipClick={handleClipClick}
        isAudioPlaying={isAudioPlaying}
        currentAudioRef={currentAudio}
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
