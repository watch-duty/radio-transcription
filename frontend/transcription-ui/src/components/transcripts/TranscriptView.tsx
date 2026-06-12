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
import {
  type AlertFilter,
  useAudioSegments,
} from '../../hooks/useAudioSegments';
import { useConsolidatedAudioSegments } from '../../hooks/useConsolidatedAudioSegments';
import { getFeed } from '../../service/getFeed';
import { listFeeds } from '../../service/listFeeds';
import { listRules } from '../../service/listRules';
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

const DEFAULT_REFRESH_INTERVAL = 10000;
const FEED_POLLING_INTERVAL_MS = 15000; // 15 seconds

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
  const transcriptsRef = useRef<AudioSegment[]>([]);

  // Cleanup effect to ensure audio is unloaded when component unmounts
  useEffect(() => {
    return () => {
      currentAudio.current?.unload();
    };
  }, []);

  // Play and pause audio from a URL.
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
            const currentTranscripts = transcriptsRef.current;
            const currentIndex = currentTranscripts.findIndex(
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

  const {
    rawAudioSegments,
    loadOlderTranscripts: fetchOlderTranscripts,
    loadNewerTranscripts: fetchNewerTranscripts,
    hasOlderTranscripts,
    hasNewerTranscripts,
    isTranscriptsSuccess,
    transcriptsError,
    transcriptsDataUpdatedAt,
    isFetchingNewerTranscripts,
    isFetchingOlderTranscripts,
    pollNewerTranscripts,
    updateCacheWithNewTranscripts,
    isLoading: isTranscriptsInitialLoading,
    isFetching: isTranscriptsFetching,
  } = useAudioSegments({
    token,
    searchedFeedId,
    searchedTimestamp,
    alertFilter,
    isFeedsSuccess,
  });

  const transcriptsLastUpdated =
    transcriptsDataUpdatedAt && transcriptsDataUpdatedAt > 0
      ? transcriptsDataUpdatedAt
      : null;

  const transcripts = useConsolidatedAudioSegments(rawAudioSegments);

  // Keep the ref in sync with the transcripts so that audio lifecycle callbacks can access the latest list.
  useEffect(() => {
    transcriptsRef.current = transcripts;
  }, [transcripts]);

  // Handles continuous auto-play by advancing to the next newer transcript when the current audio finishes.
  // Since the transcript list is sorted newest-first, the next transmission in time is at `currentIndex - 1`.
  useEffect(() => {
    if (!playbackEndedForId) return;

    // 1. First check if the ended segment was part of a silence bundle, and if there is a next newer segment in that same bundle!
    const parentBundle = transcripts.find(
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

    // 2. If it was a Speech segment, or the last segment in a silence bundle, advance to the next newer transcript row
    const currentIndex = transcripts.findIndex(
      (t) =>
        t.id === playbackEndedForId ||
        t.bundledSegmentIds?.includes(playbackEndedForId)
    );

    if (currentIndex > 0) {
      const nextTranscript = transcripts[currentIndex - 1];
      if (nextTranscript.playbackAudioUri) {
        // If the next transcript is a silence bundle, play its first segment
        if (
          nextTranscript.isSilenceBundle &&
          nextTranscript.bundledSegmentIds &&
          nextTranscript.bundledSegmentIds.length > 0
        ) {
          const firstId = nextTranscript.bundledSegmentIds[0];
          const firstSegment = rawAudioSegments.find((s) => s.id === firstId);
          if (firstSegment && firstSegment.playbackAudioUri) {
            toggleAudio(firstSegment.id, firstSegment.playbackAudioUri);
            setPlaybackEndedForId(null);
            return;
          }
        }
        toggleAudio(nextTranscript.id, nextTranscript.playbackAudioUri);
      }
    }

    setPlaybackEndedForId(null);
  }, [playbackEndedForId, transcripts, rawAudioSegments, toggleAudio]);

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

  /**
   * Background polling effect.
   * Automatically fetches new transcripts every 15 seconds, provided the user is:
   * 1. Scrolled to the top of the view.
   * 2. Looking at the "live" head of the stream (no more un-fetched newer pages available).
   */
  useEffect(() => {
    if (
      // Skip polling if the initial transcript load hasn't completed yet
      !isTranscriptsSuccess ||
      // Skip polling if not viewing at the top of the transcripts to prevent fetching data when the user would not see it.
      // User can always click refresh button if they want to.
      !isViewAtTopOfTranscripts ||
      // Skip polling if there are older historical pages ahead of us to load.
      hasNewerTranscripts ||
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

        const cachedSpeechTranscripts = cachedTranscripts.filter(
          (t) => t.classification === AudioClassification.SPEECH
        );

        if (cachedSpeechTranscripts.length > 0) {
          // Display snackbar indicator that new transcripts were received
          const message =
            cachedSpeechTranscripts.length === 1
              ? 'New transcript received'
              : `${cachedSpeechTranscripts.length} new transcripts received`;
          triggerSnackbar(message);

          // Update the new message count if the user is not viewing the screen
          if (!document.hasFocus()) {
            setNewMessageCount(
              (prevCount) => prevCount + cachedSpeechTranscripts.length
            );
          }
        }

        // Trigger the new audio to play if no audio is currently playing
        if (!isAudioPlaying && playLatestAudio) {
          const audioToPlay = cachedTranscripts[cachedTranscripts.length - 1];
          if (audioToPlay.playbackAudioUri) {
            toggleAudio(audioToPlay.id, audioToPlay.playbackAudioUri);
          }
        }
      } catch (error) {
        console.error('Polling error:', error);
      } finally {
        setIsTranscriptsPolling(false);
      }
    }, DEFAULT_REFRESH_INTERVAL);

    return () => clearInterval(interval);
  }, [
    isTranscriptsSuccess,
    isViewAtTopOfTranscripts,
    hasNewerTranscripts,
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
  }, [targetSegmentId]);

  useEffect(() => {
    if (
      isTranscriptsSuccess &&
      targetSegmentId &&
      transcripts.length > 0 &&
      !hasScrolledToTarget.current
    ) {
      const index = transcripts.findIndex(
        (t) =>
          t.id === targetSegmentId ||
          t.bundledSegmentIds?.includes(targetSegmentId)
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
  }, [isTranscriptsSuccess, targetSegmentId, transcripts]);

  const handleClipClick = (segmentId: string) => {
    const index = transcripts.findIndex(
      (t) => t.id === segmentId || t.bundledSegmentIds?.includes(segmentId)
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

  const handleTogglePlayPause = () => {
    const targetId = isAudioPlaying
      ? currentlyPlayingSegmentId || highlightedSegmentId
      : highlightedSegmentId || currentlyPlayingSegmentId || transcripts[0]?.id;
    if (!targetId) return;

    const specificSegment = rawAudioSegments.find((s) => s.id === targetId);
    if (specificSegment && specificSegment.playbackAudioUri) {
      toggleAudio(specificSegment.id, specificSegment.playbackAudioUri);
      return;
    }

    const transcript = transcripts.find(
      (t) => t.id === targetId || t.bundledSegmentIds?.includes(targetId)
    );
    if (transcript && transcript.playbackAudioUri) {
      toggleAudio(transcript.id, transcript.playbackAudioUri);
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
    setCurrentlyPlayingSegmentId(null);
    setHighlightedSegmentId(null);
    setIsViewAtTopOfTranscripts(true);
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
        transcripts={rawAudioSegments}
        currentlyPlayingSegmentId={currentlyPlayingSegmentId}
        highlightedSegmentId={highlightedSegmentId}
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
            currentlyPlayingSegmentId={currentlyPlayingSegmentId}
            highlightedSegmentId={highlightedSegmentId}
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
