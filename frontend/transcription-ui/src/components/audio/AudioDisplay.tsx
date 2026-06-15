import React, { useEffect, useMemo, useState } from 'react';

import type { Howl } from 'howler';

import PauseIcon from '@mui/icons-material/PauseCircleFilledOutlined';
import PlayArrowIcon from '@mui/icons-material/PlayCircleFilledOutlined';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import { type Theme, useTheme } from '@mui/material/styles';
import { type AudioSegment } from '@transcription/common';
import WavesurferPlayer from '@wavesurfer/react';

import { findEvaluationAnnotationData } from '../../utils/annotationUtils';
import { getAudioUrl } from '../../utils/audioUtils';
import { MAX_WINDOW_DURATION_MS } from '../../utils/timeUtils';
import { CustomAlertIcon } from '../common/AlertIcon';
import { usePeaksDecodeQueue } from './usePeaksDecodeQueue';

interface AudioDisplayProps {
  audioSegments: AudioSegment[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  onClipClick: (segmentId: string) => void;
  userDuration?: string | null;
  isAudioPlaying: boolean;
  onTogglePlayPause: () => void;
  currentTimeSeconds?: number;
  currentAudioRef?: React.RefObject<Howl | null>;
}

const PLAYING_CURSOR_WIDTH_PX = 2;

const formatTime = (timestamp: number) => {
  const date = new Date(timestamp);
  return date.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
};

interface TimelineClipProps {
  clip: {
    id: string;
    url: string;
    left: number;
    width: number;
    isAudioPlaying: boolean;
    isHighlighted: boolean;
    hasAlert: boolean;
    // Precomputed by the decode queue; absent until a clip's audio is decoded.
    peaks?: (Float32Array | number[])[];
    duration?: number;
  };
  onClipClick: (segmentId: string) => void;
  isDarkTheme: boolean;
  theme: Theme;
  currentTimeSeconds?: number;
}

const TimelineClip = React.memo(
  ({
    clip,
    onClipClick,
    isDarkTheme,
    theme,
    currentTimeSeconds,
  }: TimelineClipProps) => {
    // Render from cached peaks, never a url: 50+ clips each fetching+decoding
    // their own audio via a media element issues a storm of range reads. The
    // queue decodes once; until then, a placeholder bar.
    const renderWaveform = !!clip.peaks;

    // Playback position within the clip, for the cursor overlay.
    const cursorLeftPct =
      clip.isAudioPlaying && currentTimeSeconds !== undefined && clip.duration
        ? Math.min(100, Math.max(0, (currentTimeSeconds / clip.duration) * 100))
        : null;

    return (
      <Box
        onClick={() => onClipClick(clip.id)}
        sx={{
          position: 'absolute',
          left: `${clip.left}%`,
          width: `${clip.width}%`,
          height: '100%',
          bgcolor:
            clip.isAudioPlaying || clip.isHighlighted
              ? isDarkTheme
                ? 'rgba(255, 255, 255, 0.1)'
                : 'rgba(0, 0, 0, 0.05)'
              : 'transparent',
          cursor: 'pointer',
          '&:hover': {
            bgcolor:
              clip.isAudioPlaying || clip.isHighlighted
                ? isDarkTheme
                  ? 'rgba(255, 255, 255, 0.2)'
                  : 'rgba(0, 0, 0, 0.1)'
                : isDarkTheme
                  ? 'rgba(255, 255, 255, 0.03)'
                  : 'rgba(0, 0, 0, 0.03)',
          },
        }}
      >
        {clip.hasAlert && (
          <CustomAlertIcon
            color="warning"
            fontSize="medium"
            data-testid="warning-icon"
            sx={{
              position: 'absolute',
              // This centers the icon over the audio start, rather than left-aligned at the audio start.
              left: -11,
              // This provides enough buffer to move the icon on top of the clip view rather than on it.
              top: -25,
              zIndex: 1,
              borderRadius: '50%',
            }}
          />
        )}
        {renderWaveform ? (
          <WavesurferPlayer
            peaks={clip.peaks}
            duration={clip.duration}
            waveColor={theme.palette.text.secondary}
            progressColor={theme.palette.text.primary}
            cursorWidth={0}
            barWidth={0.5}
            barGap={0.5}
            height={60}
            interact={false}
          />
        ) : (
          <Box
            sx={{
              position: 'absolute',
              top: '50%',
              left: 0,
              right: 0,
              height: '2px',
              transform: 'translateY(-50%)',
              bgcolor: 'text.secondary',
              opacity: 0.35,
            }}
          />
        )}
        {cursorLeftPct !== null && (
          <Box
            data-testid="playing-cursor"
            // Inline style: the position updates every animation frame.
            style={{ left: `${cursorLeftPct}%` }}
            sx={{
              position: 'absolute',
              top: 0,
              bottom: 0,
              width: `${PLAYING_CURSOR_WIDTH_PX}px`,
              bgcolor: 'error.main',
              pointerEvents: 'none',
            }}
          />
        )}
      </Box>
    );
  },
  (prevProps, nextProps) => {
    // This prevents the clip from re-rendering when the parent component re-renders,
    // unless the props actually change.
    return (
      prevProps.clip.id === nextProps.clip.id &&
      prevProps.clip.url === nextProps.clip.url &&
      prevProps.clip.left === nextProps.clip.left &&
      prevProps.clip.width === nextProps.clip.width &&
      prevProps.clip.isAudioPlaying === nextProps.clip.isAudioPlaying &&
      prevProps.clip.isHighlighted === nextProps.clip.isHighlighted &&
      prevProps.clip.hasAlert === nextProps.clip.hasAlert &&
      prevProps.clip.peaks === nextProps.clip.peaks &&
      prevProps.clip.duration === nextProps.clip.duration &&
      prevProps.isDarkTheme === nextProps.isDarkTheme &&
      prevProps.theme === nextProps.theme &&
      prevProps.currentTimeSeconds === nextProps.currentTimeSeconds
    );
  }
);

/**
 * Determines whether the timeline window was aligned to the previous live head.
 *
 * If it was aligned to the head (or if there is no previous state/window bounds),
 * the viewport should automatically advance forward to show newly arriving segments.
 * If the user has scrolled back in time, we should keep the viewport anchored at their scroll position.
 */
function isViewportAlignedToHead(
  windowEndTime: number | null,
  prevFirstTranscriptEndTimestamp: string | null,
  isInitialLoad: boolean
): boolean {
  if (isInitialLoad || !windowEndTime || !prevFirstTranscriptEndTimestamp) {
    return true;
  }
  const prevHeadTime = new Date(prevFirstTranscriptEndTimestamp).getTime();
  const timeDifferenceMs = Math.abs(windowEndTime - prevHeadTime);
  // Allow a 1-second tolerance for floating point rounding in time conversions
  return timeDifferenceMs < 1000;
}

export function AudioDisplay({
  audioSegments,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
  onClipClick,
  userDuration,
  isAudioPlaying,
  onTogglePlayPause,
  currentTimeSeconds,
  currentAudioRef,
}: AudioDisplayProps) {
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';

  const { enqueueDecode, clearPending, getPeaks } = usePeaksDecodeQueue();

  const [localCurrentTimeSeconds, setLocalCurrentTimeSeconds] =
    useState<number>(0);

  // Poll current playback progress when audio is playing
  useEffect(() => {
    if (
      currentTimeSeconds !== undefined ||
      !isAudioPlaying ||
      !currentlyPlayingSegmentId ||
      !currentAudioRef?.current
    ) {
      return;
    }

    let animationFrameId: number;

    const updateProgress = () => {
      if (currentAudioRef.current) {
        const seek = currentAudioRef.current.seek();
        // seek could be the Howl instance if audio isn't yet loaded, so we should guard against that.
        if (typeof seek === 'number') {
          setLocalCurrentTimeSeconds(seek);
        }
      }
      animationFrameId = requestAnimationFrame(updateProgress);
    };

    updateProgress();

    return () => {
      cancelAnimationFrame(animationFrameId);
    };
  }, [
    isAudioPlaying,
    currentlyPlayingSegmentId,
    currentAudioRef,
    currentTimeSeconds,
  ]);

  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);

  const [prevFirstAudioSegmentId, setPrevFirstAudioSegmentId] = useState<
    string | null
  >(null);
  const [
    prevFirstAudioSegmentEndTimestamp,
    setPrevFirstAudioSegmentEndTimestamp,
  ] = useState<string | null>(null);
  const [prevPlayingId, setPrevPlayingId] = useState<string | null>(null);
  const [prevHighlightedId, setPrevHighlightedId] = useState<string | null>(
    null
  );
  const [prevUserDuration, setPrevUserDuration] = useState<string | null>(
    userDuration ?? null
  );

  const windowDurationMs = useMemo(() => {
    if (!userDuration) return MAX_WINDOW_DURATION_MS;
    const parsed = parseFloat(userDuration);
    if (isNaN(parsed) || parsed <= 0) return MAX_WINDOW_DURATION_MS;
    const userMs = parsed * 2 * 60 * 1000;
    return Math.min(MAX_WINDOW_DURATION_MS, userMs);
  }, [userDuration]);

  const firstAudioSegment = audioSegments[0];
  const firstAudioSegmentId = firstAudioSegment?.id || null;
  const firstAudioSegmentEndTimestamp = firstAudioSegment?.endTimestamp || null;

  // Check if a new segment has been added to the top of the feed
  const isNewFirstAudioSegment =
    firstAudioSegmentId !== prevFirstAudioSegmentId;

  // Check if the current top audio segment has been extended (e.g. an ongoing silence bundle).
  // When a silence bundle is extended, its ID remains the same but its end timestamp advances.
  const isFirstAudioSegmentExtended =
    firstAudioSegmentEndTimestamp !== prevFirstAudioSegmentEndTimestamp;

  const shouldUpdateWindow =
    isNewFirstAudioSegment || isFirstAudioSegmentExtended;

  if (shouldUpdateWindow) {
    const wasAlignedToHead = isViewportAlignedToHead(
      windowEndTime,
      prevFirstAudioSegmentEndTimestamp,
      !prevFirstAudioSegmentId
    );

    setPrevFirstAudioSegmentId(firstAudioSegmentId);
    setPrevFirstAudioSegmentEndTimestamp(firstAudioSegmentEndTimestamp);

    if (wasAlignedToHead) {
      setWindowEndTime(
        firstAudioSegmentEndTimestamp
          ? new Date(firstAudioSegmentEndTimestamp).getTime()
          : null
      );
    }
    setPrevPlayingId(null); // Force re-check of bounds
  }

  const playingId = currentlyPlayingSegmentId || null;

  // Shift windowEndTime when playing or highlighted transcript goes out of bounds
  if (
    playingId !== prevPlayingId ||
    highlightedSegmentId !== prevHighlightedId ||
    (userDuration ?? null) !== prevUserDuration
  ) {
    if (playingId !== prevPlayingId) {
      setLocalCurrentTimeSeconds(0);
    }
    setPrevPlayingId(playingId);
    setPrevHighlightedId(highlightedSegmentId);
    setPrevUserDuration(userDuration ?? null);

    const targetId = highlightedSegmentId || playingId;
    if (targetId) {
      const targetAudioSegment = audioSegments.find((t) => t.id === targetId);
      if (targetAudioSegment) {
        const tStart = new Date(targetAudioSegment.startTimestamp).getTime();
        const tEnd = new Date(targetAudioSegment.endTimestamp).getTime();

        const newestEnd = firstAudioSegment
          ? new Date(firstAudioSegment.endTimestamp).getTime()
          : 0;

        const currentEndTime = windowEndTime || newestEnd;
        const currentStartTime = currentEndTime - windowDurationMs;

        if (tStart < currentStartTime || tEnd > currentEndTime) {
          const preferredEndTime = tStart + windowDurationMs / 2;
          const newEndTime = Math.min(preferredEndTime, newestEnd);
          setWindowEndTime(newEndTime);
        }
      }
    }
  }

  // Calculates the visible time window bounds and processes audio segments into positioned clips for the waveform display.
  const { startTime, windowDuration, clips } = useMemo(() => {
    if (audioSegments.length === 0) {
      return {
        startTime: 0,
        windowDuration: windowDurationMs,
        clips: [],
      };
    }

    const mostRecentTime =
      windowEndTime || new Date(audioSegments[0].endTimestamp).getTime();
    const windowDuration = windowDurationMs;
    const startTime = mostRecentTime - windowDuration;

    // Filter for audio segments that overlap with the current visible time window
    const clips = audioSegments
      .filter((t) => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();
        return tStart < startTime + windowDuration && tEnd > startTime;
      })
      // Map filtered audio segments to clip objects with calculated positioning and display properties
      .map((t) => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();

        // Constrain to window bounds
        const visibleStart = Math.max(tStart, startTime);
        const visibleEnd = Math.min(tEnd, startTime + windowDuration);

        const left = ((visibleStart - startTime) / windowDuration) * 100;
        const width = ((visibleEnd - visibleStart) / windowDuration) * 100;

        const url = t.playbackAudioUri ? getAudioUrl(t.playbackAudioUri) : '';

        const evaluationAnnotation = findEvaluationAnnotationData(
          t.annotations
        );
        return {
          id: t.id,
          url,
          left,
          width,
          isAudioPlaying: t.id === currentlyPlayingSegmentId,
          isHighlighted: t.id === highlightedSegmentId,
          hasAlert:
            !!evaluationAnnotation && evaluationAnnotation.decisions.length > 0,
        };
      });

    return { startTime, windowDuration, clips };
  }, [
    audioSegments,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
    windowEndTime,
    windowDurationMs,
  ]);

  // Decode the in-view clips' peaks (bounded, off-screen). A new window drops the
  // prior pending decodes so moving the window doesn't fetch them; keyed on the
  // url set so re-renders that don't change the clips don't refire.
  const clipUrlsKey = clips.map((c) => c.url).join('|');
  useEffect(() => {
    const urls = clipUrlsKey.split('|').filter(Boolean);
    if (urls.length === 0) return;
    clearPending();
    enqueueDecode(urls);
  }, [clipUrlsKey, clearPending, enqueueDecode]);

  return (
    <Box
      sx={{ display: 'flex', alignItems: 'flex-start', width: '100%', mb: 1 }}
    >
      <Box
        sx={{ display: 'flex', mr: 1, alignItems: 'center', height: '60px' }}
      >
        <IconButton
          onClick={onTogglePlayPause}
          size="small"
          color="primary"
          aria-label={isAudioPlaying ? 'pause' : 'play'}
          disabled={audioSegments.length === 0}
        >
          {isAudioPlaying ? <PauseIcon /> : <PlayArrowIcon />}
        </IconButton>
      </Box>
      <Box sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column' }}>
        <Paper
          variant="outlined"
          sx={{
            width: '100%',
            height: '60px',
            bgcolor: 'action.hover',
            position: 'relative',
          }}
        >
          {clips.map((clip) => {
            const cached = getPeaks(clip.url);
            return (
              <TimelineClip
                key={clip.id}
                clip={{
                  ...clip,
                  peaks: cached?.peaks,
                  duration: cached?.duration,
                }}
                onClipClick={onClipClick}
                isDarkTheme={isDarkTheme}
                theme={theme}
                currentTimeSeconds={
                  clip.isAudioPlaying
                    ? currentTimeSeconds !== undefined
                      ? currentTimeSeconds
                      : currentAudioRef
                        ? localCurrentTimeSeconds
                        : undefined
                    : undefined
                }
              />
            );
          })}
          {audioSegments.length === 0 && (
            <Box
              sx={{
                position: 'absolute',
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                width: '100%',
                textAlign: 'center',
              }}
            >
              <Typography variant="body2" color="text.secondary">
                No audio found
              </Typography>
            </Box>
          )}
        </Paper>
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'space-between',
            mt: 0.5,
            // Reserve space for the time labels even when there are no
            // segments so the UI doesn't jump when segments are loaded.
            visibility: audioSegments.length > 0 ? 'visible' : 'hidden',
          }}
        >
          {formatTime &&
            Array.from({ length: 4 }).map((_, i) => (
              <Typography key={i} variant="caption" color="text.secondary">
                {formatTime(startTime + (i / 3) * windowDuration)}
              </Typography>
            ))}
        </Box>
      </Box>
    </Box>
  );
}

export default AudioDisplay;
