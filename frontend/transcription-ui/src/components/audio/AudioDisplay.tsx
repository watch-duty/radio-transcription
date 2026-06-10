import React, { useMemo, useState } from 'react';

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

interface AudioDisplayProps {
  transcripts: AudioSegment[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  onClipClick: (segmentId: string) => void;
  userDuration?: string | null;
  isAudioPlaying: boolean;
  onTogglePlayPause: () => void;
}

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
  };
  onClipClick: (segmentId: string) => void;
  isDarkTheme: boolean;
  theme: Theme;
}

const TimelineClip = React.memo(
  ({ clip, onClipClick, isDarkTheme, theme }: TimelineClipProps) => {
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
        <WavesurferPlayer
          url={clip.url}
          waveColor={theme.palette.text.secondary}
          progressColor={theme.palette.text.primary}
          cursorColor="transparent"
          barWidth={0.5}
          barGap={0.5}
          height={60}
          interact={false}
        />
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
      prevProps.isDarkTheme === nextProps.isDarkTheme &&
      prevProps.theme === nextProps.theme
    );
  }
);

export function AudioDisplay({
  transcripts,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
  onClipClick,
  userDuration,
  isAudioPlaying,
  onTogglePlayPause,
}: AudioDisplayProps) {
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';

  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);

  const [prevFirstTranscriptId, setPrevFirstTranscriptId] = useState<
    string | null
  >(null);
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

  const firstTranscript = transcripts[0];
  const firstTranscriptId = firstTranscript?.id || null;
  const firstTranscriptEndTimestamp = firstTranscript?.endTimestamp || null;

  // Reset windowEndTime when first transcript changes
  if (firstTranscriptId !== prevFirstTranscriptId) {
    setPrevFirstTranscriptId(firstTranscriptId);
    setWindowEndTime(
      firstTranscriptEndTimestamp
        ? new Date(firstTranscriptEndTimestamp).getTime()
        : null
    );
    setPrevPlayingId(null); // Force re-check of bounds
  }

  const playingId = currentlyPlayingSegmentId || null;

  // Shift windowEndTime when playing or highlighted transcript goes out of bounds
  if (
    playingId !== prevPlayingId ||
    highlightedSegmentId !== prevHighlightedId ||
    (userDuration ?? null) !== prevUserDuration
  ) {
    setPrevPlayingId(playingId);
    setPrevHighlightedId(highlightedSegmentId);
    setPrevUserDuration(userDuration ?? null);

    const targetId = highlightedSegmentId || playingId;
    if (targetId) {
      const targetTranscript = transcripts.find((t) => t.id === targetId);
      if (targetTranscript) {
        const tStart = new Date(targetTranscript.startTimestamp).getTime();
        const tEnd = new Date(targetTranscript.endTimestamp).getTime();

        const currentEndTime =
          windowEndTime ||
          (firstTranscript
            ? new Date(firstTranscript.endTimestamp).getTime()
            : 0);
        const currentStartTime = currentEndTime - windowDurationMs;

        if (tStart < currentStartTime || tEnd > currentEndTime) {
          const newEndTime = tStart + windowDurationMs / 2;
          setWindowEndTime(newEndTime);
        }
      }
    }
  }

  // Calculates the visible time window bounds and processes transcripts into positioned clips for the waveform display.
  const { startTime, windowDuration, clips } = useMemo(() => {
    if (transcripts.length === 0) {
      return {
        startTime: 0,
        windowDuration: windowDurationMs,
        clips: [],
      };
    }

    const mostRecentTime =
      windowEndTime || new Date(transcripts[0].endTimestamp).getTime();
    const windowDuration = windowDurationMs;
    const startTime = mostRecentTime - windowDuration;

    // Filter for transcripts that overlap with the current visible time window
    const clips = transcripts
      .filter((t) => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();
        return tStart < startTime + windowDuration && tEnd > startTime;
      })
      // Map filtered transcripts to clip objects with calculated positioning and display properties
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
          hasAlert: !!evaluationAnnotation && evaluationAnnotation.decisions.length > 0,
        };
      });

    return { startTime, windowDuration, clips };
  }, [
    transcripts,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
    windowEndTime,
    windowDurationMs,
  ]);

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
          disabled={transcripts.length === 0}
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
          {clips.map((clip) => (
            <TimelineClip
              key={clip.id}
              clip={clip}
              onClipClick={onClipClick}
              isDarkTheme={isDarkTheme}
              theme={theme}
            />
          ))}
          {transcripts.length === 0 && (
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
            // transcripts so the UI doesn't jump when transcripts are loaded.
            visibility: transcripts.length > 0 ? 'visible' : 'hidden',
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
