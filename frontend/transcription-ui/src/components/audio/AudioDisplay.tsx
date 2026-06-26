import React, { useEffect, useMemo, useRef, useState } from 'react';

import type WaveSurfer from 'wavesurfer.js';

import Box from '@mui/material/Box';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import { type Theme, useTheme } from '@mui/material/styles';
import { type AudioSegment } from '@transcription/common';
import WavesurferPlayer from '@wavesurfer/react';

import type { PlaybackController } from '../../audio/WebAudioPlayer';
import { findEvaluationAnnotationData } from '../../utils/annotationUtils';
import { getAudioUrl } from '../../utils/audioUtils';
import { type PlaybackState } from '../../utils/playbackUtils';
import { formatClockTime } from '../../utils/timeUtils';
import { CustomAlertIcon } from '../common/AlertIcon';
import TimelinePlayhead from './TimelinePlayhead';
import { computePlayhead } from './computePlayhead';

interface AudioDisplayProps {
  audioSegments: AudioSegment[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  onClipClick: (segmentId: string) => void;
  // Visible window, owned by useAudioTimelineWindow; null follows the live edge.
  windowEndTime: number | null;
  windowDurationMs: number;
  isAudioPlaying: boolean;
  playbackState: PlaybackState;
  currentAudioRef?: React.RefObject<PlaybackController | null>;
}

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
    const wsRef = useRef<WaveSurfer | null>(null);

    // Sync playback progress (seek)
    useEffect(() => {
      if (
        clip.isAudioPlaying &&
        wsRef.current &&
        currentTimeSeconds !== undefined
      ) {
        wsRef.current.setTime(currentTimeSeconds);
      } else if (!clip.isAudioPlaying && wsRef.current) {
        wsRef.current.setTime(0);
      }
    }, [clip.isAudioPlaying, currentTimeSeconds]);

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
          /* Wavesurfer cursor subpixel anti-aliasing / shimmering optimizations */
          '& div::part(cursor)': {
            willChange: 'left',
            transform: 'translateZ(0)',
            backfaceVisibility: 'hidden',
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
          cursorWidth={0}
          barWidth={0.5}
          barGap={0.5}
          height={60}
          interact={false}
          onReady={(ws) => {
            wsRef.current = ws;
            if (clip.isAudioPlaying && currentTimeSeconds !== undefined) {
              ws.setTime(currentTimeSeconds);
            }
          }}
          onDestroy={() => {
            wsRef.current = null;
          }}
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
      prevProps.theme === nextProps.theme &&
      prevProps.currentTimeSeconds === nextProps.currentTimeSeconds
    );
  }
);

export function AudioDisplay({
  audioSegments,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
  onClipClick,
  windowEndTime,
  windowDurationMs,
  isAudioPlaying,
  playbackState,
  currentAudioRef,
}: AudioDisplayProps) {
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';

  const [localCurrentTimeSeconds, setLocalCurrentTimeSeconds] =
    useState<number>(0);

  // Snap the playback cursor back to the start when the playing segment changes.
  const [prevPlayingId, setPrevPlayingId] = useState<string | null>(
    currentlyPlayingSegmentId
  );
  if (currentlyPlayingSegmentId !== prevPlayingId) {
    setPrevPlayingId(currentlyPlayingSegmentId);
    setLocalCurrentTimeSeconds(0);
  }

  // Poll current playback progress when audio is playing
  useEffect(() => {
    if (
      !isAudioPlaying ||
      !currentlyPlayingSegmentId ||
      !currentAudioRef?.current
    ) {
      return;
    }

    let animationFrameId: number;

    const updateProgress = () => {
      if (currentAudioRef.current) {
        setLocalCurrentTimeSeconds(currentAudioRef.current.getCurrentTime());
      }
      animationFrameId = requestAnimationFrame(updateProgress);
    };

    updateProgress();

    return () => {
      cancelAnimationFrame(animationFrameId);
    };
  }, [isAudioPlaying, currentlyPlayingSegmentId, currentAudioRef]);

  const firstAudioSegment = audioSegments[0];
  const firstAudioSegmentId = firstAudioSegment?.id || null;

  const isListening = playbackState === 'listening';

  // Calculates the visible time window bounds and processes audio segments into positioned clips for the waveform display.
  const { startTime, clips } = useMemo(() => {
    if (audioSegments.length === 0) {
      return { startTime: 0, clips: [] };
    }

    const windowEndMs =
      windowEndTime || new Date(audioSegments[0].endTimestamp).getTime();
    const startTime = windowEndMs - windowDurationMs;

    // Filter for audio segments that overlap with the current visible time window
    const clips = audioSegments
      .filter((t) => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();
        return tStart < startTime + windowDurationMs && tEnd > startTime;
      })
      // Map filtered audio segments to clip objects with calculated positioning and display properties
      .map((t) => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();

        // Constrain to window bounds
        const visibleStart = Math.max(tStart, startTime);
        const visibleEnd = Math.min(tEnd, startTime + windowDurationMs);

        const left = ((visibleStart - startTime) / windowDurationMs) * 100;
        const width = ((visibleEnd - visibleStart) / windowDurationMs) * 100;

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
          // While listening, the lozenge marks the newest clip, not selection.
          isHighlighted:
            t.id === highlightedSegmentId &&
            !(isListening && t.id === firstAudioSegmentId),
          hasAlert:
            !!evaluationAnnotation && evaluationAnnotation.decisions.length > 0,
        };
      });

    return { startTime, clips };
  }, [
    audioSegments,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
    windowEndTime,
    windowDurationMs,
    isListening,
    firstAudioSegmentId,
  ]);

  const playhead = computePlayhead({
    audioSegments,
    currentlyPlayingSegmentId,
    state: playbackState,
    localCurrentTimeSeconds,
    startTime,
    windowDurationMs,
  });

  return (
    <Box
      sx={{ display: 'flex', alignItems: 'flex-start', width: '100%', mb: 1 }}
    >
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
              currentTimeSeconds={
                clip.isAudioPlaying && currentAudioRef
                  ? localCurrentTimeSeconds
                  : undefined
              }
            />
          ))}
          {playhead.show && (
            <TimelinePlayhead
              state={playhead.state}
              left={playhead.left}
              label={playhead.label}
            />
          )}
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
          {Array.from({ length: 4 }).map((_, i) => (
            <Typography key={i} variant="caption" color="text.secondary">
              {formatClockTime(startTime + (i / 3) * windowDurationMs)}
            </Typography>
          ))}
        </Box>
      </Box>
    </Box>
  );
}

export default AudioDisplay;
