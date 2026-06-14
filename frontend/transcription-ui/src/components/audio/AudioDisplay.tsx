import React, { useEffect, useMemo, useRef, useState } from 'react';

import type { Howl } from 'howler';
import type WaveSurfer from 'wavesurfer.js';

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
import { formatClockTime } from '../../utils/timeUtils';
import { CustomAlertIcon } from '../common/AlertIcon';
import { TimelineMiniMap } from './TimelineMiniMap';
import { type TranscriptTime } from './timelineMath';

interface AudioDisplayProps {
  audioSegments: AudioSegment[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  onClipClick: (segmentId: string) => void;
  isAudioPlaying: boolean;
  onTogglePlayPause: () => void;
  currentTimeSeconds?: number;
  currentAudioRef?: React.RefObject<Howl | null>;
  windowEndTime: number | null;
  windowDurationMs: number;
  rangeStartMs: number | null;
  maxEnd: number | null;
  miniMapTimes: TranscriptTime[];
  onScrubToCenter: (centerMs: number) => void;
}

const PLAYING_CURSOR_WIDTH_PX = 1;

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

    // Update the cursor (color/width) without recreating the player.
    useEffect(() => {
      if (wsRef.current) {
        wsRef.current.setOptions({
          cursorColor: clip.isAudioPlaying
            ? theme.palette.error.main
            : 'transparent',
          cursorWidth: clip.isAudioPlaying ? PLAYING_CURSOR_WIDTH_PX : 0,
        });
      }
    }, [clip.isAudioPlaying, theme.palette.error.main]);

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
            ws.setOptions({
              cursorColor: clip.isAudioPlaying
                ? theme.palette.error.main
                : 'transparent',
              cursorWidth: clip.isAudioPlaying ? PLAYING_CURSOR_WIDTH_PX : 0,
            });
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
  isAudioPlaying,
  onTogglePlayPause,
  currentTimeSeconds,
  currentAudioRef,
  windowEndTime,
  windowDurationMs,
  rangeStartMs,
  maxEnd,
  miniMapTimes,
  onScrubToCenter,
}: AudioDisplayProps) {
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';

  const [localCurrentTimeSeconds, setLocalCurrentTimeSeconds] = useState(0);

  // Reset the cursor to the clip start when the playing clip changes.
  const [prevPlayingId, setPrevPlayingId] = useState(currentlyPlayingSegmentId);
  if (prevPlayingId !== currentlyPlayingSegmentId) {
    setPrevPlayingId(currentlyPlayingSegmentId);
    setLocalCurrentTimeSeconds(0);
  }

  useEffect(() => {
    if (
      currentTimeSeconds !== undefined ||
      !isAudioPlaying ||
      !currentlyPlayingSegmentId ||
      !currentAudioRef?.current
    ) {
      return;
    }
    let frame = 0;
    const tick = () => {
      const seek = currentAudioRef.current?.seek();
      // seek() returns the Howl instance (not a number) before audio loads.
      if (typeof seek === 'number') setLocalCurrentTimeSeconds(seek);
      frame = requestAnimationFrame(tick);
    };
    tick();
    return () => cancelAnimationFrame(frame);
  }, [
    isAudioPlaying,
    currentlyPlayingSegmentId,
    currentAudioRef,
    currentTimeSeconds,
  ]);

  const { startTime, clips } = useMemo(() => {
    if (audioSegments.length === 0) {
      return { startTime: 0, clips: [] };
    }
    const mostRecentTime =
      windowEndTime ?? new Date(audioSegments[0].endTimestamp).getTime();
    const startTime = mostRecentTime - windowDurationMs;
    const windowEnd = startTime + windowDurationMs;

    const clips = audioSegments
      .filter((t) => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();
        return tStart < windowEnd && tEnd > startTime;
      })
      .map((t) => {
        const tStart = new Date(t.startTimestamp).getTime();
        const tEnd = new Date(t.endTimestamp).getTime();
        const visibleStart = Math.max(tStart, startTime);
        const visibleEnd = Math.min(tEnd, windowEnd);
        const evaluation = findEvaluationAnnotationData(t.annotations);
        return {
          id: t.id,
          url: t.playbackAudioUri ? getAudioUrl(t.playbackAudioUri) : '',
          left: ((visibleStart - startTime) / windowDurationMs) * 100,
          width: ((visibleEnd - visibleStart) / windowDurationMs) * 100,
          isAudioPlaying: t.id === currentlyPlayingSegmentId,
          isHighlighted: t.id === highlightedSegmentId,
          hasAlert: !!evaluation && evaluation.decisions.length > 0,
        };
      });

    return { startTime, clips };
  }, [
    audioSegments,
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
          {clips.map((clip) => (
            <TimelineClip
              key={clip.id}
              clip={clip}
              onClipClick={onClipClick}
              isDarkTheme={isDarkTheme}
              theme={theme}
              currentTimeSeconds={
                clip.isAudioPlaying
                  ? (currentTimeSeconds ??
                    (currentAudioRef ? localCurrentTimeSeconds : undefined))
                  : undefined
              }
            />
          ))}
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
        <TimelineMiniMap
          transcriptTimes={miniMapTimes}
          rangeStartMs={rangeStartMs}
          maxEnd={maxEnd}
          windowEndTime={windowEndTime}
          windowDurationMs={windowDurationMs}
          isDarkTheme={isDarkTheme}
          onScrubToCenter={onScrubToCenter}
        />
      </Box>
    </Box>
  );
}

export default AudioDisplay;
