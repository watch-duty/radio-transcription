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

import { segmentHasAlert } from '../../utils/annotationUtils';
import { getAudioUrl } from '../../utils/audioUtils';
import { formatClockTime } from '../../utils/timeUtils';
import { CustomAlertIcon } from '../common/AlertIcon';
import { TimelineMiniMap } from './TimelineMiniMap';
import { type HistogramMark } from './timelineMath';
import { usePeaksDecodeQueue } from './usePeaksDecodeQueue';

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
  histogramMarks: HistogramMark[];
  onScrubToCenter: (centerMs: number) => void;
  isLoading?: boolean;
}

const PLAYING_CURSOR_WIDTH_PX = 2;

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
    // their own audio never finishes. The queue decodes once; until then, a
    // placeholder.
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
  histogramMarks,
  onScrubToCenter,
  isLoading = false,
}: AudioDisplayProps) {
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';

  const { enqueueDecode, clearPending, getPeaks } = usePeaksDecodeQueue();

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
        return {
          id: t.id,
          url: t.playbackAudioUri ? getAudioUrl(t.playbackAudioUri) : '',
          left: ((visibleStart - startTime) / windowDurationMs) * 100,
          width: ((visibleEnd - visibleStart) / windowDurationMs) * 100,
          isAudioPlaying: t.id === currentlyPlayingSegmentId,
          isHighlighted: t.id === highlightedSegmentId,
          hasAlert: segmentHasAlert(t.annotations),
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

  // Decode the in-view clips' peaks (bounded, off-screen). A new window drops the
  // prior pending decodes so scrubbing past windows doesn't fetch them; keyed on
  // the url set so polls that don't move the window don't refire.
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
                    ? (currentTimeSeconds ??
                      (currentAudioRef ? localCurrentTimeSeconds : undefined))
                    : undefined
                }
              />
            );
          })}
          {clips.length === 0 && !isLoading && (
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
          histogramMarks={histogramMarks}
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
