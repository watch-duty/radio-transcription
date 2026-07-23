import React, { useEffect, useMemo, useState } from 'react';

import Box from '@mui/material/Box';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import { type Theme, useTheme } from '@mui/material/styles';
import { type AudioSegment } from '@transcription/common';

import type { PlaybackController } from '../../audio/WebAudioPlayer';
import type { RenderableAudioSegment } from '../../hooks/useConsolidatedAudioSegments';
import { useIsNarrow } from '../../hooks/useIsNarrow';
import { type HistogramMark } from '../../hooks/useTimelineHistogram';
import {
  findEvaluationAnnotationData,
  findWaveformAnnotationData,
  segmentHasSpeech,
} from '../../utils/annotationUtils';
import { type PlaybackState, isWithinSegment } from '../../utils/playbackUtils';
import { CustomAlertIcon } from '../common/AlertIcon';
import TimelineMiniMap from './TimelineMiniMap';
import TimelinePlayhead from './TimelinePlayhead';
import { computePlayhead } from './computePlayhead';
import { resolveClickSegmentAndOffset } from './timelineMath';

interface AudioDisplayProps {
  audioSegments: RenderableAudioSegment[];
  // Unconsolidated segments, so the playhead can anchor on the raw clip playing.
  rawAudioSegments: AudioSegment[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  onClipClick: (segmentId: string, offsetSeconds?: number) => void;
  // Visible window, owned by useAudioTimelineWindow; null follows the live edge.
  windowEndTime: number | null;
  windowDurationMs: number;
  // 24h overview density + range, for the mini-map below the window strip.
  histogramMarks: HistogramMark[];
  rangeStartMs: number | null;
  maxEnd: number | null;
  onCenterWindow: (centerMs: number) => void;
  isAudioPlaying: boolean;
  playbackState: PlaybackState;
  currentAudioRef?: React.RefObject<PlaybackController | null>;
  seekTrigger?: number;
}

const WAVEFORM_MIN_AMPLITUDE = 0.0083; // silent buckets show a ~0.5px hairline at the 60px track height

// preserveAspectRatio="none" stretches the viewBox (one unit per peak) to fill
// the clip box at any width.
const Waveform = React.memo(function Waveform({
  peaks,
  color,
}: {
  peaks: number[][];
  color: string;
}) {
  const channel = peaks[0] ?? [];
  const bars = channel
    .map((value, i) => {
      const amp = Math.max(
        WAVEFORM_MIN_AMPLITUDE,
        Math.min(1, Math.abs(value))
      );
      const top = (1 - amp) / 2;
      return `M${i} ${top}h1v${amp}h-1Z`;
    })
    .join('');

  return (
    <Box
      component="svg"
      data-testid="waveform"
      viewBox={`0 0 ${channel.length || 1} 1`}
      preserveAspectRatio="none"
      sx={{ position: 'absolute', inset: 0, width: '100%', height: '100%' }}
    >
      <path d={bars} fill={color} />
    </Box>
  );
});

// Shown when a clip has no WAVEFORM annotation: a solid block when speech was
// detected (audio is present, the waveform is just unavailable) vs a thin line
// otherwise, so the two never read as the same flat "silence".
const WaveformPlaceholder = ({ isSpeech }: { isSpeech: boolean }) =>
  isSpeech ? (
    <Box
      data-testid="waveform-missing-block"
      sx={{
        position: 'absolute',
        top: '20%',
        bottom: '20%',
        left: 0,
        right: 0,
        bgcolor: 'text.secondary',
        opacity: 0.25,
      }}
    />
  ) : (
    <Box
      data-testid="waveform-silence-line"
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
  );

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
    left: number;
    width: number;
    isAudioPlaying: boolean;
    isHighlighted: boolean;
    hasAlert: boolean;
    // From the segment's WAVEFORM annotation; absent on older segments
    // (pre-rollout) or if waveform computation was skipped.
    peaks?: number[][];
    duration?: number;
    isSpeech?: boolean;
    isOutageBundle?: boolean;
    startTimestamp: string;
    endTimestamp: string;
    bundledSegmentIds?: string[];
  };
  windowStartTime: number;
  windowDurationMs: number;
  rawAudioSegments: AudioSegment[];
  onClipClick: (segmentId: string, offsetSeconds?: number) => void;
  isDarkTheme: boolean;
  theme: Theme;
}

const TimelineClip = React.memo(
  ({
    clip,
    windowStartTime,
    windowDurationMs,
    rawAudioSegments,
    onClipClick,
    isDarkTheme,
    theme,
  }: TimelineClipProps) => {
    const renderWaveform = !!clip.peaks?.[0]?.length;

    if (clip.isOutageBundle) {
      return (
        <Box
          sx={{
            position: 'absolute',
            left: `${clip.left}%`,
            width: `${clip.width}%`,
            height: '100%',
            background: isDarkTheme
              ? 'rgba(0, 0, 0, 0.25)'
              : 'rgba(0, 0, 0, 0.06)',
            borderLeft: isDarkTheme
              ? '1px solid rgba(255, 255, 255, 0.05)'
              : '1px solid rgba(0, 0, 0, 0.05)',
            borderRight: isDarkTheme
              ? '1px solid rgba(255, 255, 255, 0.05)'
              : '1px solid rgba(0, 0, 0, 0.05)',
            animation: 'outagePulse 2.5s ease-in-out infinite',
            '@keyframes outagePulse': {
              '0%, 100%': {
                opacity: 0.4,
              },
              '50%': {
                opacity: 0.85,
              },
            },
          }}
        />
      );
    }

    const handleClick = (e: React.MouseEvent<HTMLDivElement>) => {
      const rect = e.currentTarget.getBoundingClientRect();
      if (rect.width <= 0) {
        onClipClick(clip.id);
        return;
      }
      const clickX = e.clientX - rect.left;
      const fraction = Math.max(0, Math.min(1, clickX / rect.width));

      const tStart = new Date(clip.startTimestamp).getTime();
      const tEnd = new Date(clip.endTimestamp).getTime();
      const visibleStart = Math.max(tStart, windowStartTime);
      const visibleEnd = Math.min(tEnd, windowStartTime + windowDurationMs);

      const clickedTimeMs =
        visibleStart + fraction * (visibleEnd - visibleStart);

      const { segmentId, offsetSeconds } = resolveClickSegmentAndOffset(
        clip,
        clickedTimeMs,
        rawAudioSegments
      );

      onClipClick(segmentId, offsetSeconds);
    };

    return (
      <Box
        onClick={handleClick}
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
        {renderWaveform && clip.peaks ? (
          <Waveform peaks={clip.peaks} color={theme.palette.text.secondary} />
        ) : (
          <WaveformPlaceholder isSpeech={!!clip.isSpeech} />
        )}
      </Box>
    );
  },
  (prevProps, nextProps) => {
    // This prevents the clip from re-rendering when the parent component re-renders,
    // unless the props actually change.
    return (
      prevProps.clip.id === nextProps.clip.id &&
      prevProps.clip.left === nextProps.clip.left &&
      prevProps.clip.width === nextProps.clip.width &&
      prevProps.clip.isAudioPlaying === nextProps.clip.isAudioPlaying &&
      prevProps.clip.isHighlighted === nextProps.clip.isHighlighted &&
      prevProps.clip.hasAlert === nextProps.clip.hasAlert &&
      prevProps.clip.peaks === nextProps.clip.peaks &&
      prevProps.clip.duration === nextProps.clip.duration &&
      prevProps.clip.isSpeech === nextProps.clip.isSpeech &&
      prevProps.clip.isOutageBundle === nextProps.clip.isOutageBundle &&
      prevProps.clip.startTimestamp === nextProps.clip.startTimestamp &&
      prevProps.clip.endTimestamp === nextProps.clip.endTimestamp &&
      prevProps.clip.bundledSegmentIds === nextProps.clip.bundledSegmentIds &&
      prevProps.windowStartTime === nextProps.windowStartTime &&
      prevProps.windowDurationMs === nextProps.windowDurationMs &&
      prevProps.rawAudioSegments === nextProps.rawAudioSegments &&
      prevProps.isDarkTheme === nextProps.isDarkTheme &&
      prevProps.theme === nextProps.theme
    );
  }
);

export function AudioDisplay({
  audioSegments,
  rawAudioSegments,
  currentlyPlayingSegmentId,
  highlightedSegmentId,
  onClipClick,
  windowEndTime,
  windowDurationMs,
  histogramMarks,
  rangeStartMs,
  maxEnd,
  onCenterWindow,
  isAudioPlaying,
  playbackState,
  currentAudioRef,
  seekTrigger,
}: AudioDisplayProps) {
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';
  const isNarrow = useIsNarrow();

  const [localCurrentTimeSeconds, setLocalCurrentTimeSeconds] =
    useState<number>(0);

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

  // Sync progress instantly on discrete seek events (e.g. skip buttons when paused)
  useEffect(() => {
    if (currentAudioRef?.current) {
      setLocalCurrentTimeSeconds(currentAudioRef.current.getCurrentTime());
    }
  }, [seekTrigger, currentAudioRef]);

  // Reset the playback position when the playing segment changes, so the
  // playhead starts at the new clip instead of flashing the previous offset.
  const [prevPlayingId, setPrevPlayingId] = useState<string | null>(
    currentlyPlayingSegmentId
  );
  if (currentlyPlayingSegmentId !== prevPlayingId) {
    setPrevPlayingId(currentlyPlayingSegmentId);
    setLocalCurrentTimeSeconds(0);
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

        const waveform = findWaveformAnnotationData(t.annotations);

        const evaluationAnnotation = findEvaluationAnnotationData(
          t.annotations
        );
        return {
          id: t.id,
          left,
          width,
          isAudioPlaying: currentlyPlayingSegmentId
            ? isWithinSegment(t, currentlyPlayingSegmentId)
            : false,
          isHighlighted: highlightedSegmentId
            ? isWithinSegment(t, highlightedSegmentId)
            : false,
          hasAlert:
            !!evaluationAnnotation && evaluationAnnotation.decisions.length > 0,
          peaks: waveform?.peaks,
          duration: waveform?.durationSeconds,
          isSpeech: segmentHasSpeech(t),
          isOutageBundle: !!t.isOutageBundle,
          startTimestamp: t.startTimestamp,
          endTimestamp: t.endTimestamp,
          bundledSegmentIds: t.bundledSegmentIds,
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

  const playhead = computePlayhead({
    audioSegments,
    rawAudioSegments,
    currentlyPlayingSegmentId,
    state: playbackState,
    localCurrentTimeSeconds,
    startTime,
    windowDurationMs: windowDuration,
  });

  return (
    <Box
      sx={{
        display: 'flex',
        alignItems: 'flex-start',
        width: '100%',
        mb: isNarrow ? 0.5 : 1,
      }}
    >
      <Box sx={{ flexGrow: 1, display: 'flex', flexDirection: 'column' }}>
        <Paper
          variant="outlined"
          sx={{
            width: '100%',
            height: isNarrow ? '40px' : '60px',
            bgcolor: 'action.hover',
            position: 'relative',
          }}
        >
          {clips.map((clip) => (
            <TimelineClip
              key={clip.id}
              clip={clip}
              windowStartTime={startTime}
              windowDurationMs={windowDuration}
              rawAudioSegments={rawAudioSegments}
              onClipClick={onClipClick}
              isDarkTheme={isDarkTheme}
              theme={theme}
            />
          ))}
          {playhead.show && (
            <TimelinePlayhead
              state={playhead.state}
              leftOffsetPct={playhead.leftOffsetPct}
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
          {formatTime &&
            Array.from({ length: 4 }).map((_, i) => (
              <Typography key={i} variant="caption" color="text.secondary">
                {formatTime(startTime + (i / 3) * windowDuration)}
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
          onCenterWindow={onCenterWindow}
        />
      </Box>
    </Box>
  );
}

export default AudioDisplay;
