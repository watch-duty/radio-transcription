import React, {
  startTransition,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';

import { motion, useMotionValue } from 'framer-motion';

import PauseIcon from '@mui/icons-material/PauseCircleFilledOutlined';
import PlayArrowIcon from '@mui/icons-material/PlayCircleFilledOutlined';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import { type Theme, useTheme } from '@mui/material/styles';
import type { Transcript } from '@transcription/common';
import { useDrag } from '@use-gesture/react';
import WavesurferPlayer from '@wavesurfer/react';

import { getAudioUrl } from '../../utils/audioUtils';
import { hasEvaluationAlert } from '../../utils/evaluationUtils';
import { formatClockTime } from '../../utils/timeUtils';
import { CustomAlertIcon } from '../common/AlertIcon';
import { TimelineMiniMap } from './TimelineMiniMap';
import { type TranscriptTime, getWindowDurationMs } from './timelineMath';
import { usePeaksDecodeQueue } from './usePeaksDecodeQueue';

interface AudioDisplayProps {
  transcripts: Transcript[];
  currentlyPlayingSegmentId: string | null;
  highlightedSegmentId: string | null;
  onClipClick: (segmentId: string) => void;
  userDuration?: string | null;
  isAudioPlaying: boolean;
  onTogglePlayPause: () => void;
  onPannedChange?: (isPanned: boolean) => void;
  onWindowPan?: (segmentId: string) => void;
  // Debounced; the window's most-recent edge while panned, or null at live.
  onActiveTimeChange?: (windowEndMs: number | null) => void;
  // Bump to snap the window back to the live edge.
  viewLiveNonce?: number;
}

const LIVE_EDGE_EPS_MS = 1000;
// Movement before a press becomes a pan, so taps still click a clip.
const DRAG_THRESHOLD_PX = 5;
// Cap clips warmed per hover so a wide window doesn't flood the decode queue.
const MAX_PREFETCH_PER_HOVER = 16;

interface TimelineClipProps {
  clip: {
    id: string;
    url: string;
    left: number;
    width: number;
    isAudioPlaying: boolean;
    isHighlighted: boolean;
    hasAlert: boolean;
    peaks?: (Float32Array | number[])[];
    duration?: number;
  };
  onClipClick: (segmentId: string) => void;
  isDarkTheme: boolean;
  theme: Theme;
  // False during scrub; churning players thrashes loads → blank waveforms.
  showWaveform: boolean;
}

type ClipData = TimelineClipProps['clip'];

const TimelineClip = React.memo(
  ({
    clip,
    onClipClick,
    isDarkTheme,
    theme,
    showWaveform,
  }: TimelineClipProps) => {
    // Placeholder until peaks are cached (and while panning).
    const renderWaveform = showWaveform && !!clip.peaks;
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
        {renderWaveform ? (
          <WavesurferPlayer
            url={clip.url}
            peaks={clip.peaks}
            duration={clip.duration}
            waveColor={theme.palette.text.secondary}
            progressColor={theme.palette.text.primary}
            cursorColor="transparent"
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
      prevProps.showWaveform === nextProps.showWaveform
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
  onPannedChange,
  onWindowPan,
  onActiveTimeChange,
  viewLiveNonce,
}: AudioDisplayProps) {
  const theme = useTheme();
  const isDarkTheme = theme.palette.mode === 'dark';

  const { enqueueDecode, getPeaks } = usePeaksDecodeQueue();

  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);
  const [isPanned, setIsPanned] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  const [isScrubbing, setIsScrubbing] = useState(false);
  // WaveSurfer measures width at mount, so mounting mid-navigation gives blank waves.
  const [waveformsReady, setWaveformsReady] = useState(true);
  // Waveforms snapshotted at pan-start, rendered (translated) until the pan ends.
  const [frozenClips, setFrozenClips] = useState<ClipData[]>([]);
  // Off-render translate (px) for the frozen waveforms while panning.
  const waveX = useMotionValue(0);

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
  const [prevViewLiveNonce, setPrevViewLiveNonce] = useState<
    number | undefined
  >(viewLiveNonce);

  const containerRef = useRef<HTMLDivElement>(null);
  // Dedupe so a pan doesn't re-scroll the list to the same index each frame.
  const lastPannedSegmentRef = useRef<string | null>(null);

  const windowDurationMs = useMemo(
    () => getWindowDurationMs(userDuration),
    [userDuration]
  );

  // Parse timestamps/url/alert once so the derived structures don't re-parse.
  const transcriptTimes: TranscriptTime[] = useMemo(
    () =>
      transcripts.map((t) => ({
        id: t.segmentId,
        startMs: new Date(t.startTimestamp).getTime(),
        endMs: new Date(t.endTimestamp).getTime(),
        url: getAudioUrl(t.playbackAudioUri),
        hasAlert: hasEvaluationAlert(t.evaluationDecisions),
      })),
    [transcripts]
  );

  // rangeStartMs = strip's left edge; maxEnd = live edge (same value the waveform
  // window uses for its right edge, so the overview matches it when not scrolled).
  const { minEnd, maxEnd, rangeStartMs } = useMemo(() => {
    if (transcriptTimes.length === 0) {
      return {
        minEnd: null as number | null,
        maxEnd: null as number | null,
        rangeStartMs: null as number | null,
      };
    }
    let oldestStart = Infinity;
    for (const t of transcriptTimes) {
      oldestStart = Math.min(oldestStart, t.startMs);
    }
    const liveEdge = transcriptTimes[0].endMs;
    return {
      minEnd: Math.min(oldestStart + windowDurationMs, liveEdge),
      maxEnd: liveEdge,
      rangeStartMs: oldestStart,
    };
  }, [transcriptTimes, windowDurationMs]);

  const firstTranscript = transcripts[0];
  const firstTranscriptId = firstTranscript?.segmentId || null;
  const firstTranscriptEndTimestamp = firstTranscript?.endTimestamp || null;

  // Reset to the live edge when "Jump to live" is clicked or the feed changes.
  if (viewLiveNonce !== prevViewLiveNonce) {
    setPrevViewLiveNonce(viewLiveNonce);
    setWindowEndTime(null);
    setIsPanned(false);
  }

  // Follow live as new transcripts arrive, unless the user has panned away.
  if (firstTranscriptId !== prevFirstTranscriptId) {
    setPrevFirstTranscriptId(firstTranscriptId);
    if (!isPanned) {
      setWindowEndTime(
        firstTranscriptEndTimestamp
          ? new Date(firstTranscriptEndTimestamp).getTime()
          : null
      );
      setPrevPlayingId(null); // Force re-check of bounds
    }
  }

  const playingId = currentlyPlayingSegmentId || null;

  if (
    playingId !== prevPlayingId ||
    highlightedSegmentId !== prevHighlightedId ||
    (userDuration ?? null) !== prevUserDuration
  ) {
    const highlightChanged = highlightedSegmentId !== prevHighlightedId;
    setPrevPlayingId(playingId);
    setPrevHighlightedId(highlightedSegmentId);
    setPrevUserDuration(userDuration ?? null);

    // While panned, only explicit highlight changes recenter (not playback).
    const allowShift = !isPanned || highlightChanged;
    const targetId = highlightedSegmentId || playingId;
    if (allowShift && targetId) {
      const target = transcriptTimes.find((t) => t.id === targetId);
      if (target) {
        const currentEndTime = windowEndTime || maxEnd || 0;
        const currentStartTime = currentEndTime - windowDurationMs;

        if (
          target.startMs < currentStartTime ||
          target.endMs > currentEndTime
        ) {
          let newEndTime = target.startMs + windowDurationMs / 2;
          if (minEnd != null && maxEnd != null) {
            newEndTime = Math.min(maxEnd, Math.max(minEnd, newEndTime));
            setWindowEndTime(newEndTime);
            setIsPanned(newEndTime < maxEnd - LIVE_EDGE_EPS_MS);
          }
        }
      }
    }
  }

  useEffect(() => {
    onPannedChange?.(isPanned);
  }, [isPanned, onPannedChange]);

  // Re-arm so the next pan re-scrolls even if it lands on the same segment.
  useEffect(() => {
    lastPannedSegmentRef.current = null;
  }, [viewLiveNonce]);

  const findRepresentativeSegmentId = (end: number): string | null => {
    for (const t of transcriptTimes) {
      if (t.startMs <= end) return t.id;
    }
    return transcriptTimes[transcriptTimes.length - 1]?.id ?? null;
  };

  // Clamp a target window end to the pannable range, or null if bounds are unknown.
  const clampWindowEnd = (nextEnd: number): number | null =>
    minEnd == null || maxEnd == null
      ? null
      : Math.min(maxEnd, Math.max(minEnd, nextEnd));

  // Commit the window: chip state + list scroll. Drives the mini-map, labels,
  // and list — not the waveforms.
  const commitWindowEnd = (end: number) => {
    setWindowEndTime(end);
    setIsPanned(maxEnd != null && end < maxEnd - LIVE_EDGE_EPS_MS);
    const segmentId = findRepresentativeSegmentId(end);
    if (segmentId && segmentId !== lastPannedSegmentRef.current) {
      lastPannedSegmentRef.current = segmentId;
      onWindowPan?.(segmentId);
    }
  };

  // A settled move (tap, scrub, pan release): commit now and remount waveforms
  // so clips entering the window render at their final width.
  const applyWindowEnd = (nextEnd: number) => {
    const end = clampWindowEnd(nextEnd);
    if (end == null) return;
    commitWindowEnd(end);
    setWaveformsReady(false);
  };

  // Geometry only — stable across decodes so peak attachment doesn't rebuild it.
  const { startTime, visibleClips } = useMemo(() => {
    if (transcriptTimes.length === 0) {
      return { startTime: 0, visibleClips: [] };
    }
    const mostRecentTime = windowEndTime || transcriptTimes[0].endMs;
    const startTime = mostRecentTime - windowDurationMs;
    const windowEnd = startTime + windowDurationMs;
    const visibleClips = transcriptTimes
      .filter((t) => t.startMs < windowEnd && t.endMs > startTime)
      .map((t) => {
        const visibleStart = Math.max(t.startMs, startTime);
        const visibleEnd = Math.min(t.endMs, windowEnd);
        return {
          id: t.id,
          url: t.url,
          left: ((visibleStart - startTime) / windowDurationMs) * 100,
          width: ((visibleEnd - visibleStart) / windowDurationMs) * 100,
          isAudioPlaying: t.id === currentlyPlayingSegmentId,
          isHighlighted: t.id === highlightedSegmentId,
          hasAlert: t.hasAlert,
        };
      });
    return { startTime, visibleClips };
  }, [
    transcriptTimes,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
    windowEndTime,
    windowDurationMs,
  ]);

  const clips = visibleClips.map((c) => {
    const cached = getPeaks(c.url);
    return { ...c, peaks: cached?.peaks, duration: cached?.duration };
  });

  // While panning, show the frozen snapshot (translated) rather than the live
  // window, so the waveforms slide instead of re-rendering each frame.
  const displayClips = isDragging ? frozenClips : clips;

  const isInteracting = isDragging || isScrubbing;

  // Drag the strip to pan the window. use-gesture owns pointer capture, the
  // tap-vs-drag threshold, and event batching; memo holds the window end at
  // gesture start so mid-drag re-renders don't shift the anchor.
  const bindPan = useDrag(
    ({ first, last, tap, movement: [mx], memo }) => {
      if (tap || transcripts.length === 0 || minEnd == null || maxEnd == null) {
        return memo;
      }
      const width = containerRef.current?.clientWidth ?? 0;
      if (!width) return memo;
      const startEnd: number = memo ?? windowEndTime ?? maxEnd;

      if (first) {
        // Snapshot the drawn waveforms; we slide these until the pan ends.
        setFrozenClips(clips);
        setIsDragging(true);
      }

      // Dragging right moves the window back in time (reveals earlier clips).
      const deltaMs = (mx / width) * windowDurationMs;
      const end = clampWindowEnd(startEnd - deltaMs);
      if (end == null) return startEnd;

      if (last) {
        // Settle: commit + remount waveforms now, then drop the snapshot.
        commitWindowEnd(end);
        setWaveformsReady(false);
        setFrozenClips([]);
        setIsDragging(false);
        waveX.set(0);
      } else {
        // Slide the frozen waveforms (urgent), but let the mini-map, labels, and
        // list catch up as a transition so they don't block the gesture.
        waveX.set(((startEnd - end) / windowDurationMs) * width);
        startTransition(() => commitWindowEnd(end));
      }
      return startEnd;
    },
    { filterTaps: true, threshold: DRAG_THRESHOLD_PX }
  );

  // Warm the cache for the window centered on a hovered overview position.
  const prefetchWindowAt = (center: number) => {
    const winStart = center - windowDurationMs / 2;
    const winEnd = center + windowDurationMs / 2;
    const urls: string[] = [];
    for (const t of transcriptTimes) {
      if (urls.length >= MAX_PREFETCH_PER_HOVER) break;
      if (t.startMs >= winEnd || t.endMs <= winStart) continue;
      urls.push(t.url);
    }
    enqueueDecode(urls, false);
  };

  // Mount waveforms a frame after interaction stops, once clips have final width.
  useEffect(() => {
    if (waveformsReady || isInteracting) return;
    const frame = requestAnimationFrame(() => setWaveformsReady(true));
    return () => cancelAnimationFrame(frame);
  }, [waveformsReady, isInteracting]);

  // Decode peaks for the visible window first (enqueueDecode skips cached URLs).
  useEffect(() => {
    if (isInteracting) return;
    const urls = visibleClips.map((c) => c.url);
    if (urls.length > 0) enqueueDecode(urls, true);
  }, [visibleClips, isInteracting, enqueueDecode]);

  // Report the active window edge (debounced) so the parent can show the time.
  useEffect(() => {
    const id = setTimeout(() => {
      onActiveTimeChange?.(
        isPanned && windowEndTime != null ? windowEndTime : null
      );
    }, 120);
    return () => clearTimeout(id);
  }, [isPanned, windowEndTime, onActiveTimeChange]);

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
          ref={containerRef}
          variant="outlined"
          {...bindPan()}
          sx={{
            width: '100%',
            height: '60px',
            bgcolor: 'action.hover',
            position: 'relative',
            touchAction: 'none',
            userSelect: 'none',
            cursor:
              transcripts.length === 0
                ? 'default'
                : isDragging
                  ? 'grabbing'
                  : 'grab',
          }}
        >
          {/* Static clip box: stays fixed to the Paper bounds so waveforms
              sliding past an edge are clipped. The transform lives on the inner
              layer — applying it here would translate the clip region too. */}
          <Box
            sx={{
              position: 'absolute',
              inset: 0,
              overflow: 'hidden',
              borderRadius: 'inherit',
            }}
          >
            {/* Plain motion.div: the motion value must drive `style`, not sx. */}
            <motion.div
              style={{
                position: 'absolute',
                inset: 0,
                x: waveX,
              }}
            >
              {displayClips.map((clip) => (
                <TimelineClip
                  key={clip.id}
                  clip={clip}
                  onClipClick={onClipClick}
                  isDarkTheme={isDarkTheme}
                  theme={theme}
                  showWaveform={waveformsReady && !isScrubbing}
                />
              ))}
            </motion.div>
          </Box>
          {/* Icons ride the same translate and snapshot as the frozen
              waveforms, so they slide in lockstep during a pan instead of
              lagging the deferred window commit. Outside the clip box so the
              lifted icons aren't clipped at the top edge; the wrapper is
              click-through so clips below stay tappable. */}
          <motion.div
            style={{
              position: 'absolute',
              inset: 0,
              pointerEvents: 'none',
              x: waveX,
            }}
          >
            {displayClips.map(
              (clip) =>
                clip.hasAlert && (
                  <CustomAlertIcon
                    key={`alert-${clip.id}`}
                    color="warning"
                    fontSize="medium"
                    data-testid="warning-icon"
                    onClick={() => onClipClick(clip.id)}
                    sx={{
                      position: 'absolute',
                      // Centered over the audio start, lifted above the clip.
                      left: `${clip.left}%`,
                      transform: 'translateX(-11px)',
                      top: -25,
                      zIndex: 1,
                      borderRadius: '50%',
                      cursor: 'pointer',
                      pointerEvents: 'auto',
                    }}
                  />
                )
            )}
          </motion.div>
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
                No transcripts loaded
              </Typography>
            </Box>
          )}
        </Paper>
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'space-between',
            mt: 0.5,
            // Reserve space so the row doesn't jump when transcripts load.
            visibility: transcripts.length > 0 ? 'visible' : 'hidden',
          }}
        >
          {Array.from({ length: 4 }).map((_, i) => (
            <Typography key={i} variant="caption" color="text.secondary">
              {formatClockTime(startTime + (i / 3) * windowDurationMs)}
            </Typography>
          ))}
        </Box>
        <TimelineMiniMap
          transcriptTimes={transcriptTimes}
          rangeStartMs={rangeStartMs}
          maxEnd={maxEnd}
          windowEndTime={windowEndTime}
          windowDurationMs={windowDurationMs}
          isDarkTheme={isDarkTheme}
          onScrubToCenter={(center) =>
            applyWindowEnd(center + windowDurationMs / 2)
          }
          onScrubbingChange={setIsScrubbing}
          onPrefetchWindow={prefetchWindowAt}
        />
      </Box>
    </Box>
  );
}

export default AudioDisplay;
