import { useCallback, useEffect, useMemo, useRef } from 'react';

import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { useGesture } from '@use-gesture/react';

import { formatClockTime } from '../../utils/timeUtils';
import {
  type TranscriptTime,
  computeClusters,
  computeGridLineTimes,
} from './timelineMath';

// Thinnest a mark/tick may render so 1-2s clips stay visible.
const MIN_OVERVIEW_MARK_PX = 4;
// Delay before a settled hover warms the cache, so passing-through doesn't decode.
const HOVER_PREFETCH_DELAY_MS = 150;

interface TimelineMiniMapProps {
  transcriptTimes: TranscriptTime[];
  rangeStartMs: number | null;
  maxEnd: number | null;
  windowEndTime: number | null;
  windowDurationMs: number;
  isDarkTheme: boolean;
  // centerMs is the timestamp under the pointer along the strip.
  onScrubToCenter: (centerMs: number) => void;
  onScrubbingChange: (scrubbing: boolean) => void;
  onPrefetchWindow: (centerMs: number) => void;
}

export function TimelineMiniMap({
  transcriptTimes,
  rangeStartMs,
  maxEnd,
  windowEndTime,
  windowDurationMs,
  isDarkTheme,
  onScrubToCenter,
  onScrubbingChange,
  onPrefetchWindow,
}: TimelineMiniMapProps) {
  const miniMapRef = useRef<HTMLDivElement>(null);
  const hoverTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const rangeTotalMs =
    maxEnd != null && rangeStartMs != null ? maxEnd - rangeStartMs : 0;
  // Hidden when the whole loaded range fits in one window.
  const showMiniMap = rangeTotalMs > windowDurationMs;

  const clusters = useMemo(
    () => computeClusters(transcriptTimes, windowDurationMs),
    [transcriptTimes, windowDurationMs]
  );
  const ruleMatchTimes = useMemo(
    () =>
      transcriptTimes
        .filter((t) => t.hasAlert)
        .map((t) => (t.startMs + t.endMs) / 2),
    [transcriptTimes]
  );
  const gridLineTimes = useMemo(() => {
    if (!showMiniMap || rangeStartMs == null || maxEnd == null) return [];
    return computeGridLineTimes(rangeStartMs, maxEnd, rangeTotalMs);
  }, [showMiniMap, rangeStartMs, maxEnd, rangeTotalMs]);

  const pctOf = (ms: number) =>
    ((ms - (rangeStartMs ?? 0)) / rangeTotalMs) * 100;

  const centerMsAtClientX = useCallback(
    (clientX: number): number | null => {
      const el = miniMapRef.current;
      if (!el || rangeStartMs == null || rangeTotalMs <= 0) return null;
      const rect = el.getBoundingClientRect();
      if (!rect.width) return null;
      const fraction = Math.min(
        1,
        Math.max(0, (clientX - rect.left) / rect.width)
      );
      return rangeStartMs + fraction * rangeTotalMs;
    },
    [rangeStartMs, rangeTotalMs]
  );

  const scrubToClientX = (clientX: number) => {
    const center = centerMsAtClientX(clientX);
    if (center != null) onScrubToCenter(center);
  };

  useEffect(() => {
    return () => {
      if (hoverTimeoutRef.current) clearTimeout(hoverTimeoutRef.current);
    };
  }, []);

  // Drag (or tap) scrubs the window; a settled hover warms that window's cache.
  const bind = useGesture({
    onDrag: ({ first, last, xy: [x] }) => {
      if (!showMiniMap) return;
      if (first) {
        if (hoverTimeoutRef.current) clearTimeout(hoverTimeoutRef.current);
        onScrubbingChange(true);
      }
      scrubToClientX(x);
      if (last) onScrubbingChange(false);
    },
    onMove: ({ dragging, xy: [x] }) => {
      if (dragging || !showMiniMap) return;
      if (hoverTimeoutRef.current) clearTimeout(hoverTimeoutRef.current);
      hoverTimeoutRef.current = setTimeout(() => {
        const center = centerMsAtClientX(x);
        if (center != null) onPrefetchWindow(center);
      }, HOVER_PREFETCH_DELAY_MS);
    },
  });

  return (
    <>
      <Box
        ref={miniMapRef}
        {...bind()}
        aria-label="timeline overview"
        sx={{
          position: 'relative',
          height: '16px',
          mt: 0.5,
          borderRadius: 1,
          border: 1,
          borderColor: 'divider',
          bgcolor: 'action.hover',
          cursor: 'pointer',
          touchAction: 'none',
          userSelect: 'none',
          // Clip the viewport rectangle and ticks to the rounded border.
          overflow: 'hidden',
          // Keep the row's space reserved so the UI doesn't jump.
          visibility: showMiniMap ? 'visible' : 'hidden',
        }}
      >
        {showMiniMap &&
          gridLineTimes.map((t) => (
            <Box
              key={`grid-${t}`}
              sx={{
                position: 'absolute',
                top: 0,
                bottom: 0,
                width: '1px',
                bgcolor: 'divider',
                pointerEvents: 'none',
                left: `${pctOf(t)}%`,
              }}
            />
          ))}
        {showMiniMap &&
          clusters.map((cluster, i) => (
            <Box
              key={i}
              sx={{
                position: 'absolute',
                top: '50%',
                transform: 'translateY(-50%)',
                height: '8px',
                borderRadius: '2px',
                bgcolor: 'text.secondary',
                pointerEvents: 'none',
                left: `${pctOf(cluster.startMs)}%`,
                width: `${Math.max(pctOf(cluster.endMs) - pctOf(cluster.startMs), 0.5)}%`,
                minWidth: `${MIN_OVERVIEW_MARK_PX}px`,
              }}
            />
          ))}
        {showMiniMap &&
          ruleMatchTimes.map((t, i) => (
            <Box
              key={`match-${i}`}
              sx={{
                position: 'absolute',
                top: '1px',
                bottom: '1px',
                width: `${MIN_OVERVIEW_MARK_PX}px`,
                bgcolor: 'warning.main',
                pointerEvents: 'none',
                left: `${pctOf(t)}%`,
                transform: 'translateX(-50%)',
              }}
            />
          ))}
        {showMiniMap && maxEnd != null && (
          <Box
            sx={{
              position: 'absolute',
              top: 0,
              bottom: 0,
              pointerEvents: 'none',
              border: 2,
              borderColor: 'primary.main',
              borderRadius: 1,
              boxSizing: 'border-box',
              bgcolor: isDarkTheme
                ? 'rgba(144, 202, 249, 0.16)'
                : 'rgba(25, 118, 210, 0.12)',
              left: `${Math.min(Math.max(pctOf((windowEndTime ?? maxEnd) - windowDurationMs), 0), 100)}%`,
              width: `${Math.min((windowDurationMs / rangeTotalMs) * 100, 100)}%`,
            }}
          />
        )}
      </Box>
      <Box
        sx={{
          position: 'relative',
          height: '1.25em',
          mt: 0.25,
          // Reserve space so the layout doesn't jump when the strip appears.
          visibility: showMiniMap ? 'visible' : 'hidden',
        }}
      >
        {showMiniMap && rangeStartMs != null && maxEnd != null && (
          <>
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{
                position: 'absolute',
                top: 0,
                left: 0,
                whiteSpace: 'nowrap',
              }}
            >
              {formatClockTime(rangeStartMs)}
            </Typography>
            <Typography
              variant="caption"
              color="text.secondary"
              sx={{
                position: 'absolute',
                top: 0,
                right: 0,
                whiteSpace: 'nowrap',
              }}
            >
              {formatClockTime(maxEnd)}
            </Typography>
            {gridLineTimes
              // Skip labels hugging an end so they don't collide with endpoints.
              .filter((t) => pctOf(t) > 6 && pctOf(t) < 94)
              .map((t) => (
                <Typography
                  key={`grid-label-${t}`}
                  variant="caption"
                  color="text.secondary"
                  sx={{
                    position: 'absolute',
                    top: 0,
                    left: `${pctOf(t)}%`,
                    transform: 'translateX(-50%)',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {formatClockTime(t)}
                </Typography>
              ))}
          </>
        )}
      </Box>
    </>
  );
}

export default TimelineMiniMap;
