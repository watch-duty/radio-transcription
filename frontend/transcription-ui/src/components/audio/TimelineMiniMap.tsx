import { useCallback, useMemo, useRef } from 'react';

import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';

import { formatClockTime, formatDateTimeShort } from '../../utils/timeUtils';
import {
  type HistogramMark,
  clamp,
  computeGridLineTimes,
  msToPct,
  opacityForCount,
} from './timelineMath';

// Thinnest a mark/tick may render so 1-2s clips stay visible.
const MIN_OVERVIEW_MARK_PX = 4;
// Within this of an edge, anchor a label to the edge instead of centering it.
const EDGE_LABEL_MARGIN_PCT = 4;
// Drop an interior label that lands closer than this to a day-boundary label.
const MIN_LABEL_GAP_PCT = 8;

interface TimelineMiniMapProps {
  histogramMarks: HistogramMark[];
  rangeStartMs: number | null;
  maxEnd: number | null;
  windowEndTime: number | null;
  windowDurationMs: number;
  isDarkTheme: boolean;
  onScrubToCenter: (centerMs: number) => void;
}

export function TimelineMiniMap({
  histogramMarks,
  rangeStartMs,
  maxEnd,
  windowEndTime,
  windowDurationMs,
  isDarkTheme,
  onScrubToCenter,
}: TimelineMiniMapProps) {
  const miniMapRef = useRef<HTMLDivElement>(null);

  const rangeTotalMs =
    maxEnd != null && rangeStartMs != null ? maxEnd - rangeStartMs : 0;
  const showMiniMap = rangeTotalMs > windowDurationMs;

  // Hide the viewport cursor when the window sits before the range (a date
  // filter older than the live 24h the overview always shows).
  const viewportEnd = windowEndTime ?? maxEnd;
  const showViewport =
    viewportEnd != null && rangeStartMs != null && viewportEnd > rangeStartMs;

  // Alerted buckets paint last so their tint stays visible where cells overlap.
  const densityMarks = useMemo(
    () =>
      [...histogramMarks].sort(
        (a, b) => Number(a.hasAlert) - Number(b.hasAlert)
      ),
    [histogramMarks]
  );
  const gridLineTimes = useMemo(() => {
    if (!showMiniMap || rangeStartMs == null || maxEnd == null) return [];
    return computeGridLineTimes(rangeStartMs, maxEnd, rangeTotalMs);
  }, [showMiniMap, rangeStartMs, maxEnd, rangeTotalMs]);

  const pctOf = (ms: number) => msToPct(ms, rangeStartMs ?? 0, rangeTotalMs);

  // Day-boundary labels are always kept (the date shows once per day); interior
  // time labels that would crowd one are dropped.
  const gridLabels = useMemo(() => {
    if (rangeStartMs == null || rangeTotalMs <= 0) return [];
    let prevDate = '';
    const labels = gridLineTimes.map((t) => {
      const pct = msToPct(t, rangeStartMs, rangeTotalMs);
      const dateKey = new Date(t).toLocaleDateString();
      const isDayBoundary = dateKey !== prevDate;
      prevDate = dateKey;
      const anchor =
        pct <= EDGE_LABEL_MARGIN_PCT
          ? 'left'
          : pct >= 100 - EDGE_LABEL_MARGIN_PCT
            ? 'right'
            : 'center';
      return {
        t,
        pct,
        anchor,
        isDayBoundary,
        label: isDayBoundary ? formatDateTimeShort(t) : formatClockTime(t),
      };
    });

    return labels.filter((l, i) => {
      if (l.isDayBoundary) return true;
      const prev = labels[i - 1];
      const next = labels[i + 1];
      const crowdsBoundary =
        (prev?.isDayBoundary && l.pct - prev.pct < MIN_LABEL_GAP_PCT) ||
        (next?.isDayBoundary && next.pct - l.pct < MIN_LABEL_GAP_PCT);
      return !crowdsBoundary;
    });
  }, [gridLineTimes, rangeStartMs, rangeTotalMs]);

  const centerMsAtClientX = useCallback(
    (clientX: number): number | null => {
      const el = miniMapRef.current;
      if (!el || rangeStartMs == null || rangeTotalMs <= 0) return null;
      const rect = el.getBoundingClientRect();
      if (!rect.width) return null;
      const fraction = clamp((clientX - rect.left) / rect.width, 0, 1);
      return rangeStartMs + fraction * rangeTotalMs;
    },
    [rangeStartMs, rangeTotalMs]
  );

  return (
    <>
      {/* Wrapper lets the viewport rectangle overflow the strip's height; the
          strip itself clips ticks to its rounded border. */}
      <Box
        sx={{
          position: 'relative',
          mt: 0.5,
          // Keep the row's space reserved so the UI doesn't jump.
          visibility: showMiniMap ? 'visible' : 'hidden',
        }}
      >
        <Box
          ref={miniMapRef}
          aria-label="timeline overview"
          onClick={(e) => {
            if (!showMiniMap) return;
            const center = centerMsAtClientX(e.clientX);
            if (center != null) onScrubToCenter(center);
          }}
          sx={{
            position: 'relative',
            height: '16px',
            borderRadius: 1,
            border: 1,
            borderColor: 'divider',
            bgcolor: 'action.hover',
            cursor: 'pointer',
            userSelect: 'none',
            overflow: 'hidden',
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
            densityMarks.map((mark) => (
              <Box
                key={mark.startMs}
                sx={{
                  position: 'absolute',
                  top: '50%',
                  transform: 'translateY(-50%)',
                  height: '8px',
                  borderRadius: '2px',
                  bgcolor: mark.hasAlert
                    ? 'warning.main'
                    : isDarkTheme
                      ? 'grey.500'
                      : 'grey.700',
                  // Density by opacity: 1 clip reads at 50%, 5+ fully opaque.
                  opacity: opacityForCount(mark.count),
                  pointerEvents: 'none',
                  left: `${pctOf(mark.startMs)}%`,
                  width: `${Math.max(pctOf(mark.endMs) - pctOf(mark.startMs), 0.5)}%`,
                  minWidth: `${MIN_OVERVIEW_MARK_PX}px`,
                }}
              />
            ))}
        </Box>
        {/* Viewport rectangle lives outside the clipped strip and stands a few
            px taller than it, so the current window reads at a glance. */}
        {showMiniMap && maxEnd != null && showViewport && (
          <Box
            data-testid="minimap-viewport"
            sx={{
              position: 'absolute',
              top: '-3px',
              bottom: '-3px',
              pointerEvents: 'none',
              border: 2,
              borderColor: 'primary.main',
              borderRadius: 1,
              boxSizing: 'border-box',
              bgcolor: isDarkTheme
                ? 'rgba(144, 202, 249, 0.16)'
                : 'rgba(25, 118, 210, 0.12)',
              left: `${clamp(pctOf((windowEndTime ?? maxEnd) - windowDurationMs), 0, 100)}%`,
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
          visibility: showMiniMap ? 'visible' : 'hidden',
        }}
      >
        {showMiniMap &&
          gridLabels.map(({ t, pct, anchor, label }) => (
            <Typography
              key={`grid-label-${t}`}
              variant="caption"
              color="text.secondary"
              sx={{
                position: 'absolute',
                top: 0,
                whiteSpace: 'nowrap',
                ...(anchor === 'left'
                  ? { left: 0 }
                  : anchor === 'right'
                    ? { right: 0 }
                    : { left: `${pct}%`, transform: 'translateX(-50%)' }),
              }}
            >
              {label}
            </Typography>
          ))}
      </Box>
    </>
  );
}

export default TimelineMiniMap;
