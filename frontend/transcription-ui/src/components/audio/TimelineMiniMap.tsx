import { useCallback, useMemo, useRef } from 'react';

import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { alpha, useTheme } from '@mui/material/styles';

import { type HistogramMark } from '../../hooks/useTimelineHistogram';
import { formatClockTime, formatMonthDay } from '../../utils/timeUtils';
import {
  clamp,
  computeGridLineTimes,
  markColor,
  markRank,
  msToPct,
  opacityForCount,
} from './timelineMath';

// Thinnest a mark may render so 1-2s clips stay visible.
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
  onCenterWindow: (centerMs: number) => void;
}

export function TimelineMiniMap({
  histogramMarks,
  rangeStartMs,
  maxEnd,
  windowEndTime,
  windowDurationMs,
  isDarkTheme,
  onCenterWindow,
}: TimelineMiniMapProps) {
  const theme = useTheme();
  const stripRef = useRef<HTMLDivElement>(null);
  // Match the listening lozenge (TimelinePlayhead success state) so the viewport
  // reads as the live/now marker.
  const viewportAccent = theme.palette.success.main;

  const rangeTotalMs =
    maxEnd != null && rangeStartMs != null ? maxEnd - rangeStartMs : 0;
  const showMiniMap = rangeTotalMs > windowDurationMs;

  // Hide the viewport cursor when the window sits before the range (a date
  // filter older than the live 24h the overview always shows).
  const viewportEnd = windowEndTime ?? maxEnd;
  const showViewport =
    viewportEnd != null && rangeStartMs != null && viewportEnd > rangeStartMs;

  const densityMarks = useMemo(
    () => [...histogramMarks].sort((a, b) => markRank(a) - markRank(b)),
    [histogramMarks]
  );
  const gridLineTimes = useMemo(() => {
    if (!showMiniMap || rangeStartMs == null || maxEnd == null) return [];
    return computeGridLineTimes(rangeStartMs, maxEnd);
  }, [showMiniMap, rangeStartMs, maxEnd]);

  const pctOf = (ms: number) => msToPct(ms, rangeStartMs ?? 0, rangeTotalMs);

  const handleStripClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      const el = stripRef.current;
      if (!el || rangeStartMs == null || rangeTotalMs <= 0) return;
      const rect = el.getBoundingClientRect();
      if (!rect.width) return;
      const fraction = clamp((e.clientX - rect.left) / rect.width, 0, 1);
      onCenterWindow(rangeStartMs + fraction * rangeTotalMs);
    },
    [rangeStartMs, rangeTotalMs, onCenterWindow]
  );

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
        // Day boundaries show the date (bold) + time; interior ticks show time.
        dateLabel: isDayBoundary ? formatMonthDay(t) : null,
        timeLabel: formatClockTime(t),
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

  // Clamp left and width as a pair so a window at/past the range end can't push
  // the rectangle beyond the strip's right edge. (Only read when showViewport.)
  const viewportLeftPct = clamp(
    pctOf((windowEndTime ?? maxEnd ?? 0) - windowDurationMs),
    0,
    100
  );
  const viewportWidthPct = Math.min(
    (windowDurationMs / rangeTotalMs) * 100,
    100 - viewportLeftPct
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
          ref={stripRef}
          aria-label="timeline overview"
          onClick={showMiniMap ? handleStripClick : undefined}
          sx={{
            position: 'relative',
            height: '16px',
            borderRadius: 1,
            border: 1,
            borderColor: 'divider',
            bgcolor: 'action.hover',
            overflow: 'hidden',
            cursor: showMiniMap ? 'pointer' : 'default',
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
                  bgcolor: markColor(mark, isDarkTheme),
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
              borderColor: viewportAccent,
              borderRadius: 1,
              boxSizing: 'border-box',
              bgcolor: alpha(viewportAccent, isDarkTheme ? 0.16 : 0.12),
              left: `${viewportLeftPct}%`,
              width: `${viewportWidthPct}%`,
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
          gridLabels.map(({ t, pct, anchor, dateLabel, timeLabel }) => (
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
              {dateLabel && (
                <Box component="span" sx={{ fontWeight: 700 }}>
                  {dateLabel}{' '}
                </Box>
              )}
              {timeLabel}
            </Typography>
          ))}
      </Box>
    </>
  );
}

export default TimelineMiniMap;
