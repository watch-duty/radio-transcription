import React, {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';

import PauseIcon from '@mui/icons-material/PauseCircleFilledOutlined';
import PlayArrowIcon from '@mui/icons-material/PlayCircleFilledOutlined';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import { type Theme, useTheme } from '@mui/material/styles';
import type { Transcript } from '@transcription/common';
import WavesurferPlayer from '@wavesurfer/react';

import { getAudioUrl } from '../../utils/audioUtils';
import { hasEvaluationAlert } from '../../utils/evaluationUtils';
import { formatClockTime } from '../../utils/timeUtils';
import { CustomAlertIcon } from '../common/AlertIcon';
import { TimelineMiniMap } from './TimelineMiniMap';
import {
  type TranscriptTime,
  clamp,
  getWindowDurationMs,
  msToPct,
  scrollPosToMs,
  windowEndToScrollLeft,
} from './timelineMath';
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
// Cap clips warmed per hover so a wide window doesn't flood the decode queue.
const MAX_PREFETCH_PER_HOVER = 16;
// Idle after a scroll before we mount waveforms for the settled viewport.
const SETTLE_MS = 140;
// Most waveforms mounted at once (viewport + margin), bounding canvas/decode cost.
const MAX_MOUNTED = 48;

interface ClipGeom {
  id: string;
  url: string;
  startMs: number;
  endMs: number;
  left: number;
  width: number;
  isAudioPlaying: boolean;
  isHighlighted: boolean;
  hasAlert: boolean;
}

interface TimelineClipProps {
  clip: ClipGeom & {
    peaks?: (Float32Array | number[])[];
    duration?: number;
  };
  onClipClick: (segmentId: string) => void;
  isDarkTheme: boolean;
  theme: Theme;
  // Only mounted clips draw a waveform; the rest stay cheap placeholder divs.
  mount: boolean;
  // False during scrub; churning players thrashes loads → blank waveforms.
  showWaveform: boolean;
}

const TimelineClip = React.memo(
  ({
    clip,
    onClipClick,
    isDarkTheme,
    theme,
    mount,
    showWaveform,
  }: TimelineClipProps) => {
    const renderWaveform = mount && showWaveform && !!clip.peaks;
    return (
      <Box
        onClick={() => onClipClick(clip.id)}
        sx={{
          position: 'absolute',
          left: `${clip.left}%`,
          width: `${clip.width}%`,
          height: '100%',
          // Skip layout/paint for the many off-screen placeholders; never for a
          // mounted clip, so WaveSurfer can measure its width at mount.
          contentVisibility: mount ? 'visible' : 'auto',
          containIntrinsicSize: 'auto 60px',
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
      prevProps.mount === nextProps.mount &&
      prevProps.isDarkTheme === nextProps.isDarkTheme &&
      prevProps.theme === nextProps.theme &&
      prevProps.showWaveform === nextProps.showWaveform
    );
  }
);

const sameSet = (set: Set<string>, ids: string[]) =>
  set.size === ids.length && ids.every((id) => set.has(id));

interface AlertIconLayerProps {
  clips: ClipGeom[];
  widthPct: number;
  onClipClick: (segmentId: string) => void;
}

// Alert-icon layer. The forwarded ref's element is translated imperatively to
// track scroll, so this is never re-rendered per scroll frame; memoized so it
// only rebuilds when the alert set or width changes. The static outer box clips
// icons to the left/right/bottom while the negative top inset leaves the lifted
// glyphs uncropped; the clip must live here, not on the translated child.
const AlertIconLayer = React.memo(
  React.forwardRef<HTMLDivElement, AlertIconLayerProps>(
    ({ clips, widthPct, onClipClick }, ref) => (
      <Box
        sx={{
          position: 'absolute',
          inset: 0,
          clipPath: 'inset(-9999px 0px 0px 0px)',
          pointerEvents: 'none',
        }}
      >
        <Box
          ref={ref}
          sx={{
            position: 'absolute',
            top: 0,
            left: 0,
            height: '100%',
            width: `${widthPct}%`,
            willChange: 'transform',
          }}
        >
          {clips.map((clip) => (
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
          ))}
        </Box>
      </Box>
    )
  )
);
AlertIconLayer.displayName = 'AlertIconLayer';

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

  // Derived from scrollLeft on every scroll; drives the mini-map and labels.
  const [windowEndTime, setWindowEndTime] = useState<number | null>(null);
  const [isScrubbing, setIsScrubbing] = useState(false);
  // Clips currently allowed to draw a waveform (the settled viewport set).
  const [mountedIds, setMountedIds] = useState<Set<string>>(
    () => new Set<string>()
  );

  const scrollerRef = useRef<HTMLDivElement>(null);
  // Alert-icon layer; translated imperatively to track scroll without a render.
  const iconLayerRef = useRef<HTMLDivElement>(null);
  // The window end-time to keep pinned at the right edge across data growth;
  // null means "follow the live edge".
  const anchorEndTimeRef = useRef<number | null>(null);
  // The scrollLeft we last set ourselves; the matching scroll event is ours, not
  // the user's. Compared against the actual position rather than timed, so it
  // can't race the scroll event. Consumed once.
  const expectedScrollLeftRef = useRef<number | null>(null);
  const rafScrollRef = useRef(0);
  const settleTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Dedupe so a pan doesn't re-scroll the list to the same index each frame.
  const lastPannedSegmentRef = useRef<string | null>(null);
  const prevNonceRef = useRef(viewLiveNonce);
  const prevHighlightRef = useRef(highlightedSegmentId);

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

  // rangeStartMs = strip's left edge (oldest); maxEnd = live edge (newest).
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

  // Panned = the window's right edge sits before the live edge.
  const isPanned =
    windowEndTime != null &&
    maxEnd != null &&
    windowEndTime < maxEnd - LIVE_EDGE_EPS_MS;

  const rangeTotalMs =
    rangeStartMs != null && maxEnd != null
      ? Math.max(maxEnd - rangeStartMs, 1)
      : 0;
  // Inner element spans the whole range: one viewport width = one window.
  const innerWidthPct =
    rangeTotalMs > 0
      ? Math.max((rangeTotalMs / windowDurationMs) * 100, 100)
      : 100;

  // Absolute positions across the full range; geometry only, so peak decodes
  // don't rebuild it. Sorted ascending for the settled-viewport scan.
  const clipsAsc = useMemo(() => {
    if (rangeStartMs == null || rangeTotalMs <= 0) return [] as ClipGeom[];
    return transcriptTimes
      .map((t) => ({
        id: t.id,
        url: t.url,
        startMs: t.startMs,
        endMs: t.endMs,
        left: msToPct(t.startMs, rangeStartMs, rangeTotalMs),
        width: Math.max(((t.endMs - t.startMs) / rangeTotalMs) * 100, 0),
        isAudioPlaying: t.id === currentlyPlayingSegmentId,
        isHighlighted: t.id === highlightedSegmentId,
        hasAlert: t.hasAlert,
      }))
      .sort((a, b) => a.startMs - b.startMs);
  }, [
    transcriptTimes,
    rangeStartMs,
    rangeTotalMs,
    currentlyPlayingSegmentId,
    highlightedSegmentId,
  ]);

  // Only alerted clips get an icon; memoized so the icon layer doesn't rebuild
  // on every scroll-driven render.
  const alertClips = useMemo(
    () => clipsAsc.filter((c) => c.hasAlert),
    [clipsAsc]
  );

  // Latest values for imperative handlers (scroll, settle) without stale
  // closures. Synced in a layout effect below (writing refs during render is
  // disallowed), kept ahead of the scroll effects so they read fresh values.
  const stateRef = useRef({
    rangeStartMs,
    rangeTotalMs,
    minEnd,
    maxEnd,
    windowDurationMs,
    transcriptTimes,
    isPanned,
  });
  const clipsAscRef = useRef(clipsAsc);
  const enqueueDecodeRef = useRef(enqueueDecode);
  const onWindowPanRef = useRef(onWindowPan);

  useLayoutEffect(() => {
    stateRef.current = {
      rangeStartMs,
      rangeTotalMs,
      minEnd,
      maxEnd,
      windowDurationMs,
      transcriptTimes,
      isPanned,
    };
    clipsAscRef.current = clipsAsc;
    enqueueDecodeRef.current = enqueueDecode;
    onWindowPanRef.current = onWindowPan;
  });

  const syncIconLayer = useCallback((scrollLeft: number) => {
    if (iconLayerRef.current) {
      iconLayerRef.current.style.transform = `translateX(${-scrollLeft}px)`;
    }
  }, []);

  const findRepresentativeSegmentId = useCallback(
    (end: number): string | null => {
      const times = stateRef.current.transcriptTimes;
      for (const t of times) {
        if (t.startMs <= end) return t.id;
      }
      return times[times.length - 1]?.id ?? null;
    },
    []
  );

  // Scroll the transcript list to the clip at the window's right edge, so the
  // clips on screen in the timeline stay in view in the list. Deduped so a drag
  // (or scrub) that stays on one clip doesn't re-scroll the list each frame.
  const notifyWindowPan = useCallback(
    (end: number) => {
      const segId = findRepresentativeSegmentId(end);
      if (segId && segId !== lastPannedSegmentRef.current) {
        lastPannedSegmentRef.current = segId;
        onWindowPanRef.current?.(segId);
      }
    },
    [findRepresentativeSegmentId]
  );

  // Pick the clips inside the settled viewport (+ margin) and mount their
  // waveforms; warm their peaks. Runs on settle, not per scroll frame.
  const computeMounted = useCallback(() => {
    const el = scrollerRef.current;
    const { rangeStartMs, rangeTotalMs } = stateRef.current;
    const clips = clipsAscRef.current;
    if (
      !el ||
      rangeStartMs == null ||
      rangeTotalMs <= 0 ||
      el.scrollWidth <= 0
    ) {
      setMountedIds((prev) => (prev.size === 0 ? prev : new Set<string>()));
      return;
    }
    const startT =
      rangeStartMs + (el.scrollLeft / el.scrollWidth) * rangeTotalMs;
    const endT =
      rangeStartMs +
      ((el.scrollLeft + el.clientWidth) / el.scrollWidth) * rangeTotalMs;
    const margin = (endT - startT) * 0.5;
    const lo = startT - margin;
    const hi = endT + margin;
    const inView: ClipGeom[] = [];
    for (const c of clips) {
      if (c.startMs > hi) break;
      if (c.endMs > lo) inView.push(c);
    }
    // Decode every clip in view+margin, even past the mount cap, so an on-screen
    // clip beyond the cap still loads its peaks (it just renders as a placeholder
    // until it falls inside the mounted run).
    if (inView.length > 0) {
      enqueueDecodeRef.current(
        inView.map((c) => c.url),
        true
      );
    }
    // Mount a contiguous run centered on the viewport (clips are start-sorted),
    // bounding live canvases without scattering the mounted set across the strip.
    let toMount = inView;
    if (inView.length > MAX_MOUNTED) {
      const center = (startT + endT) / 2;
      let nearest = 0;
      let best = Infinity;
      for (let i = 0; i < inView.length; i++) {
        const d = Math.abs((inView[i].startMs + inView[i].endMs) / 2 - center);
        if (d < best) {
          best = d;
          nearest = i;
        }
      }
      const start = clamp(
        nearest - Math.floor(MAX_MOUNTED / 2),
        0,
        inView.length - MAX_MOUNTED
      );
      toMount = inView.slice(start, start + MAX_MOUNTED);
    }
    const ids = toMount.map((c) => c.id);
    setMountedIds((prev) => (sameSet(prev, ids) ? prev : new Set(ids)));
  }, []);

  const scheduleMounted = useCallback(() => {
    if (settleTimerRef.current) clearTimeout(settleTimerRef.current);
    settleTimerRef.current = setTimeout(() => {
      settleTimerRef.current = null;
      computeMounted();
    }, SETTLE_MS);
  }, [computeMounted]);

  // Record the end-time to hold at the right edge; null (follow live) once the
  // end reaches the live edge, so live-follow is never poisoned by a stale anchor.
  const setAnchor = useCallback((end: number) => {
    const { maxEnd } = stateRef.current;
    anchorEndTimeRef.current =
      maxEnd != null && end >= maxEnd - LIVE_EDGE_EPS_MS ? null : end;
  }, []);

  // Scroll the window so `end` sits at the right edge (programmatic move).
  const scrollToWindowEnd = useCallback(
    (end: number) => {
      const el = scrollerRef.current;
      const { rangeStartMs, rangeTotalMs } = stateRef.current;
      if (!el || rangeStartMs == null || rangeTotalMs <= 0) return;
      const maxScroll = el.scrollWidth - el.clientWidth;
      const target = clamp(
        windowEndToScrollLeft(
          end,
          rangeStartMs,
          rangeTotalMs,
          el.scrollWidth,
          el.clientWidth
        ),
        0,
        maxScroll
      );
      // Only a real position change emits a scroll event; flag it so the handler
      // ignores our own move. If nothing changes, no event comes, so don't flag.
      if (Math.abs(target - el.scrollLeft) > 0.5) {
        expectedScrollLeftRef.current = target;
        el.scrollLeft = target;
      }
      syncIconLayer(target);
      setWindowEndTime(end);
      scheduleMounted();
    },
    [scheduleMounted, syncIconLayer]
  );

  // Move the window to a chosen end-time (mini-map scrub, highlight recenter):
  // remember it as the anchor and scroll there.
  const goToWindowEnd = useCallback(
    (end: number) => {
      const { minEnd, maxEnd } = stateRef.current;
      if (minEnd == null || maxEnd == null) return;
      const clamped = clamp(end, minEnd, maxEnd);
      setAnchor(clamped);
      scrollToWindowEnd(clamped);
    },
    [scrollToWindowEnd, setAnchor]
  );

  const handleScroll = useCallback(() => {
    if (rafScrollRef.current) return;
    rafScrollRef.current = requestAnimationFrame(() => {
      rafScrollRef.current = 0;
      const el = scrollerRef.current;
      const { rangeStartMs, rangeTotalMs } = stateRef.current;
      if (
        !el ||
        rangeStartMs == null ||
        rangeTotalMs <= 0 ||
        el.scrollWidth <= 0
      )
        return;
      syncIconLayer(el.scrollLeft);
      // Our own programmatic scroll? Identify by the position we set, consumed
      // once, so it can't be mistaken for a user pan.
      const expected = expectedScrollLeftRef.current;
      expectedScrollLeftRef.current = null;
      if (expected !== null && Math.abs(el.scrollLeft - expected) <= 1.5) {
        scheduleMounted();
        return;
      }
      const end = scrollPosToMs(
        el.scrollLeft + el.clientWidth,
        rangeStartMs,
        rangeTotalMs,
        el.scrollWidth
      );
      setWindowEndTime(end);
      setAnchor(end);
      notifyWindowPan(end);
      scheduleMounted();
    });
  }, [notifyWindowPan, scheduleMounted, setAnchor, syncIconLayer]);

  // Mini-map scrub: move the window to the chosen center and scroll the list to
  // match, so a click on the overview brings those clips into view in the list.
  const handleMiniMapScrub = useCallback(
    (center: number) => {
      const end = center + windowDurationMs / 2;
      goToWindowEnd(end);
      const { minEnd, maxEnd } = stateRef.current;
      if (minEnd == null || maxEnd == null) return;
      notifyWindowPan(clamp(end, minEnd, maxEnd));
    },
    [windowDurationMs, goToWindowEnd, notifyWindowPan]
  );

  // Keep the right edge pinned across data growth and zoom: follow the live edge
  // when not panned; otherwise hold the anchored end-time steady (so paging in
  // older history doesn't shift the viewport). "Jump to live" resets the anchor.
  useLayoutEffect(() => {
    const { maxEnd, isPanned } = stateRef.current;
    let target: number | null;
    if (prevNonceRef.current !== viewLiveNonce) {
      prevNonceRef.current = viewLiveNonce;
      anchorEndTimeRef.current = null;
      target = maxEnd;
    } else {
      target = isPanned ? (anchorEndTimeRef.current ?? maxEnd) : maxEnd;
    }
    if (target != null) {
      scrollToWindowEnd(target);
    } else {
      scheduleMounted();
    }
  }, [
    rangeStartMs,
    rangeTotalMs,
    maxEnd,
    windowDurationMs,
    viewLiveNonce,
    scrollToWindowEnd,
    scheduleMounted,
  ]);

  // Recenter on the highlighted (or playing) clip when it leaves the window.
  // While panned, only an explicit highlight change recenters (not playback).
  useLayoutEffect(() => {
    const s = stateRef.current;
    const highlightChanged = highlightedSegmentId !== prevHighlightRef.current;
    prevHighlightRef.current = highlightedSegmentId;
    if (s.minEnd == null || s.maxEnd == null) return;
    const allowShift = !s.isPanned || highlightChanged;
    const targetId = highlightedSegmentId || currentlyPlayingSegmentId;
    if (!allowShift || !targetId) return;
    const target = s.transcriptTimes.find((t) => t.id === targetId);
    if (!target) return;
    const currentEnd = anchorEndTimeRef.current ?? s.maxEnd;
    const currentStart = currentEnd - s.windowDurationMs;
    if (target.startMs < currentStart || target.endMs > currentEnd) {
      goToWindowEnd(target.startMs + s.windowDurationMs / 2);
    }
  }, [highlightedSegmentId, currentlyPlayingSegmentId, goToWindowEnd]);

  // Mouse wheel pans horizontally (a plain wheel has no horizontal axis).
  useEffect(() => {
    const el = scrollerRef.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      if (el.scrollWidth <= el.clientWidth) return;
      const delta =
        Math.abs(e.deltaX) >= Math.abs(e.deltaY) ? e.deltaX : e.deltaY;
      if (delta === 0) return;
      el.scrollLeft += delta;
      e.preventDefault();
    };
    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  }, []);

  useEffect(() => {
    return () => {
      if (rafScrollRef.current) cancelAnimationFrame(rafScrollRef.current);
      if (settleTimerRef.current) clearTimeout(settleTimerRef.current);
    };
  }, []);

  useEffect(() => {
    onPannedChange?.(isPanned);
  }, [isPanned, onPannedChange]);

  // Re-arm so the next pan re-scrolls even if it lands on the same segment.
  useEffect(() => {
    lastPannedSegmentRef.current = null;
  }, [viewLiveNonce]);

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

  // Report the active window edge (debounced) so the parent can show the time.
  useEffect(() => {
    const id = setTimeout(() => {
      onActiveTimeChange?.(
        isPanned && windowEndTime != null ? windowEndTime : null
      );
    }, 120);
    return () => clearTimeout(id);
  }, [isPanned, windowEndTime, onActiveTimeChange]);

  const labelStart = (windowEndTime ?? maxEnd ?? 0) - windowDurationMs;

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
        {/* Paper keeps overflow visible so lifted alert icons aren't clipped;
            the inner scroller owns the horizontal clip + native scrolling. */}
        <Paper
          variant="outlined"
          sx={{
            width: '100%',
            height: '60px',
            bgcolor: 'action.hover',
            position: 'relative',
            overflow: 'visible',
            userSelect: 'none',
          }}
        >
          <Box
            ref={scrollerRef}
            onScroll={handleScroll}
            sx={{
              position: 'absolute',
              inset: 0,
              overflowX: 'auto',
              overflowY: 'hidden',
              borderRadius: 'inherit',
              touchAction: 'pan-x',
              WebkitOverflowScrolling: 'touch',
              // The mini-map shows scroll position, so hide the scrollbar.
              scrollbarWidth: 'none',
              '&::-webkit-scrollbar': { display: 'none' },
            }}
          >
            <Box
              sx={{
                position: 'relative',
                height: '100%',
                width: `${innerWidthPct}%`,
              }}
            >
              {clipsAsc.map((clip) => {
                const mount = mountedIds.has(clip.id);
                // Only mounted clips need peaks; skip the per-clip object spread
                // for the (many) placeholders to avoid allocating across the range.
                if (!mount) {
                  return (
                    <TimelineClip
                      key={clip.id}
                      clip={clip}
                      onClipClick={onClipClick}
                      isDarkTheme={isDarkTheme}
                      theme={theme}
                      mount={false}
                      showWaveform={!isScrubbing}
                    />
                  );
                }
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
                    mount
                    showWaveform={!isScrubbing}
                  />
                );
              })}
            </Box>
          </Box>
          <AlertIconLayer
            ref={iconLayerRef}
            clips={alertClips}
            widthPct={innerWidthPct}
            onClipClick={onClipClick}
          />
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
              {formatClockTime(labelStart + (i / 3) * windowDurationMs)}
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
          onScrubToCenter={handleMiniMapScrub}
          onScrubbingChange={setIsScrubbing}
          onPrefetchWindow={prefetchWindowAt}
        />
      </Box>
    </Box>
  );
}

export default AudioDisplay;
