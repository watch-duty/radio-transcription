import { MAX_WINDOW_DURATION_MS } from '../../utils/timeUtils';

// A transcript reduced to the fields the timeline needs, parsed once.
export interface TranscriptTime {
  id: string;
  startMs: number;
  endMs: number;
  url: string;
  hasAlert: boolean;
}

// A run of activity on the overview strip (clips with no large gap between them).
export interface TimelineCluster {
  startMs: number;
  endMs: number;
}

const MINUTE_MS = 60 * 1000;
const HOUR_MS = 60 * MINUTE_MS;
// Overview time-marker spacings; the smallest one under the target count is used.
const NICE_INTERVALS_MS = [
  MINUTE_MS,
  2 * MINUTE_MS,
  5 * MINUTE_MS,
  10 * MINUTE_MS,
  15 * MINUTE_MS,
  30 * MINUTE_MS,
  HOUR_MS,
  2 * HOUR_MS,
  3 * HOUR_MS,
  6 * HOUR_MS,
  12 * HOUR_MS,
  24 * HOUR_MS,
];
const GRID_TARGET_COUNT = 5;

// The visible window: half the user's clip length, capped to the max window.
export function getWindowDurationMs(userDuration?: string | null): number {
  if (!userDuration) return MAX_WINDOW_DURATION_MS;
  const parsed = parseFloat(userDuration);
  if (isNaN(parsed) || parsed <= 0) return MAX_WINDOW_DURATION_MS;
  const userMs = parsed * 2 * 60 * 1000;
  return Math.min(MAX_WINDOW_DURATION_MS, userMs);
}

export function pickGridIntervalMs(totalMs: number): number {
  for (const interval of NICE_INTERVALS_MS) {
    if (totalMs / interval <= GRID_TARGET_COUNT) return interval;
  }
  return NICE_INTERVALS_MS[NICE_INTERVALS_MS.length - 1];
}

// Split transcripts into runs, breaking wherever a gap exceeds one window.
export function computeClusters(
  times: TranscriptTime[],
  windowMs: number
): TimelineCluster[] {
  if (times.length === 0) return [];
  const sorted = [...times].sort((a, b) => a.startMs - b.startMs);
  const result: TimelineCluster[] = [];
  let curStart = sorted[0].startMs;
  let curEnd = sorted[0].endMs;
  for (let i = 1; i < sorted.length; i++) {
    if (sorted[i].startMs - curEnd > windowMs) {
      result.push({ startMs: curStart, endMs: curEnd });
      curStart = sorted[i].startMs;
      curEnd = sorted[i].endMs;
    } else {
      curEnd = Math.max(curEnd, sorted[i].endMs);
    }
  }
  result.push({ startMs: curStart, endMs: curEnd });
  return result;
}

// Round-interval gridline timestamps spanning [rangeStartMs, maxEnd].
export function computeGridLineTimes(
  rangeStartMs: number,
  maxEnd: number,
  rangeTotalMs: number
): number[] {
  const interval = pickGridIntervalMs(rangeTotalMs);
  const times: number[] = [];
  for (
    let t = Math.ceil(rangeStartMs / interval) * interval;
    t <= maxEnd;
    t += interval
  ) {
    times.push(t);
  }
  return times;
}
