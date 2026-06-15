// A single overview histogram bucket positioned in wall-clock time.
export interface HistogramMark {
  startMs: number;
  endMs: number;
  count: number;
  hasAlert: boolean;
}

// Bucket opacity by count: 1 clip = 50%, ramping to fully opaque at 5+.
export function opacityForCount(count: number): number {
  if (count <= 0) return 0;
  return clamp(0.5 + (0.5 * (count - 1)) / 4, 0.5, 1);
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

export function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

export function msToPct(
  ms: number,
  rangeStartMs: number,
  rangeTotalMs: number
): number {
  return ((ms - rangeStartMs) / rangeTotalMs) * 100;
}

export function pickGridIntervalMs(totalMs: number): number {
  for (const interval of NICE_INTERVALS_MS) {
    if (totalMs / interval <= GRID_TARGET_COUNT) return interval;
  }
  return NICE_INTERVALS_MS[NICE_INTERVALS_MS.length - 1];
}

// Round-interval gridline timestamps over [rangeStartMs, maxEnd], aligned to
// local boundaries (e.g. local midnight) not UTC so day boundaries land on a
// mark. Uses the offset at rangeStartMs; a mid-range DST shift is fine here.
export function computeGridLineTimes(
  rangeStartMs: number,
  maxEnd: number,
  rangeTotalMs: number
): number[] {
  const interval = pickGridIntervalMs(rangeTotalMs);
  const offsetMs = new Date(rangeStartMs).getTimezoneOffset() * 60 * 1000;
  const times: number[] = [];
  // Step through local-epoch space (utc - offset) so multiples fall on local rounds.
  for (
    let local = Math.ceil((rangeStartMs - offsetMs) / interval) * interval;
    local + offsetMs <= maxEnd;
    local += interval
  ) {
    times.push(local + offsetMs);
  }
  return times;
}
