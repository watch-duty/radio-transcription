export interface TranscriptTime {
  id: string;
  startMs: number;
  endMs: number;
  hasAlert: boolean;
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

// Round-interval gridline timestamps spanning [rangeStartMs, maxEnd], aligned to
// round *local* boundaries (e.g. local midnight) rather than UTC, so day
// boundaries land on a mark. Uses the offset at rangeStartMs; a DST change
// mid-range can shift later marks by an hour, which is fine for an overview.
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

// The segment at the window's right edge, for scrolling the list to match.
// `times` is newest-first, so the first starting at/before `end` is it; falls
// back to the oldest, else null.
export function representativeSegmentId(
  times: TranscriptTime[],
  end: number
): string | null {
  for (const t of times) {
    if (t.startMs <= end) return t.id;
  }
  return times[times.length - 1]?.id ?? null;
}
