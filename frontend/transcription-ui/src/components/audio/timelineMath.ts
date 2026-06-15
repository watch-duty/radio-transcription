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

// The overview is a fixed 24h range, so a 6h grid yields ~4 marks at local
// 00:00/06:00/12:00/18:00. Revisit if TIMELINE_RANGE_DURATION_MS becomes variable.
const GRID_INTERVAL_MS = 6 * 60 * 60 * 1000;

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

// Gridline times aligned to local boundaries (not UTC) so day boundaries land
// on a mark. Uses the offset at rangeStartMs; a mid-range DST shift is fine.
export function computeGridLineTimes(
  rangeStartMs: number,
  maxEnd: number
): number[] {
  const offsetMs = new Date(rangeStartMs).getTimezoneOffset() * 60 * 1000;
  const times: number[] = [];
  // Step through local-epoch space (utc - offset) so multiples fall on local rounds.
  for (
    let local =
      Math.ceil((rangeStartMs - offsetMs) / GRID_INTERVAL_MS) *
      GRID_INTERVAL_MS;
    local + offsetMs <= maxEnd;
    local += GRID_INTERVAL_MS
  ) {
    times.push(local + offsetMs);
  }
  return times;
}
