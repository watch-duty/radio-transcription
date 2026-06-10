import { describe, expect, it } from 'vitest';

import { MAX_WINDOW_DURATION_MS } from '../../utils/timeUtils';
import {
  type TranscriptTime,
  computeClusters,
  computeGridLineTimes,
  getWindowDurationMs,
  pickGridIntervalMs,
} from './timelineMath';

const MINUTE = 60 * 1000;
const HOUR = 60 * MINUTE;

const time = (startMs: number, endMs: number): TranscriptTime => ({
  id: `${startMs}`,
  startMs,
  endMs,
  url: '',
  hasAlert: false,
});

describe('getWindowDurationMs', () => {
  it('falls back to the max window when unset or invalid', () => {
    expect(getWindowDurationMs(null)).toBe(MAX_WINDOW_DURATION_MS);
    expect(getWindowDurationMs('0')).toBe(MAX_WINDOW_DURATION_MS);
    expect(getWindowDurationMs('abc')).toBe(MAX_WINDOW_DURATION_MS);
  });

  it('doubles the user duration (minutes) and caps at the max window', () => {
    expect(getWindowDurationMs('5')).toBe(5 * 2 * MINUTE);
    expect(getWindowDurationMs('60')).toBe(MAX_WINDOW_DURATION_MS);
  });
});

describe('pickGridIntervalMs', () => {
  it('picks the smallest interval keeping the count under the target', () => {
    expect(pickGridIntervalMs(4 * MINUTE)).toBe(MINUTE);
    expect(pickGridIntervalMs(50 * MINUTE)).toBe(10 * MINUTE);
  });

  it('caps at the largest interval for very wide ranges', () => {
    expect(pickGridIntervalMs(1000 * HOUR)).toBe(24 * HOUR);
  });
});

describe('computeClusters', () => {
  it('returns an empty list for no transcripts', () => {
    expect(computeClusters([], MINUTE)).toEqual([]);
  });

  it('merges runs and splits on gaps larger than the window', () => {
    const clusters = computeClusters(
      [
        time(0, MINUTE),
        time(2 * MINUTE, 3 * MINUTE),
        time(10 * HOUR, 11 * HOUR),
      ],
      5 * MINUTE
    );
    expect(clusters).toEqual([
      { startMs: 0, endMs: 3 * MINUTE },
      { startMs: 10 * HOUR, endMs: 11 * HOUR },
    ]);
  });
});

describe('computeGridLineTimes', () => {
  it('emits round-interval marks within the range', () => {
    expect(computeGridLineTimes(0, 4 * MINUTE, 4 * MINUTE)).toEqual([
      0,
      MINUTE,
      2 * MINUTE,
      3 * MINUTE,
      4 * MINUTE,
    ]);
  });
});
