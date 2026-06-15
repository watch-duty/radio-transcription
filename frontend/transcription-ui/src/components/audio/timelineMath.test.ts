import { describe, expect, it } from 'vitest';

import {
  clamp,
  computeGridLineTimes,
  msToPct,
  opacityForCount,
  pickGridIntervalMs,
} from './timelineMath';

const MINUTE = 60 * 1000;
const HOUR = 60 * MINUTE;

describe('clamp', () => {
  it('constrains to the range', () => {
    expect(clamp(5, 0, 10)).toBe(5);
    expect(clamp(-1, 0, 10)).toBe(0);
    expect(clamp(11, 0, 10)).toBe(10);
  });
});

describe('msToPct', () => {
  it('projects a timestamp to a percentage of the range', () => {
    expect(msToPct(0, 0, 1000)).toBe(0);
    expect(msToPct(250, 0, 1000)).toBe(25);
    expect(msToPct(1000, 0, 1000)).toBe(100);
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

describe('computeGridLineTimes', () => {
  it('emits evenly spaced marks aligned to round local boundaries', () => {
    const interval = pickGridIntervalMs(6 * HOUR);
    const marks = computeGridLineTimes(0, 6 * HOUR, 6 * HOUR);

    expect(marks.length).toBeGreaterThan(0);
    for (const m of marks) {
      expect(m).toBeGreaterThanOrEqual(0);
      expect(m).toBeLessThanOrEqual(6 * HOUR);
      // Each mark is a round boundary in local time.
      const offsetMs = new Date(m).getTimezoneOffset() * 60 * 1000;
      expect((m - offsetMs) % interval).toBe(0);
    }
    for (let i = 1; i < marks.length; i++) {
      expect(marks[i] - marks[i - 1]).toBe(interval);
    }
  });
});

describe('opacityForCount', () => {
  it('is invisible for empty buckets', () => {
    expect(opacityForCount(0)).toBe(0);
  });

  it('reads a single clip at half opacity', () => {
    expect(opacityForCount(1)).toBe(0.5);
  });

  it('ramps linearly between 1 and 5 clips', () => {
    expect(opacityForCount(3)).toBeCloseTo(0.75);
  });

  it('saturates at fully opaque for 5 or more clips', () => {
    expect(opacityForCount(5)).toBe(1);
    expect(opacityForCount(50)).toBe(1);
  });
});
