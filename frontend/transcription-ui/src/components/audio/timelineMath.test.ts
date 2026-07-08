import { describe, expect, it } from 'vitest';

import {
  clamp,
  computeGridLineTimes,
  msToPct,
  opacityForCount,
} from './timelineMath';

describe('clamp', () => {
  it('bounds a value to [min, max]', () => {
    expect(clamp(5, 0, 10)).toBe(5);
    expect(clamp(-1, 0, 10)).toBe(0);
    expect(clamp(11, 0, 10)).toBe(10);
  });
});

describe('msToPct', () => {
  it('maps a time to its percentage within the range', () => {
    expect(msToPct(0, 0, 100)).toBe(0);
    expect(msToPct(50, 0, 100)).toBe(50);
    expect(msToPct(100, 0, 100)).toBe(100);
  });
});

describe('opacityForCount', () => {
  it('is 0 for empty buckets and ramps 0.5 -> 1 by 5 clips', () => {
    expect(opacityForCount(0)).toBe(0);
    expect(opacityForCount(1)).toBe(0.5);
    expect(opacityForCount(5)).toBe(1);
    expect(opacityForCount(50)).toBe(1);
    expect(opacityForCount(3)).toBeCloseTo(0.75, 5);
  });
});

describe('computeGridLineTimes', () => {
  it('produces 6h-spaced local-aligned gridlines across the range', () => {
    // A 24h range yields 4 interior 6h marks (00:00/06:00/12:00/18:00 local).
    const start = new Date('2026-06-20T00:00:00').getTime();
    const end = new Date('2026-06-21T00:00:00').getTime();
    const times = computeGridLineTimes(start, end);

    expect(times.length).toBeGreaterThanOrEqual(4);
    for (const t of times) {
      const d = new Date(t);
      expect(d.getHours() % 6).toBe(0);
      expect(d.getMinutes()).toBe(0);
    }
    for (let i = 1; i < times.length; i++) {
      expect(times[i]).toBeGreaterThan(times[i - 1]);
    }
    expect(times[0]).toBeGreaterThanOrEqual(start);
    expect(times[times.length - 1]).toBeLessThanOrEqual(end);
  });
});
