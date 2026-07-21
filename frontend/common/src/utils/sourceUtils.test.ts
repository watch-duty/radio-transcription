import { describe, expect, it } from 'vitest';

import { SourceType } from '../types/feeds.js';
import { isContinuousSource } from './sourceUtils.js';

describe('isContinuousSource', () => {
  it('returns false for undefined or empty source', () => {
    expect(isContinuousSource(undefined)).toBe(false);
  });

  it('returns true for SourceType.BCFY_FEEDS', () => {
    expect(isContinuousSource(SourceType.BCFY_FEEDS)).toBe(true);
  });

  it('returns false for non-continuous call-based source types', () => {
    expect(isContinuousSource(SourceType.BCFY_CALLS)).toBe(false);
    expect(isContinuousSource(SourceType.OPENMHZ)).toBe(false);
    expect(isContinuousSource(SourceType.ECHO)).toBe(false);
    expect(isContinuousSource(SourceType.FIRE_NOTIFICATIONS)).toBe(false);
  });
});
