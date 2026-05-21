import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  getInitialTimestamp,
  getRelativeTimeString,
  getSearchedEndTime,
  getSearchedStartTime,
} from './timeUtils';

describe('timeUtils', () => {
  describe('getInitialTimestamp', () => {
    it('should return Date if timestamp param is provided', () => {
      const searchParams = new URLSearchParams('timestamp=1700000000000');
      const result = getInitialTimestamp(searchParams);
      expect(result).toEqual(new Date(1700000000000));
    });

    it('should return null if no params are provided', () => {
      const searchParams = new URLSearchParams();
      expect(getInitialTimestamp(searchParams)).toBeNull();
    });
  });

  describe('getSearchedStartTime', () => {
    it('should return offset start time (15m before) if timestamp is provided', () => {
      const searchParams = new URLSearchParams('timestamp=1700000000000');
      const result = getSearchedStartTime(searchParams);
      expect(result).toEqual(new Date(1700000000000 - 15 * 60 * 1000));
    });

    it('should return null if timestamp is not provided', () => {
      const searchParams = new URLSearchParams();
      expect(getSearchedStartTime(searchParams)).toBeNull();
    });
  });

  describe('getSearchedEndTime', () => {
    it('should return offset end time (15m after) if timestamp is provided', () => {
      const searchParams = new URLSearchParams('timestamp=1700000000000');
      const result = getSearchedEndTime(searchParams);
      expect(result).toEqual(new Date(1700000000000 + 15 * 60 * 1000));
    });

    it('should return null if timestamp is not provided', () => {
      const searchParams = new URLSearchParams();
      expect(getSearchedEndTime(searchParams)).toBeNull();
    });
  });

  describe('getRelativeTimeString', () => {
    beforeEach(() => {
      vi.useFakeTimers();
      // Base date 2026-04-28T19:00:00Z
      vi.setSystemTime(new Date('2026-04-28T19:00:00Z'));
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('returns empty string for undefined input', () => {
      expect(getRelativeTimeString(undefined)).toBe('');
    });

    it('returns empty string for invalid date string', () => {
      expect(getRelativeTimeString('invalid date')).toBe('');
    });

    it('returns empty string for NaN inputs', () => {
      expect(getRelativeTimeString(NaN)).toBe('');
    });

    it('handles Date objects as inputs', () => {
      const date = new Date('2026-04-28T18:59:00Z');
      expect(getRelativeTimeString(date)).toBe('1 minute ago');
    });

    it('handles primitive millisecond numbers as inputs', () => {
      const timestamp = new Date('2026-04-28T18:55:00Z').getTime();
      expect(getRelativeTimeString(timestamp)).toBe('5 minutes ago');
    });

    it('correctly respects granular seconds checks with primitive inputs', () => {
      // 12 seconds diff -> <15 seconds
      const date = new Date('2026-04-28T18:59:48Z');
      expect(getRelativeTimeString(date, false)).toBe('<15 seconds ago');

      // 5 seconds diff -> <10 seconds
      const timestamp = new Date('2026-04-28T18:59:55Z').getTime();
      expect(getRelativeTimeString(timestamp, false)).toBe('<10 seconds ago');
    });

    it('returns "<1 minute ago" for events under 60 seconds', () => {
      expect(getRelativeTimeString('2026-04-28T18:59:45Z')).toBe(
        '<1 minute ago'
      );
      expect(getRelativeTimeString('2026-04-28T18:59:31Z')).toBe(
        '<1 minute ago'
      );
      expect(getRelativeTimeString('2026-04-28T18:59:30Z')).toBe(
        '<1 minute ago'
      );
      expect(getRelativeTimeString('2026-04-28T18:59:15Z')).toBe(
        '<1 minute ago'
      );
      expect(getRelativeTimeString('2026-04-28T18:59:01Z')).toBe(
        '<1 minute ago'
      );
    });

    it('returns formatted minutes for events under 60 minutes', () => {
      expect(getRelativeTimeString('2026-04-28T18:59:00Z')).toBe(
        '1 minute ago'
      );
      expect(getRelativeTimeString('2026-04-28T18:55:00Z')).toBe(
        '5 minutes ago'
      );
      expect(getRelativeTimeString('2026-04-28T18:01:00Z')).toBe(
        '59 minutes ago'
      );
    });

    it('returns formatted hours for events under 24 hours', () => {
      expect(getRelativeTimeString('2026-04-28T18:00:00Z')).toBe('1 hour ago');
      expect(getRelativeTimeString('2026-04-28T14:00:00Z')).toBe('5 hours ago');
      expect(getRelativeTimeString('2026-04-27T20:00:00Z')).toBe(
        '23 hours ago'
      );
    });

    it('returns formatted days for events 24 hours or older', () => {
      expect(getRelativeTimeString('2026-04-27T19:00:00Z')).toBe('1 day ago');
      expect(getRelativeTimeString('2026-04-26T19:00:00Z')).toBe('2 days ago');
      expect(getRelativeTimeString('2025-04-28T19:00:00Z')).toBe(
        '365 days ago'
      );
    });
  });
});
