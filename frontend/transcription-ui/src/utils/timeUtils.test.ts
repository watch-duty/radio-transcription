import { describe, expect, it } from 'vitest';

import {
  calculateSearchTimes,
  getInitialDuration,
  getInitialTimestamp,
  getSearchedEndTime,
  getSearchedStartTime,
  validateDuration,
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

  describe('getInitialDuration', () => {
    it('should return duration param if provided', () => {
      const searchParams = new URLSearchParams('duration=15');
      expect(getInitialDuration(searchParams)).toBe('15');
    });

    it('should return null if no params are provided', () => {
      const searchParams = new URLSearchParams();
      expect(getInitialDuration(searchParams)).toBeNull();
    });
  });

  describe('getSearchedStartTime', () => {
    it('should return offset start time if timestamp and duration are provided', () => {
      const searchParams = new URLSearchParams(
        'timestamp=1700000000000&duration=5'
      );
      const result = getSearchedStartTime(searchParams);
      expect(result).toEqual(new Date(1700000000000 - 5 * 60000));
    });

    it('should return null if only timestamp is provided', () => {
      const searchParams = new URLSearchParams('timestamp=1700000000000');
      expect(getSearchedStartTime(searchParams)).toBeNull();
    });
  });

  describe('getSearchedEndTime', () => {
    it('should return offset end time with 1min padding if timestamp and duration are provided', () => {
      const searchParams = new URLSearchParams(
        'timestamp=1700000000000&duration=5'
      );
      const result = getSearchedEndTime(searchParams);
      expect(result).toEqual(new Date(1700000000000 + 5 * 60000 + 60000));
    });

    it('should return timestamp plus 1min padding if duration is not provided', () => {
      const searchParams = new URLSearchParams('timestamp=1700000000000');
      const result = getSearchedEndTime(searchParams);
      expect(result).toEqual(new Date(1700000000000 + 60000));
    });
  });

  describe('calculateSearchTimes', () => {
    it('should correctly center search times around timestamp with duration', () => {
      const now = new Date(1700000000000);
      const { startTime, endTime } = calculateSearchTimes(now, '5');
      expect(startTime).toEqual(new Date(1700000000000 - 5 * 60000));
      expect(endTime).toEqual(new Date(1700000000000 + 5 * 60000 + 60000));
    });

    it('should leave startTime null and pad endTime if duration is missing', () => {
      const now = new Date(1700000000000);
      const { startTime, endTime } = calculateSearchTimes(now, '');
      expect(startTime).toBeNull();
      expect(endTime).toEqual(new Date(1700000000000 + 60000));
    });

    it('should return nulls if timestamp is null', () => {
      const { startTime, endTime } = calculateSearchTimes(null, '5');
      expect(startTime).toBeNull();
      expect(endTime).toBeNull();
    });
  });

  describe('validateDuration', () => {
    it('should return true for valid cases', () => {
      expect(validateDuration('')).toBe(true);
      expect(validateDuration('0')).toBe(true);
      expect(validateDuration('15')).toBe(true);
    });

    it('should return false for invalid cases', () => {
      expect(validateDuration('-5')).toBe(false);
      expect(validateDuration('abc')).toBe(false);
    });
  });
});
