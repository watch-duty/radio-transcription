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
    it('should return endTimestamp - 1min if only endTimestamp is provided', () => {
      const searchParams = new URLSearchParams('endTimestamp=1700000060000');
      const result = getInitialTimestamp(searchParams);
      expect(result).toEqual(new Date(1700000000000));
    });

    it('should return midpoint Date if start and end timestamps are provided', () => {
      const searchParams = new URLSearchParams(
        'startTimestamp=1700000000000&endTimestamp=1700000060000'
      );
      const result = getInitialTimestamp(searchParams);
      expect(result).toEqual(new Date(1700000030000));
    });

    it('should return null if no params are provided', () => {
      const searchParams = new URLSearchParams();
      expect(getInitialTimestamp(searchParams)).toBeNull();
    });
  });

  describe('getInitialDuration', () => {
    it('should return calculated duration if start and end timestamps are provided', () => {
      const searchParams = new URLSearchParams(
        'startTimestamp=1700000000000&endTimestamp=1700000120000'
      );
      expect(getInitialDuration(searchParams)).toBe('1'); // 120000ms diff / 2 minutes = 1 min from start to center
    });

    it('should return empty string if no params are provided', () => {
      const searchParams = new URLSearchParams();
      expect(getInitialDuration(searchParams)).toBe('');
    });
  });

  describe('getSearchedStartTime', () => {
    it('should return startTimestamp if provided without timestamp', () => {
      const searchParams = new URLSearchParams('startTimestamp=1700000000000');
      const result = getSearchedStartTime(searchParams);
      expect(result).toEqual(new Date(1700000000000));
    });
  });

  describe('getSearchedEndTime', () => {
    it('should return endTimestamp if provided without timestamp', () => {
      const searchParams = new URLSearchParams('endTimestamp=1700000000000');
      const result = getSearchedEndTime(searchParams);
      expect(result).toEqual(new Date(1700000000000));
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
