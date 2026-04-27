import { describe, expect, it } from 'vitest';

import {
  getInitialTimestamp,
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
});
