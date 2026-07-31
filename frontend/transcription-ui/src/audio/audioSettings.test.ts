import { describe, expect, it } from 'vitest';

import { STORAGE_KEYS, feedKey } from './audioSettings';

describe('audioSettings persistence utilities', () => {
  describe('STORAGE_KEYS', () => {
    it('defines expected storage key mappings', () => {
      expect(STORAGE_KEYS).toEqual({
        themeMode: 'radio.themeMode',
        volumeDb: 'radio.audio.volumeDb',
        pan: 'radio.audio.pan',
        speed: 'radio.audio.speed',
      });
    });
  });

  describe('feedKey', () => {
    it('appends feedId to base key when feedId is provided', () => {
      expect(feedKey(STORAGE_KEYS.volumeDb, 'feed-123')).toBe(
        'radio.audio.volumeDb.feed-123'
      );
    });

    it('returns base key when feedId is omitted or empty', () => {
      expect(feedKey(STORAGE_KEYS.volumeDb)).toBe('radio.audio.volumeDb');
      expect(feedKey(STORAGE_KEYS.volumeDb, '')).toBe('radio.audio.volumeDb');
    });
  });
});
