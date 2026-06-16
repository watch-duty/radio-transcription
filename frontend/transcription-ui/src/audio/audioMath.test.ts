import { describe, expect, it } from 'vitest';

import {
  VOLUME_MAX_DB,
  VOLUME_MIN_DB,
  dbToGain,
  formatVolumeDb,
  gainToDb,
} from './audioMath';

describe('audioMath', () => {
  describe('dbToGain', () => {
    it('maps 0 dB to unity gain', () => {
      expect(dbToGain(0)).toBeCloseTo(1);
    });

    it('maps +20 dB to 10x gain', () => {
      expect(dbToGain(VOLUME_MAX_DB)).toBeCloseTo(10);
    });

    it('maps -6 dB to ~0.5 gain', () => {
      expect(dbToGain(-6)).toBeCloseTo(0.501, 2);
    });

    it('snaps to 0 at and below the mute threshold', () => {
      expect(dbToGain(VOLUME_MIN_DB)).toBe(0);
      expect(dbToGain(VOLUME_MIN_DB + 0.5)).toBe(0);
    });
  });

  describe('gainToDb', () => {
    it('is the inverse of dbToGain for audible values', () => {
      expect(gainToDb(dbToGain(-6))).toBeCloseTo(-6);
      expect(gainToDb(1)).toBeCloseTo(0);
    });

    it('returns -Infinity for zero gain', () => {
      expect(gainToDb(0)).toBe(-Infinity);
    });
  });

  describe('formatVolumeDb', () => {
    it('formats unity as 0 dB', () => {
      expect(formatVolumeDb(0)).toBe('0 dB');
    });

    it('prefixes positive values with +', () => {
      expect(formatVolumeDb(12)).toBe('+12 dB');
    });

    it('formats negative values', () => {
      expect(formatVolumeDb(-12)).toBe('-12 dB');
    });

    it('shows Muted at the floor', () => {
      expect(formatVolumeDb(VOLUME_MIN_DB)).toBe('Muted');
    });
  });
});
