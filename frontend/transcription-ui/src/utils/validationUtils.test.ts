import { describe, expect, it } from 'vitest';

import { SourceType } from '@transcription/common';

import { validateFeedSourceId } from './validationUtils';

describe('validationUtils', () => {
  describe('validateFeedSourceId', () => {
    it('returns error when sourceId is empty or whitespace only', () => {
      expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '')).toBe(
        'Source feed ID is required.'
      );
      expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '   ')).toBe(
        'Source feed ID is required.'
      );
    });

    describe('BCFY_CALLS validation', () => {
      it('returns null for valid format (numbers separated by a dash)', () => {
        expect(
          validateFeedSourceId(SourceType.BCFY_CALLS, '123-456')
        ).toBeNull();
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, '0-0')).toBeNull();
      });

      it('returns error for invalid formats', () => {
        const expectedError =
          'Must only contain numbers with a dash in the middle.';
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, '123')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, '123-')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, '-456')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, 'abc-def')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_CALLS, '123a-456')).toBe(
          expectedError
        );
      });
    });

    describe('BCFY_FEEDS validation', () => {
      it('returns null for valid format (only numbers)', () => {
        expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '12345')).toBeNull();
        expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '0')).toBeNull();
      });

      it('returns error for invalid formats', () => {
        const expectedError = 'Must be a number.';
        expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '123a')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_FEEDS, '123-456')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.BCFY_FEEDS, 'abc')).toBe(
          expectedError
        );
      });
    });

    describe('ECHO validation', () => {
      it('returns null for valid format (letters, numbers, dashes, underscores)', () => {
        expect(
          validateFeedSourceId(SourceType.ECHO, 'feed-123_abc')
        ).toBeNull();
        expect(validateFeedSourceId(SourceType.ECHO, 'ECHO_FEED')).toBeNull();
        expect(validateFeedSourceId(SourceType.ECHO, 'simple')).toBeNull();
      });

      it('returns error for invalid formats', () => {
        const expectedError =
          'Must only contain letters, numbers, and the following special characters: - _';
        expect(validateFeedSourceId(SourceType.ECHO, 'feed.123')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.ECHO, 'feed/123')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.ECHO, 'feed@123')).toBe(
          expectedError
        );
      });
    });

    describe('FIRE_NOTIFICATIONS validation', () => {
      it('returns null for valid format (uppercase letters, numbers, slash, dash, parentheses, underscore)', () => {
        expect(
          validateFeedSourceId(
            SourceType.FIRE_NOTIFICATIONS,
            'FIRE/DEPT-1(A)_B'
          )
        ).toBeNull();
        expect(
          validateFeedSourceId(SourceType.FIRE_NOTIFICATIONS, 'FDNY')
        ).toBeNull();
        expect(
          validateFeedSourceId(SourceType.FIRE_NOTIFICATIONS, '123')
        ).toBeNull();
      });

      it('returns error for invalid formats (e.g. lowercase letters or unsupported characters)', () => {
        const expectedError =
          'Must only contain uppercase letters, numbers, and the following special characters: / - ( )';
        expect(
          validateFeedSourceId(SourceType.FIRE_NOTIFICATIONS, 'fire')
        ).toBe(expectedError);
        expect(
          validateFeedSourceId(SourceType.FIRE_NOTIFICATIONS, 'FIRE.DEPT')
        ).toBe(expectedError);
        expect(
          validateFeedSourceId(SourceType.FIRE_NOTIFICATIONS, 'FIRE@DEPT')
        ).toBe(expectedError);
      });
    });

    describe('OPENMHZ validation', () => {
      it('returns null for valid format (alphanumeric, including underscores)', () => {
        expect(
          validateFeedSourceId(SourceType.OPENMHZ, 'openmhz123')
        ).toBeNull();
        expect(
          validateFeedSourceId(SourceType.OPENMHZ, 'open_mhz_456')
        ).toBeNull();
      });

      it('returns error for invalid formats', () => {
        const expectedError = 'Must be alphanumeric.';
        expect(validateFeedSourceId(SourceType.OPENMHZ, 'open-mhz')).toBe(
          expectedError
        );
        expect(validateFeedSourceId(SourceType.OPENMHZ, 'open.mhz')).toBe(
          expectedError
        );
      });
    });
  });
});
