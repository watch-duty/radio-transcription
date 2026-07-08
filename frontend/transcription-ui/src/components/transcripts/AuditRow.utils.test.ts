// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import {
  formatDiff,
  getEffectiveStatus,
  getFieldLabel,
} from './AuditRow.utils';

describe('AuditRow utils', () => {
  describe('getFieldLabel', () => {
    it('returns custom display names for known fields', () => {
      expect(getFieldLabel('name')).toBe('Name');
      expect(getFieldLabel('status')).toBe('Status');
      expect(getFieldLabel('substatus')).toBe('Substatus');
      expect(getFieldLabel('sourceFeedId')).toBe('Source Feed ID');
      expect(getFieldLabel('sourceType')).toBe('Source Type');
      expect(getFieldLabel('statusReason')).toBe('Status Reason');
      expect(getFieldLabel('statusReasonDetail')).toBe('Status Reason Detail');
      expect(getFieldLabel('sourceUrl')).toBe('Source URL');
      expect(getFieldLabel('archiveUrl')).toBe('Archive URL');
    });

    it('falls back to capitalized spaced words for unknown fields', () => {
      expect(getFieldLabel('someCustomProperty')).toBe('Some Custom Property');
    });
  });

  describe('getEffectiveStatus', () => {
    it('returns undefined if values is undefined', () => {
      expect(getEffectiveStatus(undefined)).toBeUndefined();
    });

    it('returns undefined if status is missing or not a string', () => {
      expect(getEffectiveStatus({})).toBeUndefined();
      expect(getEffectiveStatus({ status: 123 })).toBeUndefined();
    });

    it('returns the status directly if status is active and no failures/reasons exist', () => {
      expect(getEffectiveStatus({ status: 'active' })).toBe('active');
      expect(getEffectiveStatus({ status: 'active', failureCount: 0 })).toBe(
        'active'
      );
    });

    it('returns the status directly for non-active statuses even if they have failures', () => {
      expect(
        getEffectiveStatus({ status: 'deactivated', failureCount: 5 })
      ).toBe('deactivated');
    });

    it('returns failing if status is active and failureCount is greater than 0', () => {
      expect(getEffectiveStatus({ status: 'active', failureCount: 2 })).toBe(
        'failing'
      );
    });

    it('returns failing if status is active and failure_count is greater than 0', () => {
      expect(getEffectiveStatus({ status: 'active', failure_count: 5 })).toBe(
        'failing'
      );
    });

    it('returns failing if status is active and statusReason is set', () => {
      expect(
        getEffectiveStatus({ status: 'active', statusReason: 'unreachable' })
      ).toBe('failing');
    });

    it('returns failing if status is active and status_reason is set', () => {
      expect(
        getEffectiveStatus({ status: 'active', status_reason: 'unreachable' })
      ).toBe('failing');
    });

    it('returns active if statusReason is an empty string', () => {
      expect(getEffectiveStatus({ status: 'active', statusReason: '' })).toBe(
        'active'
      );
    });
  });

  describe('formatDiff', () => {
    it('returns empty array if no differences exist', () => {
      expect(formatDiff({ name: 'Alpha' }, { name: 'Alpha' })).toEqual([]);
    });

    it('ignores status field changes', () => {
      expect(formatDiff({ status: 'active' }, { status: 'failing' })).toEqual(
        []
      );
    });

    it('formats string changes correctly', () => {
      const diff = formatDiff({ name: 'Alpha' }, { name: 'Beta' });
      expect(diff).toContain('Name changed from "Alpha" to "Beta"');
    });

    it('formats additions correctly', () => {
      const diff = formatDiff({}, { name: 'Alpha' });
      expect(diff).toContain('Name set to "Alpha"');
    });

    it('formats clearances correctly', () => {
      const diff = formatDiff({ name: 'Alpha' }, {});
      expect(diff).toContain('Name cleared (was "Alpha")');
    });

    it('formats tag changes (added and removed)', () => {
      const before = {
        tags: [
          { key: 'county', value: 'Marin' },
          { key: 'agency', value: 'Police' },
        ],
      };
      const after = {
        tags: [
          { key: 'county', value: 'Marin' },
          { key: 'agency', value: 'Fire' },
        ],
      };
      const diff = formatDiff(before, after);
      expect(diff).toContain(
        'Tags: added "agency=Fire" and removed "agency=Police"'
      );
    });
  });
});
