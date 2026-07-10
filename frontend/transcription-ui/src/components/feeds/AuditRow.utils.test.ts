// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import type { FeedHistoryEvent } from '@transcription/common';

import {
  formatDiff,
  getDisplayStatus,
  groupEventsByDate,
} from './AuditRow.utils';

describe('AuditRow utils', () => {
  describe('getDisplayStatus', () => {
    it('returns undefined if values is undefined', () => {
      expect(getDisplayStatus(undefined)).toBeUndefined();
    });

    it('returns undefined if status is missing', () => {
      expect(getDisplayStatus({})).toBeUndefined();
    });

    it('returns the status directly if status is active and no failures/reasons exist', () => {
      expect(getDisplayStatus({ status: 'active' })).toBe('active');
      expect(getDisplayStatus({ status: 'active', failureCount: 0 })).toBe(
        'active'
      );
    });

    it('returns the status directly for non-active statuses even if they have failures', () => {
      expect(getDisplayStatus({ status: 'deactivated', failureCount: 5 })).toBe(
        'deactivated'
      );
    });

    it('returns failing if status is active and failureCount is greater than 0', () => {
      expect(getDisplayStatus({ status: 'active', failureCount: 2 })).toBe(
        'failing'
      );
    });

    it('returns failing if status is active and statusReason is set', () => {
      expect(
        getDisplayStatus({ status: 'active', statusReason: 'source_offline' })
      ).toBe('failing');
    });

    it('returns active if statusReason is null or undefined', () => {
      expect(getDisplayStatus({ status: 'active', statusReason: null })).toBe(
        'active'
      );
      expect(
        getDisplayStatus({ status: 'active', statusReason: undefined })
      ).toBe('active');
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
      expect(diff).toContain('name changed from "Alpha" to "Beta"');
    });

    it('formats additions correctly', () => {
      const diff = formatDiff({}, { name: 'Alpha' });
      expect(diff).toContain('name set to "Alpha"');
    });

    it('formats clearances correctly', () => {
      const diff = formatDiff({ name: 'Alpha' }, {});
      expect(diff).toContain('name cleared (was "Alpha")');
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

  describe('groupEventsByDate', () => {
    it('groups events occurring on the same date together', () => {
      const mockEvents: FeedHistoryEvent[] = [
        {
          id: '1',
          feedId: 'f1',
          action: 'feed.created',
          actor: 'admin',
          occurredAt: Date.parse('2026-06-26T12:00:00Z'),
          feedRevision: 1,
          beforeValues: {},
          afterValues: {},
        },
        {
          id: '2',
          feedId: 'f1',
          action: 'feed.updated',
          actor: 'admin',
          occurredAt: Date.parse('2026-06-26T14:30:00Z'),
          feedRevision: 2,
          beforeValues: {},
          afterValues: {},
        },
        {
          id: '3',
          feedId: 'f1',
          action: 'feed.reset',
          actor: 'admin',
          occurredAt: Date.parse('2026-06-27T09:00:00Z'),
          feedRevision: 3,
          beforeValues: {},
          afterValues: {},
        },
      ];

      const grouped = groupEventsByDate(mockEvents);

      expect(grouped).toHaveLength(2);
      expect(grouped[0].dateStr).toBe(
        new Date('2026-06-26T12:00:00Z').toLocaleDateString([], {
          weekday: 'long',
          month: 'long',
          day: 'numeric',
          year: 'numeric',
        })
      );
      expect(grouped[0].events).toHaveLength(2);
      expect(grouped[0].events[0].id).toBe('1');
      expect(grouped[0].events[1].id).toBe('2');

      expect(grouped[1].dateStr).toBe(
        new Date('2026-06-27T09:00:00Z').toLocaleDateString([], {
          weekday: 'long',
          month: 'long',
          day: 'numeric',
          year: 'numeric',
        })
      );
      expect(grouped[1].events).toHaveLength(1);
      expect(grouped[1].events[0].id).toBe('3');
    });

    it('returns empty array when given empty list', () => {
      expect(groupEventsByDate([])).toEqual([]);
    });
  });
});
