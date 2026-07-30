import { describe, expect, it } from 'vitest';

import {
  AudioClassification,
  type AudioSegment,
  type BackendFeedStatusReason,
  type FeedHistoryEvent,
  SourceType,
} from '@transcription/common';

import {
  type TimeInterval,
  consolidateAudioSegments,
  deriveOfflineWindows,
  isOverlapWithOfflineWindows,
} from './useConsolidatedAudioSegments';

describe('useConsolidatedAudioSegments', () => {
  const createSegment = (
    id: string,
    start: string,
    end: string,
    classification: AudioClassification = AudioClassification.OTHER,
    missingPriorContext = false,
    missingPostContext = false
  ): AudioSegment => ({
    id,
    feedId: 'test-feed',
    classification,
    startTimestamp: start,
    endTimestamp: end,
    missingPriorContext,
    missingPostContext,
    sourceAudioUris: [],
    canonicalAudioUri: '',
    playbackAudioUri: '',
    startAudioOffset: '0',
    endAudioOffset: '0',
    createdAt: start,
    annotations: [],
  });

  const createHistoryEvent = (
    id: string,
    occurredAt: number,
    status?: 'active' | 'failing' | 'unclaimed' | 'deactivated',
    statusReason: BackendFeedStatusReason | null = null
  ): FeedHistoryEvent => ({
    id,
    feedId: 'test-feed',
    action: 'status_change',
    actor: 'system',
    occurredAt,
    feedRevision: 1,
    beforeValues: {},
    afterValues: {
      status,
      statusReason,
    },
  });

  describe('deriveOfflineWindows', () => {
    it('returns empty array when historyEvents is undefined or empty', () => {
      expect(deriveOfflineWindows(undefined)).toEqual([]);
      expect(deriveOfflineWindows([])).toEqual([]);
    });

    it('derives offline window when feed transitions active -> failing -> active', () => {
      const events: FeedHistoryEvent[] = [
        createHistoryEvent('e1', 1000, 'active'),
        createHistoryEvent('e2', 2000, 'failing', 'source_offline'),
        createHistoryEvent('e3', 5000, 'active'),
      ];

      const windows = deriveOfflineWindows(events);
      expect(windows).toEqual([{ startMs: 2000, endMs: 5000 }]);
    });

    it('derives ongoing offline window using parameterized nowMs timestamp', () => {
      const events: FeedHistoryEvent[] = [
        createHistoryEvent('e1', 1000, 'active'),
        createHistoryEvent('e2', 2000, 'failing', 'source_unreachable'),
      ];

      const windows = deriveOfflineWindows(events, 10000);
      expect(windows).toEqual([{ startMs: 2000, endMs: 10000 }]);
    });

    it('ignores non-status audit events while offline state is active', () => {
      const events: FeedHistoryEvent[] = [
        createHistoryEvent('e1', 1000, 'active'),
        createHistoryEvent('e2', 2000, 'failing', 'source_offline'),
        createHistoryEvent('e3', 3000, undefined, null), // e.g. tag edit
        createHistoryEvent('e4', 5000, 'active'),
      ];

      const windows = deriveOfflineWindows(events);
      expect(windows).toEqual([{ startMs: 2000, endMs: 5000 }]);
    });
  });

  describe('isOverlapWithOfflineWindows', () => {
    const windows: TimeInterval[] = [
      { startMs: 1000, endMs: 2000 },
      { startMs: 5000, endMs: 8000 },
    ];

    it('returns false for empty windows array', () => {
      expect(isOverlapWithOfflineWindows(100, 200, [])).toBe(false);
    });

    it('returns true when gap overlaps with a window', () => {
      expect(isOverlapWithOfflineWindows(1500, 1800, windows)).toBe(true);
      expect(isOverlapWithOfflineWindows(500, 1500, windows)).toBe(true);
      expect(isOverlapWithOfflineWindows(1800, 6000, windows)).toBe(true);
    });

    it('returns false when gap falls between or outside windows', () => {
      expect(isOverlapWithOfflineWindows(100, 900, windows)).toBe(false);
      expect(isOverlapWithOfflineWindows(2500, 4500, windows)).toBe(false);
      expect(isOverlapWithOfflineWindows(8500, 9500, windows)).toBe(false);
    });
  });

  describe('consolidateAudioSegments', () => {
    it('consolidates consecutive silence segments within the same calendar day', () => {
      const segments = [
        createSegment('s1', '2026-07-08T12:00:00Z', '2026-07-08T12:00:10Z'),
        createSegment('s2', '2026-07-08T12:00:10Z', '2026-07-08T12:00:20Z'),
      ];

      const result = consolidateAudioSegments(segments);

      expect(result).toHaveLength(1);
      expect(result[0].isSilenceBundle).toBe(true);
      expect(result[0].bundledSegmentIds).toEqual(['s1', 's2']);
      expect(result[0].startTimestamp).toBe('2026-07-08T12:00:00Z');
      expect(result[0].endTimestamp).toBe('2026-07-08T12:00:20Z');
    });

    it('injects outage bundle when a timestamp gap falls inside a feed offline window', () => {
      const start1 = '2026-07-08T12:00:00.000Z';
      const end1 = '2026-07-08T12:00:10.000Z';
      const start2 = '2026-07-08T12:00:20.000Z';
      const end2 = '2026-07-08T12:00:30.000Z';

      const segments = [
        createSegment('s1', start1, end1),
        createSegment('s2', start2, end2),
      ];

      const historyEvents: FeedHistoryEvent[] = [
        createHistoryEvent(
          'e1',
          new Date('2026-07-08T12:00:10.000Z').getTime(),
          'failing',
          'source_offline'
        ),
        createHistoryEvent(
          'e2',
          new Date('2026-07-08T12:00:20.000Z').getTime(),
          'active'
        ),
      ];

      const result = consolidateAudioSegments(
        segments,
        SourceType.BCFY_FEEDS,
        historyEvents
      );

      expect(result).toHaveLength(3); // s2 (top), outage, s1 (bottom)
      expect(result[0].id).toBe('s2');
      expect(result[1].isOutageBundle).toBe(true);
      expect(result[1].startTimestamp).toBe(end1);
      expect(result[1].endTimestamp).toBe(start2);
      expect(result[2].id).toBe('s1');
    });

    it('flushes silence bundle without injecting outage when minor gap (15s) occurs during active feed status', () => {
      const start1 = '2026-07-08T12:00:00.000Z';
      const end1 = '2026-07-08T12:00:10.000Z';
      const start2 = '2026-07-08T12:00:25.000Z';
      const end2 = '2026-07-08T12:00:35.000Z';

      const segments = [
        createSegment('s1', start1, end1),
        createSegment('s2', start2, end2),
      ];

      // Feed was continuously active
      const historyEvents: FeedHistoryEvent[] = [
        createHistoryEvent(
          'e1',
          new Date('2026-07-08T11:00:00.000Z').getTime(),
          'active'
        ),
      ];

      const result = consolidateAudioSegments(
        segments,
        SourceType.BCFY_FEEDS,
        historyEvents
      );

      expect(result).toHaveLength(2); // s2 and s1 as separate silence bundles
      expect(result[0].id).toBe('s2');
      expect(result[1].id).toBe('s1');
      expect(result[0].isSilenceBundle).toBe(true);
      expect(result[1].isSilenceBundle).toBe(true);
    });

    it('injects outage segment when missingPriorContext or missingPostContext is set', () => {
      const start1 = '2026-07-08T12:00:00.000Z';
      const end1 = '2026-07-08T12:00:10.000Z';
      const start2 = '2026-07-08T12:00:15.000Z';
      const end2 = '2026-07-08T12:00:20.000Z';

      const segments = [
        createSegment(
          's1',
          start1,
          end1,
          AudioClassification.SPEECH,
          false,
          true
        ),
        createSegment(
          's2',
          start2,
          end2,
          AudioClassification.SPEECH,
          true,
          false
        ),
      ];

      const result = consolidateAudioSegments(segments, SourceType.BCFY_FEEDS);

      expect(result).toHaveLength(3);
      expect(result[1].isOutageBundle).toBe(true);
    });

    it('does NOT inject outage bundle for non-continuous feeds (e.g. BCFY_CALLS) even if timestamp gap falls inside a feed offline window', () => {
      const start1 = '2026-07-08T12:00:00.000Z';
      const end1 = '2026-07-08T12:00:10.000Z';
      const start2 = '2026-07-08T12:15:00.000Z';
      const end2 = '2026-07-08T12:15:10.000Z';

      const segments = [
        createSegment('s1', start1, end1, AudioClassification.SPEECH),
        createSegment('s2', start2, end2, AudioClassification.SPEECH),
      ];

      const historyEvents: FeedHistoryEvent[] = [
        createHistoryEvent(
          'e1',
          new Date('2026-07-08T12:05:00.000Z').getTime(),
          'failing',
          'source_offline'
        ),
        createHistoryEvent(
          'e2',
          new Date('2026-07-08T12:06:00.000Z').getTime(),
          'active'
        ),
      ];

      const result = consolidateAudioSegments(
        segments,
        SourceType.BCFY_CALLS,
        historyEvents
      );

      expect(result).toHaveLength(2); // s2 (top), s1 (bottom) — no outage bundle
      expect(result[0].id).toBe('s2');
      expect(result[1].id).toBe('s1');
      expect(result.some((s) => s.isOutageBundle)).toBe(false);
    });

    it('handles undefined audioSource by defaulting to non-continuous feed behavior', () => {
      const start1 = '2026-07-08T12:00:00.000Z';
      const end1 = '2026-07-08T12:00:10.000Z';
      const start2 = '2026-07-08T12:10:00.000Z';
      const end2 = '2026-07-08T12:10:10.000Z';

      const segments = [
        createSegment('s1', start1, end1, AudioClassification.SPEECH),
        createSegment('s2', start2, end2, AudioClassification.SPEECH),
      ];

      const historyEvents: FeedHistoryEvent[] = [
        createHistoryEvent(
          'e1',
          new Date('2026-07-08T12:05:00.000Z').getTime(),
          'failing',
          'source_offline'
        ),
      ];

      const result = consolidateAudioSegments(
        segments,
        undefined,
        historyEvents
      );

      expect(result).toHaveLength(2);
      expect(result.some((s) => s.isOutageBundle)).toBe(false);
    });

    it('flushes silence bundle across time gaps for non-continuous feeds without creating outage bundles', () => {
      const start1 = '2026-07-08T12:00:00.000Z';
      const end1 = '2026-07-08T12:00:10.000Z';
      const start2 = '2026-07-08T12:00:10.000Z';
      const end2 = '2026-07-08T12:00:20.000Z';
      const start3 = '2026-07-08T12:15:00.000Z';
      const end3 = '2026-07-08T12:15:10.000Z';

      const segments = [
        createSegment('s1', start1, end1, AudioClassification.OTHER),
        createSegment('s2', start2, end2, AudioClassification.OTHER),
        createSegment('s3', start3, end3, AudioClassification.SPEECH),
      ];

      const historyEvents: FeedHistoryEvent[] = [
        createHistoryEvent(
          'e1',
          new Date('2026-07-08T12:05:00.000Z').getTime(),
          'failing',
          'source_offline'
        ),
      ];

      const result = consolidateAudioSegments(
        segments,
        SourceType.BCFY_CALLS,
        historyEvents
      );

      expect(result).toHaveLength(2); // s3 (speech) and s1+s2 silence bundle
      expect(result[0].id).toBe('s3');
      expect(result[1].isSilenceBundle).toBe(true);
      expect(result[1].bundledSegmentIds).toEqual(['s1', 's2']);
      expect(result.some((s) => s.isOutageBundle)).toBe(false);
    });
  });
});
