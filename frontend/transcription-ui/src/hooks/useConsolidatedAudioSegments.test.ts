import { describe, expect, it } from 'vitest';

import {
  AudioClassification,
  type AudioSegment,
  type BackendFeedStatusReason,
  type FeedHistoryEvent,
} from '@transcription/common';

import {
  consolidateAudioSegments,
  deriveOfflineWindows,
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
    status: 'active' | 'failing' | 'unclaimed' | 'deactivated',
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

    it('derives ongoing offline window when feed remains failing', () => {
      const events: FeedHistoryEvent[] = [
        createHistoryEvent('e1', 1000, 'active'),
        createHistoryEvent('e2', 2000, 'failing', 'source_unreachable'),
      ];

      const windows = deriveOfflineWindows(events);
      expect(windows).toHaveLength(1);
      expect(windows[0].startMs).toBe(2000);
      expect(windows[0].endMs).toBeGreaterThanOrEqual(2000);
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
      const start2 = '2026-07-08T12:30:00.000Z';
      const end2 = '2026-07-08T12:30:10.000Z';

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
          new Date('2026-07-08T12:30:00.000Z').getTime(),
          'active'
        ),
      ];

      const result = consolidateAudioSegments(segments, true, historyEvents);

      expect(result).toHaveLength(3); // s2 (top), outage, s1 (bottom)
      expect(result[0].id).toBe('s2');
      expect(result[1].isOutageBundle).toBe(true);
      expect(result[1].startTimestamp).toBe(end1);
      expect(result[1].endTimestamp).toBe(start2);
      expect(result[2].id).toBe('s1');
    });

    it('flushes silence bundle without injecting outage when gap occurs during active feed status', () => {
      const start1 = '2026-07-08T12:00:00.000Z';
      const end1 = '2026-07-08T12:00:10.000Z';
      const start2 = '2026-07-08T12:05:00.000Z';
      const end2 = '2026-07-08T12:05:10.000Z';

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

      const result = consolidateAudioSegments(segments, true, historyEvents);

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

      const result = consolidateAudioSegments(segments, true);

      expect(result).toHaveLength(3);
      expect(result[1].isOutageBundle).toBe(true);
    });
  });
});
