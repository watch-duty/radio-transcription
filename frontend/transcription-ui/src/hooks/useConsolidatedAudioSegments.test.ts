import { describe, expect, it } from 'vitest';

import { AudioClassification, type AudioSegment } from '@transcription/common';

import { consolidateAudioSegments } from './useConsolidatedAudioSegments';

describe('consolidateAudioSegments', () => {
  const createSegment = (
    id: string,
    start: string,
    end: string,
    classification: AudioClassification = AudioClassification.OTHER
  ): AudioSegment => ({
    id,
    feedId: 'test-feed',
    classification,
    startTimestamp: start,
    endTimestamp: end,
    missingPriorContext: false,
    missingPostContext: false,
    sourceAudioUris: [],
    canonicalAudioUri: '',
    playbackAudioUri: '',
    startAudioOffset: '0',
    endAudioOffset: '0',
    createdAt: start,
    annotations: [],
  });

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

  it('consolidates consecutive silence segments even across local calendar day boundaries', () => {
    const day1Date = new Date(2026, 6, 8, 23, 59, 50).toISOString();
    const day1End = new Date(2026, 6, 8, 23, 59, 59).toISOString();
    const day2Date = new Date(2026, 6, 9, 0, 0, 5).toISOString();
    const day2End = new Date(2026, 6, 9, 0, 0, 15).toISOString();

    const segments = [
      createSegment('day1-seg', day1Date, day1End),
      createSegment('day2-seg', day2Date, day2End),
    ];

    const result = consolidateAudioSegments(segments);

    expect(result).toHaveLength(1);
    expect(result[0].isSilenceBundle).toBe(true);
    expect(result[0].bundledSegmentIds).toEqual(['day1-seg', 'day2-seg']);
    expect(result[0].startTimestamp).toBe(day1Date);
    expect(result[0].endTimestamp).toBe(day2End);
  });
});
