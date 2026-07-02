import { describe, expect, it } from 'vitest';

import { AudioClassification, type AudioSegment } from '@transcription/common';

import { consolidateAudioSegments } from '../hooks/useConsolidatedAudioSegments';
import { getNextContinuousSegment } from './playbackUtils';

function segment(
  id: string,
  classification: AudioClassification,
  startSeconds: number
): AudioSegment {
  const start = new Date(Date.UTC(2026, 3, 10, 12, 0, startSeconds));
  const end = new Date(Date.UTC(2026, 3, 10, 12, 0, startSeconds + 5));
  return {
    id,
    feedId: 'feed-123',
    classification,
    startTimestamp: start.toISOString(),
    endTimestamp: end.toISOString(),
    createdAt: start.toISOString(),
    annotations: [],
    missingPriorContext: false,
    missingPostContext: false,
    sourceAudioUris: [],
    playbackAudioUri: `gs://bucket/${id}.m4a`,
  };
}

describe('getNextContinuousSegment', () => {
  it('advances to the next clip within a non-speech bundle', () => {
    const raw = [
      segment('non-1', AudioClassification.OTHER, 0),
      segment('non-2', AudioClassification.OTHER, 5),
    ];
    const consolidated = consolidateAudioSegments(raw);

    const next = getNextContinuousSegment(consolidated, raw, 'non-1');

    expect(next).toMatchObject({ id: 'non-2' });
  });

  it('advances into a non-speech bundle at its first clip', () => {
    const raw = [
      segment('speech-1', AudioClassification.SPEECH, 0),
      segment('non-1', AudioClassification.OTHER, 5),
    ];
    const consolidated = consolidateAudioSegments(raw);

    const next = getNextContinuousSegment(consolidated, raw, 'speech-1');

    expect(next).toMatchObject({ id: 'non-1' });
  });

  it('advances to the next speech segment', () => {
    const raw = [
      segment('non-1', AudioClassification.OTHER, 0),
      segment('speech-1', AudioClassification.SPEECH, 5),
    ];
    const consolidated = consolidateAudioSegments(raw);

    const next = getNextContinuousSegment(consolidated, raw, 'non-1');

    expect(next).toMatchObject({ id: 'speech-1' });
  });
});
