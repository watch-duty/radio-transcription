import { AudioClassification, type AudioSegment } from '@transcription/common';

import { type RenderableAudioSegment } from '../hooks/useConsolidatedAudioSegments';

/**
 * Creates a mock AudioSegment with sensible defaults.
 * Useful for unit tests and demos to avoid verbose boilerplate.
 */
export function createMockAudioSegment(
  overrides: Partial<AudioSegment> = {}
): AudioSegment {
  return {
    id: 'mock-segment-id',
    feedId: 'feed-1',
    classification: AudioClassification.SPEECH,
    startTimestamp: '2026-06-29T20:00:00Z',
    endTimestamp: '2026-06-29T20:00:05Z',
    playbackAudioUri: '/test_middlebury.mp3',
    canonicalAudioUri: '/test_middlebury.mp3',
    missingPriorContext: false,
    missingPostContext: false,
    sourceAudioUris: [],
    startAudioOffset: '0',
    endAudioOffset: '0',
    createdAt: '2026-06-29T20:00:00Z',
    annotations: [],
    ...overrides,
  };
}

/**
 * Creates a mock RenderableAudioSegment (used in the frontend for consolidated views)
 * with sensible defaults.
 */
export function createMockRenderableAudioSegment(
  overrides: Partial<RenderableAudioSegment> = {}
): RenderableAudioSegment {
  const base = createMockAudioSegment(overrides);
  return {
    ...base,
    isOutageBundle: overrides.isOutageBundle,
    isNonSpeechBundle: overrides.isNonSpeechBundle,
    bundledSegmentIds: overrides.bundledSegmentIds,
  };
}
