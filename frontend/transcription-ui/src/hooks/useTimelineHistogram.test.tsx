// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import { renderHook } from '@testing-library/react';
import {
  AnnotationType,
  AudioClassification,
  type AudioSegment,
} from '@transcription/common';

import {
  HISTOGRAM_BUCKET_DURATION_MS,
  type HistogramMark,
  useTimelineHistogram,
} from './useTimelineHistogram';

// The live edge (newest segment end) drives the range; anchor it to a fixed
// time, then place bucketed segments at offsets from the range start.
const LIVE_EDGE_MS = new Date('2026-06-20T12:00:00.000Z').getTime();
const RANGE_START_MS = LIVE_EDGE_MS - 24 * 60 * 60 * 1000;

// A short clip whose end sets the live edge (the right edge of the overview).
const LIVE_EDGE_SEGMENT: AudioSegment = {
  id: 'live-edge',
  feedId: 'feed-1',
  classification: AudioClassification.OTHER,
  startTimestamp: new Date(LIVE_EDGE_MS - 1000).toISOString(),
  endTimestamp: new Date(LIVE_EDGE_MS).toISOString(),
  missingPriorContext: false,
  missingPostContext: false,
  sourceAudioUris: [],
  createdAt: new Date(LIVE_EDGE_MS).toISOString(),
  annotations: [],
};

function makeSegment(
  id: string,
  offsetMs: number,
  classification: AudioClassification,
  decisions: string[] = []
): AudioSegment {
  const startTimestamp = new Date(RANGE_START_MS + offsetMs).toISOString();
  return {
    id,
    feedId: 'feed-1',
    classification,
    startTimestamp,
    endTimestamp: startTimestamp,
    missingPriorContext: false,
    missingPostContext: false,
    sourceAudioUris: [],
    createdAt: startTimestamp,
    annotations:
      decisions.length > 0
        ? [
            {
              type: AnnotationType.EVALUATION,
              createdAt: startTimestamp,
              data: { decisions, errors: [] },
            },
          ]
        : [],
  };
}

function render(segments: AudioSegment[], anchorTimestamp: Date | null = null) {
  return renderHook(() => useTimelineHistogram({ segments, anchorTimestamp }));
}

const markAt = (marks: HistogramMark[], startMs: number) =>
  marks.find((m) => m.startMs === startMs);

describe('useTimelineHistogram', () => {
  it('buckets segments and aggregates speech/alert/count per bucket', () => {
    const { result } = render([
      LIVE_EDGE_SEGMENT,
      makeSegment('a', 0, AudioClassification.SPEECH),
      makeSegment('b', 1000, AudioClassification.OTHER),
      makeSegment(
        'c',
        HISTOGRAM_BUCKET_DURATION_MS,
        AudioClassification.OTHER,
        ['rule-1']
      ),
    ]);

    expect(result.current.rangeStartMs).toBe(RANGE_START_MS);
    expect(result.current.rangeEndMs).toBe(LIVE_EDGE_MS);

    const first = markAt(result.current.marks, RANGE_START_MS);
    expect(first?.count).toBe(2);
    expect(first?.hasSpeech).toBe(true);
    expect(first?.hasAlert).toBe(false);

    const second = markAt(
      result.current.marks,
      RANGE_START_MS + HISTOGRAM_BUCKET_DURATION_MS
    );
    expect(second?.count).toBe(1);
    expect(second?.hasSpeech).toBe(false);
    expect(second?.hasAlert).toBe(true);
  });

  it('drops segments whose start falls outside the 24h range', () => {
    const { result } = render([
      LIVE_EDGE_SEGMENT,
      makeSegment('before', -60_000, AudioClassification.SPEECH),
    ]);

    // Only the live-edge segment is in range; the pre-range one is dropped.
    expect(
      markAt(result.current.marks, RANGE_START_MS - 60_000)
    ).toBeUndefined();
    expect(result.current.marks.every((m) => m.startMs >= RANGE_START_MS)).toBe(
      true
    );
  });

  it('is empty until any segment loads', () => {
    const { result } = render([]);
    expect(result.current.marks).toEqual([]);
    expect(result.current.rangeStartMs).toBeNull();
    expect(result.current.rangeEndMs).toBeNull();
  });

  it('centers the window on the picked time in date-filter mode', () => {
    const HALF = 12 * 60 * 60 * 1000;
    const anchor = new Date(LIVE_EDGE_MS);
    // A segment at the anchor falls in the middle bucket of [T-12h, T+12h].
    const atAnchor: AudioSegment = {
      ...makeSegment('at-anchor', 0, AudioClassification.SPEECH),
      startTimestamp: anchor.toISOString(),
      endTimestamp: anchor.toISOString(),
    };

    const { result } = render([atAnchor], anchor);

    expect(result.current.rangeStartMs).toBe(LIVE_EDGE_MS - HALF);
    expect(result.current.rangeEndMs).toBe(LIVE_EDGE_MS + HALF);
    expect(result.current.marks.length).toBe(1);
    expect(result.current.marks[0].count).toBe(1);
    expect(result.current.marks[0].startMs).toBeGreaterThanOrEqual(
      LIVE_EDGE_MS - HALF
    );
  });
});
