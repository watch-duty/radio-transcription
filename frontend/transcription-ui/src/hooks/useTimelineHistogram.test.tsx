// @vitest-environment jsdom
import React from 'react';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import {
  AnnotationType,
  AudioClassification,
  type AudioSegment,
} from '@transcription/common';

import { listAudioSegments } from '../service/listAudioSegments';
import {
  HISTOGRAM_BUCKET_DURATION_MS,
  useTimelineHistogram,
} from './useTimelineHistogram';

vi.mock('../service/listAudioSegments', () => ({
  listAudioSegments: vi.fn(),
}));

const ANCHOR = new Date('2026-06-20T12:00:00.000Z');
const RANGE_START_MS = ANCHOR.getTime() - 24 * 60 * 60 * 1000;

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

describe('useTimelineHistogram hook', () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    vi.resetAllMocks();
  });

  function render(anchorTimestamp: Date | null = ANCHOR) {
    return renderHook(
      () =>
        useTimelineHistogram({
          token: 'fake-token',
          searchedFeedId: 'feed-1',
          isFeedsSuccess: true,
          anchorTimestamp,
        }),
      { wrapper }
    );
  }

  it('buckets segments and aggregates speech/alert/count per bucket', async () => {
    // Two segments in the first bucket, one alert+other in a later bucket.
    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: [
        makeSegment('a', 0, AudioClassification.SPEECH),
        makeSegment('b', 1000, AudioClassification.OTHER),
        makeSegment(
          'c',
          HISTOGRAM_BUCKET_DURATION_MS,
          AudioClassification.OTHER,
          ['rule-1']
        ),
      ],
      nextToken: undefined,
    });

    const { result } = render();

    await waitFor(() => expect(result.current.marks.length).toBe(2));

    const [first, second] = result.current.marks;
    expect(first.startMs).toBe(RANGE_START_MS);
    expect(first.count).toBe(2);
    expect(first.hasSpeech).toBe(true);
    expect(first.hasAlert).toBe(false);

    expect(second.startMs).toBe(RANGE_START_MS + HISTOGRAM_BUCKET_DURATION_MS);
    expect(second.count).toBe(1);
    expect(second.hasSpeech).toBe(false);
    expect(second.hasAlert).toBe(true);
  });

  it('paginates across nextToken until exhausted', async () => {
    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: [makeSegment('a', 0, AudioClassification.SPEECH)],
        nextToken: 'page-2',
      })
      .mockResolvedValueOnce({
        segments: [makeSegment('b', 1000, AudioClassification.SPEECH)],
        nextToken: undefined,
      });

    const { result } = render();

    await waitFor(() => expect(result.current.marks.length).toBe(1));
    expect(result.current.marks[0].count).toBe(2);
    expect(listAudioSegments).toHaveBeenCalledTimes(2);
  });

  it('drops segments whose start falls outside the 24h range', async () => {
    // The server filters on end_timestamp, so a segment starting before the
    // window can come back; it must not produce an out-of-range mark.
    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: [
        makeSegment('before', -60_000, AudioClassification.SPEECH),
        makeSegment('in-range', 1000, AudioClassification.SPEECH),
      ],
      nextToken: undefined,
    });

    const { result } = render();

    await waitFor(() => expect(result.current.marks.length).toBe(1));
    expect(result.current.marks[0].startMs).toBe(RANGE_START_MS);
    expect(result.current.marks[0].count).toBe(1);
  });

  it('stops paginating at the page cap and warns', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    // Always returns another token, so only the MAX_PAGES cap can end the loop.
    vi.mocked(listAudioSegments).mockResolvedValue({
      segments: [makeSegment('x', 0, AudioClassification.SPEECH)],
      nextToken: 'more',
    });

    const { result } = render();

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(listAudioSegments).toHaveBeenCalledTimes(25); // MAX_PAGES
    expect(warnSpy).toHaveBeenCalledTimes(1);
    warnSpy.mockRestore();
  });

  it('queries the anchor-relative 24h range newest-first', async () => {
    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: [],
      nextToken: undefined,
    });

    const { result } = render();
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(listAudioSegments).toHaveBeenCalledWith(
      'feed-1',
      'fake-token',
      expect.any(Number),
      undefined,
      RANGE_START_MS,
      ANCHOR.getTime(),
      'desc'
    );
    expect(result.current.rangeStartMs).toBe(RANGE_START_MS);
    expect(result.current.rangeEndMs).toBe(ANCHOR.getTime());
  });

  it('does not fetch when the token is missing', async () => {
    const { result } = renderHook(
      () =>
        useTimelineHistogram({
          token: null,
          searchedFeedId: 'feed-1',
          isFeedsSuccess: true,
          anchorTimestamp: ANCHOR,
        }),
      { wrapper }
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(listAudioSegments).not.toHaveBeenCalled();
    expect(result.current.marks).toEqual([]);
  });
});
