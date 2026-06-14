// @vitest-environment jsdom
import type { ReactNode } from 'react';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, renderHook, waitFor } from '@testing-library/react';
import { AudioClassification, type AudioSegment } from '@transcription/common';

import { listAudioSegments } from '../service/listAudioSegments';
import { useAudioTimelineSummary } from './useAudioTimelineSummary';

vi.mock('../service/listAudioSegments');
const mockListAudioSegments = vi.mocked(listAudioSegments);

const MIN = 60 * 1000;
const HOUR = 60 * MIN;
const LIVE = new Date('2026-04-20T12:00:00Z').getTime();

const seg = (id: string, startMs: number, endMs: number): AudioSegment => ({
  id,
  feedId: 'feed1',
  classification: AudioClassification.SPEECH,
  startTimestamp: new Date(startMs).toISOString(),
  endTimestamp: new Date(endMs).toISOString(),
  missingPriorContext: false,
  missingPostContext: false,
  sourceAudioUris: [],
  createdAt: new Date(startMs).toISOString(),
  annotations: [],
});

const createWrapper = () => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
};

const baseOptions = {
  token: 'tok',
  searchedFeedId: 'feed1',
  alertFilter: 'all' as const,
  isFeedsSuccess: true,
};

const render = () =>
  renderHook(() => useAudioTimelineSummary(baseOptions), {
    wrapper: createWrapper(),
  });

beforeEach(() => {
  mockListAudioSegments.mockReset();
});
afterEach(cleanup);

describe('useAudioTimelineSummary', () => {
  it('keeps paging until 24h back from the live edge is covered', async () => {
    mockListAudioSegments
      .mockResolvedValueOnce({
        segments: [seg('a', LIVE - 5 * MIN, LIVE)],
        nextToken: 't1',
      })
      .mockResolvedValueOnce({
        segments: [seg('b', LIVE - 13 * HOUR, LIVE - 12 * HOUR)],
        nextToken: 't2',
      })
      .mockResolvedValueOnce({
        segments: [seg('c', LIVE - 25 * HOUR, LIVE - 24 * HOUR)],
        nextToken: 't3',
      })
      .mockResolvedValueOnce({
        segments: [seg('d', LIVE - 30 * HOUR, LIVE - 29 * HOUR)],
        nextToken: undefined,
      });

    const { result } = render();

    await waitFor(() => expect(result.current.summarySegments).toHaveLength(3));
    // Stops once a page reaches past the 24h cutoff; never fetches the 4th page.
    expect(mockListAudioSegments).toHaveBeenCalledTimes(3);
    expect(result.current.summarySegments.map((s) => s.id)).toEqual([
      'a',
      'b',
      'c',
    ]);
  });

  it('stops when the server runs out of pages', async () => {
    mockListAudioSegments.mockResolvedValueOnce({
      segments: [seg('a', LIVE - 5 * MIN, LIVE)],
      nextToken: undefined,
    });

    const { result } = render();

    await waitFor(() => expect(result.current.summarySegments).toHaveLength(1));
    expect(mockListAudioSegments).toHaveBeenCalledTimes(1);
  });

  it('requests large desc pages and forwards the alerts filter', async () => {
    mockListAudioSegments.mockResolvedValue({
      segments: [seg('a', LIVE - 5 * MIN, LIVE)],
      nextToken: undefined,
    });

    const { result } = renderHook(
      () => useAudioTimelineSummary({ ...baseOptions, alertFilter: 'alerts' }),
      { wrapper: createWrapper() }
    );

    await waitFor(() => expect(result.current.summarySegments).toHaveLength(1));
    const [, , limit, , , , order, isAlert] =
      mockListAudioSegments.mock.calls[0];
    expect(limit).toBe(2000);
    expect(order).toBe('desc');
    expect(isAlert).toBe(true);
  });

  it('does not run until a feed and token are present', () => {
    renderHook(
      () => useAudioTimelineSummary({ ...baseOptions, searchedFeedId: null }),
      { wrapper: createWrapper() }
    );
    expect(mockListAudioSegments).not.toHaveBeenCalled();
  });
});
