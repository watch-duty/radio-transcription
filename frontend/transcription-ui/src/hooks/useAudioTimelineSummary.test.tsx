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
const DAY = 24 * HOUR;
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
  vi.spyOn(Date, 'now').mockReturnValue(LIVE);
});
afterEach(() => {
  vi.restoreAllMocks();
  cleanup();
});

describe('useAudioTimelineSummary', () => {
  it('fetches the whole 24h window in a single request', async () => {
    mockListAudioSegments.mockResolvedValueOnce({
      segments: [
        seg('a', LIVE - 5 * MIN, LIVE),
        seg('b', LIVE - 13 * HOUR, LIVE - 12 * HOUR),
      ],
      nextToken: undefined,
    });

    const { result } = render();

    await waitFor(() => expect(result.current.summarySegments).toHaveLength(2));
    expect(mockListAudioSegments).toHaveBeenCalledTimes(1);
    expect(result.current.summarySegments.map((s) => s.id)).toEqual(['a', 'b']);
  });

  it('requests start = now − 24h with a high row limit and desc order', async () => {
    mockListAudioSegments.mockResolvedValue({
      segments: [seg('a', LIVE - 5 * MIN, LIVE)],
      nextToken: undefined,
    });

    const { result } = renderHook(
      () => useAudioTimelineSummary({ ...baseOptions, alertFilter: 'alerts' }),
      { wrapper: createWrapper() }
    );

    await waitFor(() => expect(result.current.summarySegments).toHaveLength(1));
    const [, , limit, nextToken, startTime, endTime, order, isAlert] =
      mockListAudioSegments.mock.calls[0];
    expect(limit).toBeGreaterThanOrEqual(100000);
    expect(nextToken).toBeUndefined();
    expect(startTime).toBe(LIVE - DAY);
    expect(endTime).toBeUndefined();
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
