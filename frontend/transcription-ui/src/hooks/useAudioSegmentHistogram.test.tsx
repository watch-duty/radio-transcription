// @vitest-environment jsdom
import type { ReactNode } from 'react';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, renderHook, waitFor } from '@testing-library/react';
import type { HistogramBucket } from '@transcription/common';

import { getAudioSegmentHistogram } from '../service/getAudioSegmentHistogram';
import { useAudioSegmentHistogram } from './useAudioSegmentHistogram';

vi.mock('../service/getAudioSegmentHistogram');
const mockGetHistogram = vi.mocked(getAudioSegmentHistogram);

const MIN = 60 * 1000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;
const LIVE = new Date('2026-04-20T12:00:00Z').getTime();

const bucket = (
  startMs: number,
  count: number,
  isAlert = false
): HistogramBucket => ({
  bucketStart: new Date(startMs).toISOString(),
  count,
  isAlert,
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
  anchorTimestamp: null,
};

beforeEach(() => {
  mockGetHistogram.mockReset();
  vi.spyOn(Date, 'now').mockReturnValue(LIVE);
});
afterEach(() => {
  vi.restoreAllMocks();
  cleanup();
});

describe('useAudioSegmentHistogram', () => {
  it('fetches the histogram and exposes buckets + bucket duration', async () => {
    mockGetHistogram.mockResolvedValueOnce({
      buckets: [bucket(LIVE - 10 * MIN, 3), bucket(LIVE - 5 * MIN, 1, true)],
    });

    const { result } = renderHook(() => useAudioSegmentHistogram(baseOptions), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.buckets).toHaveLength(2));
    expect(mockGetHistogram).toHaveBeenCalledTimes(1);
    // 24h / 288 buckets = 5 minutes.
    expect(result.current.bucketDurationMs).toBe(5 * MIN);
  });

  it('requests start = now − 24h with no end and 288 buckets when live', async () => {
    mockGetHistogram.mockResolvedValue({ buckets: [] });

    renderHook(
      () => useAudioSegmentHistogram({ ...baseOptions, alertFilter: 'alerts' }),
      { wrapper: createWrapper() }
    );

    await waitFor(() => expect(mockGetHistogram).toHaveBeenCalledTimes(1));
    const [, , startTime, endTime, buckets, isAlert] =
      mockGetHistogram.mock.calls[0];
    expect(startTime).toBe(LIVE - DAY);
    expect(endTime).toBeUndefined();
    expect(buckets).toBe(288);
    expect(isAlert).toBe(true);
  });

  it('anchors the window to anchorTimestamp when set', async () => {
    const anchor = new Date(LIVE - 10 * HOUR);
    mockGetHistogram.mockResolvedValue({ buckets: [] });

    renderHook(
      () =>
        useAudioSegmentHistogram({ ...baseOptions, anchorTimestamp: anchor }),
      { wrapper: createWrapper() }
    );

    await waitFor(() => expect(mockGetHistogram).toHaveBeenCalledTimes(1));
    const [, , startTime, endTime] = mockGetHistogram.mock.calls[0];
    expect(endTime).toBe(anchor.getTime());
    expect(startTime).toBe(anchor.getTime() - DAY);
  });

  it('does not run until a feed and token are present', () => {
    renderHook(
      () => useAudioSegmentHistogram({ ...baseOptions, searchedFeedId: null }),
      { wrapper: createWrapper() }
    );
    expect(mockGetHistogram).not.toHaveBeenCalled();
  });
});
