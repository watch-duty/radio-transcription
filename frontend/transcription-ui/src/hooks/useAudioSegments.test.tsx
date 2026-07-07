// @vitest-environment jsdom
import React from 'react';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { AudioClassification } from '@transcription/common';

import { listAudioSegments } from '../service/listAudioSegments';
import { useAudioSegments } from './useAudioSegments';

vi.mock('../service/listAudioSegments', () => ({
  listAudioSegments: vi.fn(),
}));

describe('useAudioSegments hook', () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });
    vi.resetAllMocks();
  });

  it('should subtract POLLING_LOOKBACK_BUFFER_MS from newestTimestamp when polling', async () => {
    const mockNewestTimestamp = '2026-06-18T12:00:00.000Z';

    // Initial fetch of segments on mount
    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: [
        {
          id: 'segment-1',
          feedId: 'feed123',
          classification: AudioClassification.SPEECH,
          startTimestamp: mockNewestTimestamp,
          endTimestamp: '2026-06-18T12:00:05.000Z',
          missingPriorContext: false,
          missingPostContext: false,
          sourceAudioUris: [],
          createdAt: mockNewestTimestamp,
          annotations: [],
        },
      ],
      nextToken: undefined,
    });

    const { result } = renderHook(
      () =>
        useAudioSegments({
          token: 'fake-token',
          searchedFeedId: 'feed-123',
          searchedTimestamp: null,
          alertFilter: 'all',
          isFeedsSuccess: true,
          pollingEnabled: true,
        }),
      { wrapper }
    );

    // Wait for the initial query to complete
    await waitFor(() => {
      expect(result.current.isAudioSegmentsSuccess).toBe(true);
    });

    // Mock response for the poll call
    vi.mocked(listAudioSegments).mockResolvedValueOnce({
      segments: [],
      nextToken: undefined,
    });

    // Manually trigger the polling query refetch via the queryClient.
    // This bypasses fragile fake timer mechanics while perfectly executing
    // the queryFn (pollNewerAudioSegments) to test its parameter formulation.
    await queryClient.refetchQueries({
      queryKey: ['liveAudioSegmentsPoll', 'feed-123', 'all'],
    });

    // Verify that the second call was made
    expect(listAudioSegments).toHaveBeenCalledTimes(2);

    const expectedStartTime = new Date(mockNewestTimestamp).getTime() - 90000; // 90 seconds lookback

    expect(listAudioSegments).toHaveBeenLastCalledWith(
      'feed-123',
      'fake-token',
      undefined,
      undefined,
      expectedStartTime,
      undefined,
      'asc',
      undefined,
      undefined
    );
  });
});
