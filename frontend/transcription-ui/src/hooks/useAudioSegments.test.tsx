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

  it('is inert when preloadWindowMs is unset: default page size, no preload paging', async () => {
    // Seed a recent head with an older-page token available, so the ONLY reason
    // to page older would be the preload — which must not run when unset.
    const recent = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: [
          {
            id: 'segment-1',
            feedId: 'feed-123',
            classification: AudioClassification.SPEECH,
            startTimestamp: recent,
            endTimestamp: recent,
            missingPriorContext: false,
            missingPostContext: false,
            sourceAudioUris: [],
            createdAt: recent,
            annotations: [],
          },
        ],
        nextToken: 'older-token',
      })
      .mockResolvedValue({ segments: [], nextToken: undefined });

    renderHook(
      () =>
        useAudioSegments({
          token: 'fake-token',
          searchedFeedId: 'feed-123',
          searchedTimestamp: null,
          alertFilter: 'all',
          isFeedsSuccess: true,
          pollingEnabled: false,
          // preloadWindowMs intentionally unset
        }),
      { wrapper }
    );

    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledTimes(1);
    });
    // Server default page size (no explicit limit)...
    expect(vi.mocked(listAudioSegments).mock.calls[0][2]).toBeUndefined();
    // ...and no eager older paging despite the available older-page token.
    expect(listAudioSegments).toHaveBeenCalledTimes(1);
  });

  it('pages older with an explicit limit to preload the timeline window', async () => {
    // Newest-first seed only an hour back, so the 24h preload window is not yet
    // covered and the hook should eagerly page older.
    const recent = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    vi.mocked(listAudioSegments)
      .mockResolvedValueOnce({
        segments: [
          {
            id: 'segment-1',
            feedId: 'feed-123',
            classification: AudioClassification.SPEECH,
            startTimestamp: recent,
            endTimestamp: recent,
            missingPriorContext: false,
            missingPostContext: false,
            sourceAudioUris: [],
            createdAt: recent,
            annotations: [],
          },
        ],
        nextToken: 'older-token',
      })
      .mockResolvedValue({ segments: [], nextToken: undefined });

    renderHook(
      () =>
        useAudioSegments({
          token: 'fake-token',
          searchedFeedId: 'feed-123',
          searchedTimestamp: null,
          alertFilter: 'all',
          isFeedsSuccess: true,
          pollingEnabled: false,
          preloadWindowMs: 24 * 60 * 60 * 1000,
        }),
      { wrapper }
    );

    // The preload pages older (desc) from the older-page token with an explicit
    // page limit (larger than the server default of 100) to cover 24h in fewer
    // round-trips.
    await waitFor(() => {
      expect(listAudioSegments).toHaveBeenCalledWith(
        'feed-123',
        'fake-token',
        500,
        'older-token',
        undefined,
        undefined,
        'desc',
        undefined,
        undefined
      );
    });
  });

  it('preloads both directions of a centered date window', async () => {
    const anchor = new Date('2026-06-20T12:00:00.000Z');
    const at = (offsetMs: number) =>
      new Date(anchor.getTime() + offsetMs).toISOString();
    const makeSeg = (id: string, startIso: string) => ({
      id,
      feedId: 'feed-123',
      classification: AudioClassification.SPEECH,
      startTimestamp: startIso,
      endTimestamp: startIso,
      missingPriorContext: false,
      missingPostContext: false,
      sourceAudioUris: [],
      createdAt: startIso,
      annotations: [],
    });

    // Date mode: initial asc load sits at the anchor and exposes a newer-page
    // token; the older (desc) and newer (asc-token) fetches fill each side.
    vi.mocked(listAudioSegments).mockImplementation(
      async (_feedId, _token, _limit, nextToken, _start, _end, order) => {
        if (order === 'asc' && !nextToken) {
          return {
            segments: [makeSeg('mid', at(0))],
            nextToken: 'newer-token',
          };
        }
        if (order === 'asc' && nextToken === 'newer-token') {
          return {
            segments: [makeSeg('newer', at(60 * 60 * 1000))],
            nextToken: undefined,
          };
        }
        // desc = older side
        return {
          segments: [makeSeg('older', at(-60 * 60 * 1000))],
          nextToken: undefined,
        };
      }
    );

    renderHook(
      () =>
        useAudioSegments({
          token: 'fake-token',
          searchedFeedId: 'feed-123',
          searchedTimestamp: anchor,
          alertFilter: 'all',
          isFeedsSuccess: true,
          pollingEnabled: false,
          preloadWindowMs: 24 * 60 * 60 * 1000,
        }),
      { wrapper }
    );

    await waitFor(() => {
      // Older side (desc) bounded by the anchor (endTime) toward T - window/2.
      expect(listAudioSegments).toHaveBeenCalledWith(
        'feed-123',
        'fake-token',
        500,
        undefined,
        undefined,
        anchor.getTime(),
        'desc',
        undefined,
        undefined
      );
      // Newer side (asc from the newer-page token) toward T + window/2.
      expect(listAudioSegments).toHaveBeenCalledWith(
        'feed-123',
        'fake-token',
        500,
        'newer-token',
        undefined,
        undefined,
        'asc',
        undefined,
        undefined
      );
    });
  });
});
