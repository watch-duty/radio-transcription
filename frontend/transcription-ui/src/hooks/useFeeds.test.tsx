// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { type Feed, SourceType } from '@transcription/common';

import { listFeeds } from '../service/listFeeds';
import { useFeeds } from './useFeeds';

vi.mock('../service/listFeeds', () => ({
  listFeeds: vi.fn(),
}));

const createWrapper = () => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
};

describe('useFeeds', () => {
  const mockFeeds: Feed[] = [
    {
      id: 'feed-1',
      name: 'Marin Fire',
      sourceType: SourceType.BCFY_FEEDS,
      status: 'active',
      substatus: 'active',
    },
  ];

  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(listFeeds).mockResolvedValue({
      feeds: mockFeeds,
      total: mockFeeds.length,
    });
  });

  it('fetches feeds successfully using useInfiniteQuery', async () => {
    const { result } = renderHook(() => useFeeds({ token: 'fake-token' }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => {
      expect(result.current.feeds).toEqual(mockFeeds);
      expect(result.current.total).toBe(1);
    });

    expect(listFeeds).toHaveBeenCalledWith('fake-token', {
      limit: 50,
      nextToken: undefined,
      name: undefined,
      sourceTypes: undefined,
      statuses: undefined,
      tags: undefined,
    });
  });

  it('does not fetch when token is null', () => {
    const { result } = renderHook(() => useFeeds({ token: null }), {
      wrapper: createWrapper(),
    });

    expect(result.current.feeds).toEqual([]);
    expect(listFeeds).not.toHaveBeenCalled();
  });
});
