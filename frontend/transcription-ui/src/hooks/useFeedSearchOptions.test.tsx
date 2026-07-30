// @vitest-environment jsdom
import type { ReactNode } from 'react';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { FEED_STATUS_BUCKETS, SourceType } from '@transcription/common';

import { getFeedSearchOptions } from '../service/getFeedSearchOptions';
import { useFeedSearchOptions } from './useFeedSearchOptions';

vi.mock('../service/getFeedSearchOptions', () => ({
  getFeedSearchOptions: vi.fn(),
}));

describe('useFeedSearchOptions', () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    vi.clearAllMocks();
  });

  it('fetches search options when token is present', async () => {
    const mockData = {
      sourceTypes: [SourceType.BCFY_FEEDS],
      statuses: ['active'],
      tags: [{ key: 'county', value: 'Marin' }],
    };
    vi.mocked(getFeedSearchOptions).mockResolvedValueOnce(mockData);

    const { result } = renderHook(() => useFeedSearchOptions('test-token'), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(mockData);
    expect(getFeedSearchOptions).toHaveBeenCalledWith('test-token');
  });

  it('normalizes raw backend statuses into deduplicated presentation buckets', async () => {
    vi.mocked(getFeedSearchOptions).mockResolvedValueOnce({
      sourceTypes: [SourceType.BCFY_FEEDS],
      statuses: Object.keys(FEED_STATUS_BUCKETS),
      tags: [],
    });

    const { result } = renderHook(() => useFeedSearchOptions('test-token'), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.statuses).toEqual([
      'active',
      'inactive',
      'error',
    ]);
  });

  it('does not run query when token is null', () => {
    const { result } = renderHook(() => useFeedSearchOptions(null), {
      wrapper,
    });

    expect(result.current.fetchStatus).toBe('idle');
    expect(getFeedSearchOptions).not.toHaveBeenCalled();
  });
});
