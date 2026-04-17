import { beforeEach, describe, expect, it, vi } from 'vitest';

import { listFeeds } from './listFeeds';

describe('listFeeds', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should fetch feeds successfully', async () => {
    const mockData = [
      { id: '1', name: 'Feed 1' },
      { id: '2', name: 'Feed 2' },
    ];

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => mockData,
    });

    const feeds = await listFeeds('tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds'),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(feeds).toEqual(mockData);
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
    });

    await expect(listFeeds('tokenXYZ')).rejects.toThrow(
      'Error: 500 Internal Server Error'
    );
  });
});
