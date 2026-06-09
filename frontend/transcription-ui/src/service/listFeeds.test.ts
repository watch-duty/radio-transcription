import { beforeEach, describe, expect, it, vi } from 'vitest';

import { listFeeds } from './listFeeds';

describe('listFeeds', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should fetch feeds successfully when response is a raw array', async () => {
    const mockData = [
      { id: '1', name: 'Feed 1' },
      { id: '2', name: 'Feed 2' },
    ];

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
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
    expect(feeds).toStrictEqual({ feeds: mockData, total: mockData.length });
  });

  it('should loop and fetch all pages when response is paginated ListFeedsResponse object', async () => {
    const page1Feeds = [{ id: '1', name: 'Feed 1' }];
    const page2Feeds = [{ id: '2', name: 'Feed 2' }];

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () =>
        JSON.stringify({
          feeds: page1Feeds,
          nextToken: 'token_abc',
          total: 2,
        }),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () =>
        JSON.stringify({
          feeds: page2Feeds,
          total: 2,
        }),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const feeds = await listFeeds('tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(2);
    expect(mockFetch).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining('/api/v1/feeds'),
      expect.any(Object)
    );
    expect(mockFetch).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining('/api/v1/feeds?limit=100&nextToken=token_abc'),
      expect.any(Object)
    );
    expect(feeds).toStrictEqual({
      feeds: [...page1Feeds, ...page2Feeds],
      total: 2,
    });
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
      headers: {
        get: () => null,
      },
      text: async () => 'Internal Server Error',
    });
    await expect(listFeeds('tokenXYZ')).rejects.toThrow(
      /500.*Internal Server Error/
    );
  });

  it('should serialize query parameters correctly including JSON-serialized tags array', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      text: async () => JSON.stringify({ feeds: [], total: 0 }),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    // Test Active status mapping
    await listFeeds('tokenXYZ', {
      name: 'Alpha',
      sourceTypes: ['bcfy_feeds', 'echo'],
      statuses: ['Active'],
      tags: [{ key: 'County', value: 'Marin' }],
    });

    const urlParams1 = new URLSearchParams(
      mockFetch.mock.calls[0][0].split('?')[1]
    );
    expect(urlParams1.get('name')).toBe('Alpha');
    expect(urlParams1.get('sourceTypes')).toBe('bcfy_feeds,echo');
    expect(urlParams1.get('statuses')).toBe('active');
    expect(urlParams1.get('tags')).toBe('[{"key":"County","value":"Marin"}]');

    // Test Inactive and Error status mapping
    await listFeeds('tokenXYZ', {
      statuses: ['Inactive', 'Error'],
    });

    const urlParams2 = new URLSearchParams(
      mockFetch.mock.calls[1][0].split('?')[1]
    );
    expect(urlParams2.get('statuses')).toBe(
      'unclaimed,deactivated,failing,quarantined'
    );
  });
});
