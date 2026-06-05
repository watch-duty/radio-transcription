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
      text: async () => JSON.stringify(mockData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const resp = await listFeeds('tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds'),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(resp).toEqual({ feeds: mockData });
  });

  it('should fetch feeds successfully when response is paginated ListFeedsResponse object', async () => {
    const mockFeeds = [
      { id: '1', name: 'Feed 1' },
      { id: '2', name: 'Feed 2' },
    ];
    const mockData = {
      feeds: mockFeeds,
      nextToken: 'token_abc',
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const resp = await listFeeds('tokenXYZ', {
      nextToken: 'token_abc',
      limit: 10,
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds?limit=10&nextToken=token_abc'),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(resp).toEqual(mockData);
  });

  it('should support other filter query parameters', async () => {
    const mockFeeds = [{ id: '1', name: 'Feed 1' }];
    const mockData = { feeds: mockFeeds };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const resp = await listFeeds('tokenXYZ', {
      name: 'Feed 1',
      statuses: ['Active'],
      sourceTypes: ['echo'],
      tags: [{ key: 'tagKey', value: 'tagVal' }],
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining(
        '/api/v1/feeds?name=Feed+1&sourceTypes=echo&statuses=active&tags=%7B%22key%22%3A%22tagKey%22%2C%22value%22%3A%22tagVal%22%7D'
      ),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(resp).toEqual(mockData);
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
});
