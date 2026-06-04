import { beforeEach, describe, expect, it, vi } from 'vitest';

import { SourceType } from '@transcription/common';

import { listFeeds } from './listFeeds';

describe('listFeeds', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should fetch feeds successfully without params', async () => {
    const mockResponse = {
      feeds: [
        { id: '1', name: 'Feed 1' },
        { id: '2', name: 'Feed 2' },
      ],
      nextToken: 'next-123',
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockResponse),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const response = await listFeeds('tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds'),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(response).toEqual(mockResponse);
  });

  it('should pass query parameters to fetch call', async () => {
    const mockResponse = {
      feeds: [],
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockResponse),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    await listFeeds('tokenXYZ', {
      limit: 5,
      nextToken: 'token_abc',
      order: 'desc',
      sourceTypes: [SourceType.OPENMHZ, SourceType.ECHO],
      statuses: ['active', 'inactive'],
      tags: [{ key: 'county', value: 'Fulton' }],
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [calledUrl] = mockFetch.mock.calls[0];
    expect(calledUrl).toContain('/api/v1/feeds');
    expect(calledUrl).toContain('limit=5');
    expect(calledUrl).toContain('nextToken=token_abc');
    expect(calledUrl).toContain('order=desc');
    expect(calledUrl).toContain('sourceTypes=openmhz%2Cecho');
    expect(calledUrl).toContain('statuses=active%2Cinactive');
    expect(calledUrl).toContain(
      'tags=%7B%22key%22%3A%22county%22%2C%22value%22%3A%22Fulton%22%7D'
    );
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

    const response = await listFeeds('tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(response).toEqual(mockData);
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

    const feeds = await listFeeds('tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(feeds).toEqual(mockFeeds);
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
