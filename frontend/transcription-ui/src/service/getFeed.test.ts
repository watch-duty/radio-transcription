import { beforeEach, describe, expect, it, vi } from 'vitest';

import { getFeed } from './getFeed';

describe('getFeed', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should fetch feed successfully', async () => {
    const mockData = {
      id: '1',
      name: 'Feed 1',
      status: 'active',
      lastHeartbeat: '2026-04-10T12:00:00Z',
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => mockData,
    });

    const feed = await getFeed('1', 'tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds/1'),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(feed).toEqual(mockData);
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      statusText: 'Not Found',
    });

    await expect(getFeed('1', 'tokenXYZ')).rejects.toThrow(
      'Error: 404 Not Found'
    );
  });
});
