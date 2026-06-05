import { beforeEach, describe, expect, it, vi } from 'vitest';

import { updateFeed } from './updateFeed';

describe('updateFeed', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should update feed successfully', async () => {
    const feedUpdate = {
      name: 'Updated Feed Name',
      tags: [{ key: 'county', value: 'Marin' }],
    };

    const mockResponseData = {
      id: '12',
      name: 'Updated Feed Name',
      sourceType: 'bcfy_feeds',
      sourceFeedId: '12345',
      status: 'active',
      tags: [{ key: 'county', value: 'Marin' }],
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockResponseData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const feed = await updateFeed('12', feedUpdate, 'tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds/12'),
      expect.objectContaining({
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          Authorization: 'Bearer tokenXYZ',
        },
        body: JSON.stringify(feedUpdate),
      })
    );
    expect(feed).toEqual(mockResponseData);
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      statusText: 'Not Found',
      headers: {
        get: () => null,
      },
      text: async () => 'Feed not found',
    });

    await expect(
      updateFeed(
        '12',
        {
          name: 'Updated Feed Name',
        },
        'tokenXYZ'
      )
    ).rejects.toThrow(/404.*Not Found/);
  });
});
