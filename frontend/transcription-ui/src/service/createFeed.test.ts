import { beforeEach, describe, expect, it, vi } from 'vitest';

import { SourceType } from '@transcription/common';

import { createFeed } from './createFeed';

describe('createFeed', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should create feed successfully', async () => {
    const feedCreate = {
      name: 'New Feed',
      sourceType: SourceType.BCFY_FEEDS,
      sourceFeedId: '12345',
      tags: [{ key: 'county', value: 'Marin' }],
    };

    const mockResponseData = {
      id: '99',
      name: 'New Feed',
      sourceType: SourceType.BCFY_FEEDS,
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

    const feed = await createFeed(feedCreate, 'tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds'),
      expect.objectContaining({
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: 'Bearer tokenXYZ',
        },
        body: JSON.stringify(feedCreate),
      })
    );
    expect(feed).toEqual(mockResponseData);
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
      headers: {
        get: () => null,
      },
      text: async () => 'Invalid payload',
    });

    await expect(
      createFeed(
        {
          name: 'New Feed',
          sourceType: SourceType.BCFY_FEEDS,
          sourceFeedId: '12345',
        },
        'tokenXYZ'
      )
    ).rejects.toThrow(/400.*Bad Request/);
  });
});
