import { beforeEach, describe, expect, it, vi } from 'vitest';

import { resetFeed } from './resetFeed';

describe('resetFeed', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should reset feed successfully', async () => {
    const mockResponseData = {
      id: '12',
      name: 'Sonoma Sheriff dispatch',
      sourceType: 'openmhz',
      sourceFeedId: 'sonoma-county',
      externalId: 'ca-snm-sheriff',
      status: 'inactive',
      substatus: 'unclaimed',
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockResponseData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const feed = await resetFeed('12', 'tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds/12/reset'),
      expect.objectContaining({
        method: 'POST',
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(feed).toEqual(mockResponseData);
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

    await expect(resetFeed('12', 'tokenXYZ')).rejects.toThrow(
      /500.*Internal Server Error/
    );
  });
});
