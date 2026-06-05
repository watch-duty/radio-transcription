import { beforeEach, describe, expect, it, vi } from 'vitest';

import { deactivateFeed } from './deactivateFeed';

describe('deactivateFeed', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should deactivate feed successfully', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => '',
      headers: {
        get: () => null,
      },
    });

    await deactivateFeed('12', 'tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/feeds/12/deactivate'),
      expect.objectContaining({
        method: 'POST',
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
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

    await expect(deactivateFeed('12', 'tokenXYZ')).rejects.toThrow(
      /500.*Internal Server Error/
    );
  });
});
