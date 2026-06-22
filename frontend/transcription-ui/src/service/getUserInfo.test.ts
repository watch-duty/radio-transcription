import { beforeEach, describe, expect, it, vi } from 'vitest';

import { getUserInfo } from './getUserInfo';

describe('getUserInfo', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should fetch user info successfully', async () => {
    const mockData = {
      email: 'test@email.org',
      isAdmin: true,
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => JSON.stringify(mockData),
      headers: {
        get: (key: string) =>
          key === 'content-type' ? 'application/json' : null,
      },
    });

    const userInfo = await getUserInfo('tokenXYZ');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/users'),
      expect.objectContaining({
        headers: {
          Authorization: 'Bearer tokenXYZ',
        },
      })
    );
    expect(userInfo).toEqual(mockData);
  });

  it('should throw error if response not ok', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 401,
      statusText: 'Unauthorized',
      headers: {
        get: () => null,
      },
      text: async () => 'Unauthorized',
    });

    await expect(getUserInfo('tokenXYZ')).rejects.toThrow(/401.*Unauthorized/);
  });
});
