import { beforeEach, describe, expect, it, vi } from 'vitest';

import { authLogout } from './authLogout';

describe('authLogout', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should make post logout API request successfully', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
    });

    await authLogout();

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/auth/logout'),
      expect.objectContaining({
        method: 'POST',
        credentials: 'include',
      })
    );
  });

  it('should trigger error response if logout endpoint crashes', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
    });

    await expect(authLogout()).rejects.toThrow('Logout failed');
  });
});
