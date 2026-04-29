import { beforeEach, describe, expect, it, vi } from 'vitest';

import { authSession } from './authSession';

describe('authSession', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
  });

  it('should successfully read session idToken', async () => {
    const responsePayload = {
      idToken: 'restored-session-id-token',
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => responsePayload,
    });

    const token = await authSession();

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/auth/session'),
      expect.objectContaining({
        credentials: 'include',
      })
    );
    expect(token).toEqual('restored-session-id-token');
  });

  it('should error if backend returns not ok status code', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
    });

    await expect(authSession()).rejects.toThrow('Session failed');
  });
});
