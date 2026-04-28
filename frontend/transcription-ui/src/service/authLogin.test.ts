import { beforeEach, describe, expect, it, vi } from 'vitest';

import { authLogin } from './authLogin';

describe('authLogin', () => {
  const mockFetch = vi.fn();

  beforeEach(() => {
    mockFetch.mockClear();
    vi.stubGlobal('fetch', mockFetch);
    vi.stubGlobal('console', { ...console, error: vi.fn() });
  });

  it('should authenticate successfully with authorization code', async () => {
    const responsePayload = {
      idToken: 'test-jwt-id-token',
    };

    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => responsePayload,
    });

    const token = await authLogin('google-oauth-code');

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/auth/google'),
      expect.objectContaining({
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({ code: 'google-oauth-code' }),
      })
    );
    expect(token).toEqual('test-jwt-id-token');
  });

  it('should throw error if network response returns not ok status', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
    });

    await expect(authLogin('error-generating-code')).rejects.toThrow(
      'Login failed'
    );
  });
});
