import type * as express from 'express';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { AuthController } from './authController.js';

const { mockGetToken, mockRefreshAccessToken, mockSetCredentials, mockState } =
  vi.hoisted(() => ({
    mockGetToken: vi.fn(),
    mockRefreshAccessToken: vi.fn(),
    mockSetCredentials: vi.fn(),
    mockState: {
      allowedOrigin: 'http://localhost:5173',
    },
  }));

vi.mock('google-auth-library', () => {
  class MockOAuth2Client {
    getToken = mockGetToken;
    refreshAccessToken = mockRefreshAccessToken;
    setCredentials = mockSetCredentials;
  }

  const OAuth2Client = MockOAuth2Client;
  return { OAuth2Client };
});

vi.mock('../config.js', () => ({
  AUTH_BACKEND: 'google',
  get ALLOWED_ORIGIN() {
    return mockState.allowedOrigin;
  },
  GOOGLE_AUTH_CLIENT_ID: 'test-google-client-id',
  GOOGLE_AUTH_CLIENT_SECRET: 'test-google-client-secret',
}));

describe('AuthController', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockState.allowedOrigin = 'http://localhost:5173';
  });

  describe('login', () => {
    it('should successfully set cookie and return idToken', async () => {
      mockGetToken.mockResolvedValueOnce({
        tokens: {
          refresh_token: 'test_refresh_token',
          id_token: 'test_id_token',
        },
      });

      const controller = new AuthController();
      const mockCookie = vi.fn();
      const mockReq = {
        res: {
          cookie: mockCookie,
        },
      } as unknown as express.Request;

      const result = await controller.login({ code: 'test_code' }, mockReq);

      expect(result).toEqual({ idToken: 'test_id_token' });
      expect(mockGetToken).toHaveBeenCalledWith('test_code');
      expect(mockCookie).toHaveBeenCalledWith(
        'refresh_token',
        'test_refresh_token',
        expect.objectContaining({
          httpOnly: true,
          secure: false,
          sameSite: 'lax',
        })
      );
    });

    it('should successfully set cookie with secure=true and sameSite="none" for non-localhost cross-origin request', async () => {
      mockState.allowedOrigin = 'https://app.mywebsite.org';

      mockGetToken.mockResolvedValueOnce({
        tokens: {
          refresh_token: 'prod_refresh_token',
          id_token: 'test_id_token',
        },
      });

      const controller = new AuthController();
      const mockCookie = vi.fn();
      const mockReq = {
        res: {
          cookie: mockCookie,
        },
      } as unknown as express.Request;

      const result = await controller.login({ code: 'test_code' }, mockReq);

      expect(result).toEqual({ idToken: 'test_id_token' });
      expect(mockCookie).toHaveBeenCalledWith(
        'refresh_token',
        'prod_refresh_token',
        expect.objectContaining({
          httpOnly: true,
          secure: true,
          sameSite: 'none',
        })
      );
    });

    it('should throw when Google responds with no id_token and overwrite status to 401', async () => {
      mockGetToken.mockResolvedValueOnce({
        tokens: {
          refresh_token: 'test_refresh_token',
        },
      });

      const controller = new AuthController();
      const mockReq = {
        res: {
          cookie: vi.fn(),
        },
      } as unknown as express.Request;

      await expect(
        controller.login({ code: 'test_code' }, mockReq)
      ).rejects.toThrow('No ID token returned from Google');
    });

    it('should bubble exceptions up with status 401 on google auth failures', async () => {
      mockGetToken.mockRejectedValueOnce(
        new Error('Invalid authorization token')
      );

      const controller = new AuthController();
      const mockReq = {
        res: {
          cookie: vi.fn(),
        },
      } as unknown as express.Request;

      await expect(
        controller.login({ code: 'invalid_code' }, mockReq)
      ).rejects.toThrow('Invalid authorization token');
    });
  });

  describe('refresh', () => {
    it('should successfully return refreshed idToken and set rotated cookie when available', async () => {
      mockRefreshAccessToken.mockResolvedValueOnce({
        credentials: {
          id_token: 'refreshed_id_token',
          refresh_token: 'rotated_refresh_token',
        },
      });

      const controller = new AuthController();
      const mockCookie = vi.fn();
      const mockReq = {
        cookies: {
          refresh_token: 'test_refresh_token',
        },
        res: {
          cookie: mockCookie,
        },
      } as unknown as express.Request;

      const result = await controller.refresh(mockReq);

      expect(result).toEqual({ idToken: 'refreshed_id_token' });
      expect(mockSetCredentials).toHaveBeenCalledWith({
        refresh_token: 'test_refresh_token',
      });

      // Rotated cookie configuration check
      expect(mockCookie).toHaveBeenCalledWith(
        'refresh_token',
        'rotated_refresh_token',
        expect.objectContaining({
          httpOnly: true,
          secure: false,
          sameSite: 'lax',
        })
      );
    });

    it('should throw 401 if no refresh token exists in session cookies', async () => {
      const controller = new AuthController();
      const mockReq = {
        cookies: {},
      } as unknown as express.Request;

      await expect(controller.refresh(mockReq)).rejects.toThrow(
        'No refresh token in session'
      );
    });

    it('should throw and overwrite status to 401 when refreshed id token is missing from Google credentials', async () => {
      mockRefreshAccessToken.mockResolvedValueOnce({
        credentials: {
          refresh_token: 'rotated_refresh_token',
        },
      });

      const controller = new AuthController();
      const mockReq = {
        cookies: {
          refresh_token: 'test_refresh_token',
        },
      } as unknown as express.Request;

      await expect(controller.refresh(mockReq)).rejects.toThrow(
        'Failed to refresh ID token'
      );
    });

    it('should bubble exception and set status 401 on credential failure', async () => {
      mockRefreshAccessToken.mockRejectedValueOnce(
        new Error('Credentials validation error')
      );

      const controller = new AuthController();
      const mockReq = {
        cookies: {
          refresh_token: 'stale_refresh_token',
        },
      } as unknown as express.Request;

      await expect(controller.refresh(mockReq)).rejects.toThrow(
        'Credentials validation error'
      );
    });
  });

  describe('logout', () => {
    it('should clear cookie storage values and respond with status 204', async () => {
      const controller = new AuthController();
      const mockClearCookie = vi.fn();
      const mockReq = {
        res: {
          clearCookie: mockClearCookie,
        },
      } as unknown as express.Request;

      await controller.logout(mockReq);

      expect(mockClearCookie).toHaveBeenCalledWith('refresh_token');
      expect(controller.getStatus()).toBe(204);
    });
  });
});
