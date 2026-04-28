import type * as express from 'express';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { AuthController } from './authController.js';

const { mockGetToken, mockRefreshAccessToken, mockSetCredentials } = vi.hoisted(
  () => ({
    mockGetToken: vi.fn(),
    mockRefreshAccessToken: vi.fn(),
    mockSetCredentials: vi.fn(),
  })
);

vi.mock('google-auth-library', () => {
  const OAuth2Client = vi.fn().mockImplementation(() => {
    return {
      getToken: mockGetToken,
      refreshAccessToken: mockRefreshAccessToken,
      setCredentials: mockSetCredentials,
    };
  });
  return { OAuth2Client };
});

vi.mock('../config.js', () => ({
  ALLOWED_ORIGIN: 'http://localhost:5173',
  GOOGLE_AUTH_CLIENT_ID: 'test-google-client-id',
  GOOGLE_AUTH_CLIENT_SECRET: 'test-google-client-secret',
}));

describe('AuthController', () => {
  beforeEach(() => {
    vi.clearAllMocks();
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

      // The catch block unconditionally calls setStatus(401) after setting setStatus(400)
      expect(controller.getStatus()).toBe(401);
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

      expect(controller.getStatus()).toBe(401);
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
          sameSite: 'none',
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

      expect(controller.getStatus()).toBe(401);
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

      // The catch block overrides the status code to 401
      expect(controller.getStatus()).toBe(401);
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

      expect(controller.getStatus()).toBe(401);
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
