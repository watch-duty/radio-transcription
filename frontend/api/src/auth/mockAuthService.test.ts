import type * as express from 'express';

import { describe, expect, it, vi } from 'vitest';

import { MockAuthService } from './mockAuthService.js';

vi.mock('../config.js', () => ({
  ALLOWED_ORIGIN: 'http://localhost:5173',
}));

describe('MockAuthService', () => {
  describe('login', () => {
    it('should successfully set mock cookie and return a mock idToken', async () => {
      const service = new MockAuthService();
      const mockCookie = vi.fn();
      const mockReq = {
        res: {
          cookie: mockCookie,
        },
      } as unknown as express.Request;

      const result = await service.login('any-code', mockReq);

      expect(result.idToken).toBeDefined();
      expect(mockCookie).toHaveBeenCalledWith(
        'refresh_token',
        'mock_refresh_token',
        expect.objectContaining({
          httpOnly: true,
          secure: false,
          sameSite: 'lax',
        })
      );
    });
  });

  describe('refresh', () => {
    it('should successfully refresh session with rotated cookie when a cookie exists', async () => {
      const service = new MockAuthService();
      const mockCookie = vi.fn();
      const mockReq = {
        cookies: {
          refresh_token: 'existing_mock_token',
        },
        res: {
          cookie: mockCookie,
        },
      } as unknown as express.Request;

      const result = await service.refresh(mockReq);

      expect(result.idToken).toBeDefined();
      expect(mockCookie).toHaveBeenCalledWith(
        'refresh_token',
        'mock_refresh_token_rotated',
        expect.objectContaining({
          httpOnly: true,
          secure: false,
          sameSite: 'lax',
        })
      );
    });

    it('should throw 401 if refresh token is missing from cookies', async () => {
      const service = new MockAuthService();
      const mockReq = {
        cookies: {},
      } as unknown as express.Request;

      await expect(service.refresh(mockReq)).rejects.toThrow(
        'No refresh token in session'
      );
    });
  });
});
