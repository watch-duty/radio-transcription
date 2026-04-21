// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { clearAuthToken, getAuthToken, setAuthToken } from './authCookie';

/**
 * Builds a minimal base64url-encoded JWT with the given `exp` Unix timestamp.
 * The signature segment is a placeholder — only the header and payload are
 * decoded by the production code.
 */
function makeJwt(exp: number): string {
  const toBase64url = (s: string) =>
    btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
  const header = toBase64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = toBase64url(JSON.stringify({ sub: 'user1', exp }));
  return `${header}.${payload}.fakesig`;
}

/** Removes all cookies from the jsdom document. */
function clearAllCookies() {
  document.cookie.split('; ').forEach((cookie) => {
    const name = cookie.split('=')[0];
    if (name) {
      document.cookie = `${name}=; max-age=0`;
    }
  });
}

describe('authCookie', () => {
  beforeEach(() => {
    clearAllCookies();
  });

  describe('getAuthToken', () => {
    it('returns null when no cookie is present', () => {
      expect(getAuthToken()).toBeNull();
    });

    it('returns the token string for a valid unexpired token', () => {
      const futureExp = Math.floor(Date.now() / 1000) + 3600;
      const token = makeJwt(futureExp);
      document.cookie = `auth_token=${token}`;
      expect(getAuthToken()).toBe(token);
    });

    it('returns null and clears the cookie for an expired token', () => {
      const pastExp = Math.floor(Date.now() / 1000) - 3600;
      const token = makeJwt(pastExp);
      document.cookie = `auth_token=${token}`;
      expect(getAuthToken()).toBeNull();
      expect(document.cookie).not.toContain('auth_token=');
    });

    it('returns null for a malformed non-JWT cookie value without throwing', () => {
      document.cookie = 'auth_token=not-a-jwt';
      expect(getAuthToken()).toBeNull();
    });
  });

  describe('setAuthToken', () => {
    it('writes auth_token with path=/, SameSite=Strict, and expires derived from exp', () => {
      const cookieSetter = vi.spyOn(document, 'cookie', 'set');
      const futureExp = Math.floor(Date.now() / 1000) + 3600;
      const token = makeJwt(futureExp);

      setAuthToken(token);

      const written = cookieSetter.mock.calls[0][0];
      expect(written).toContain('auth_token=');
      expect(written).toContain('path=/');
      expect(written).toContain('SameSite=Strict');
      expect(written).toContain('expires=');
      cookieSetter.mockRestore();
    });

    it('makes the token readable back via getAuthToken', () => {
      const futureExp = Math.floor(Date.now() / 1000) + 3600;
      const token = makeJwt(futureExp);
      setAuthToken(token);
      expect(getAuthToken()).toBe(token);
    });
  });

  describe('clearAuthToken', () => {
    it('removes the auth_token cookie', () => {
      const futureExp = Math.floor(Date.now() / 1000) + 3600;
      const token = makeJwt(futureExp);
      document.cookie = `auth_token=${token}`;
      expect(document.cookie).toContain('auth_token=');

      clearAuthToken();

      expect(document.cookie).not.toContain('auth_token=');
    });
  });
});
