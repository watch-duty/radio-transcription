// @vitest-environment jsdom
import { act, renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useAuth } from './AuthContext';
import { AuthProvider } from './AuthProvider';
import * as authCookie from '../utils/authCookie';

describe('AuthProvider', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('initializes token to null when no cookie is present', () => {
    vi.spyOn(authCookie, 'getAuthToken').mockReturnValue(null);

    const { result } = renderHook(() => useAuth(), { wrapper: AuthProvider });

    expect(result.current.token).toBeNull();
  });

  it('initializes token from cookie when a valid stored token exists', () => {
    vi.spyOn(authCookie, 'getAuthToken').mockReturnValue('stored-token');

    const { result } = renderHook(() => useAuth(), { wrapper: AuthProvider });

    expect(result.current.token).toBe('stored-token');
  });

  it('calls setAuthToken and updates token state when setToken is called with a value', () => {
    vi.spyOn(authCookie, 'getAuthToken').mockReturnValue(null);
    const setAuthTokenSpy = vi
      .spyOn(authCookie, 'setAuthToken')
      .mockImplementation(() => {});

    const { result } = renderHook(() => useAuth(), { wrapper: AuthProvider });

    act(() => {
      result.current.setToken('new-token');
    });

    expect(setAuthTokenSpy).toHaveBeenCalledWith('new-token');
    expect(result.current.token).toBe('new-token');
  });

  it('calls clearAuthToken and resets token state when setToken is called with null', () => {
    vi.spyOn(authCookie, 'getAuthToken').mockReturnValue('existing-token');
    const clearAuthTokenSpy = vi
      .spyOn(authCookie, 'clearAuthToken')
      .mockImplementation(() => {});

    const { result } = renderHook(() => useAuth(), { wrapper: AuthProvider });

    act(() => {
      result.current.setToken(null);
    });

    expect(clearAuthTokenSpy).toHaveBeenCalled();
    expect(result.current.token).toBeNull();
  });
});
