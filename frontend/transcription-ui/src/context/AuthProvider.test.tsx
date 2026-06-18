// @vitest-environment jsdom
import { decodeJwt } from 'jose';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { act, cleanup, render, screen } from '@testing-library/react';

import { authSession } from '../service/authSession';
import { useAuth } from './AuthContext';
import { AuthProvider } from './AuthProvider';

vi.mock('jose', () => ({
  decodeJwt: vi.fn(),
}));

vi.mock('../service/authSession', () => ({
  authSession: vi.fn(),
}));

vi.mock('../service/getUserInfo', () => ({
  getUserInfo: vi
    .fn()
    .mockResolvedValue({ email: 'test@watchduty.org', isAdmin: false }),
}));

const TestConsumer = () => {
  const { token } = useAuth();
  return <div data-testid="token-display">{token || 'no-token'}</div>;
};

describe('AuthProvider', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('fetches session on mount and provides the token', async () => {
    vi.mocked(authSession).mockResolvedValueOnce('fake-jwt-123');

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    );

    // Should render loading state initially
    expect(screen.getByText(/loading/i)).toBeTruthy();

    // Resolve the promise
    await act(async () => {
      await Promise.resolve();
    });

    expect(screen.getByTestId('token-display').textContent).toBe(
      'fake-jwt-123'
    );
    expect(authSession).toHaveBeenCalledTimes(1);
  });

  it('does not refresh the token if it is not expired or expiring soon', async () => {
    vi.mocked(authSession).mockResolvedValueOnce('fake-jwt-123');
    vi.mocked(decodeJwt).mockReturnValue({
      exp: (Date.now() + 30 * 60 * 1000) / 1000,
    }); // Expires in 30 minutes

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    );

    await act(async () => {
      await Promise.resolve();
    });

    expect(authSession).toHaveBeenCalledTimes(1);

    // Trigger visibilitychange (tab becomes visible)
    await act(async () => {
      Object.defineProperty(document, 'visibilityState', {
        value: 'visible',
        writable: true,
      });
      document.dispatchEvent(new Event('visibilitychange'));
    });

    // Should not call authSession since token is still valid (expires in 30m)
    expect(authSession).toHaveBeenCalledTimes(1);
  });

  it('refreshes the token on visibility change if the token is close to expiring', async () => {
    vi.mocked(authSession).mockResolvedValueOnce('fake-jwt-123');
    // First decode: token is expiring in 4 minutes (less than 5m threshold)
    vi.mocked(decodeJwt).mockReturnValueOnce({
      exp: (Date.now() + 4 * 60 * 1000) / 1000,
    });
    vi.mocked(authSession).mockResolvedValueOnce('new-refreshed-jwt');

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    );

    await act(async () => {
      await Promise.resolve();
    });

    expect(screen.getByTestId('token-display').textContent).toBe(
      'fake-jwt-123'
    );

    // Trigger visibilitychange
    await act(async () => {
      Object.defineProperty(document, 'visibilityState', {
        value: 'visible',
        writable: true,
      });
      document.dispatchEvent(new Event('visibilitychange'));
    });

    // Wait for the async refresh to resolve
    await act(async () => {
      await Promise.resolve();
    });

    expect(authSession).toHaveBeenCalledTimes(2);
    expect(screen.getByTestId('token-display').textContent).toBe(
      'new-refreshed-jwt'
    );
  });

  it('refreshes the token on window focus if the token is close to expiring', async () => {
    vi.mocked(authSession).mockResolvedValueOnce('fake-jwt-123');
    // Token is expiring in 1 minute
    vi.mocked(decodeJwt).mockReturnValueOnce({
      exp: (Date.now() + 1 * 60 * 1000) / 1000,
    });
    vi.mocked(authSession).mockResolvedValueOnce(
      'new-refreshed-jwt-from-focus'
    );

    render(
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    );

    await act(async () => {
      await Promise.resolve();
    });

    // Trigger window focus
    await act(async () => {
      window.dispatchEvent(new Event('focus'));
    });

    // Wait for the async refresh to resolve
    await act(async () => {
      await Promise.resolve();
    });

    expect(authSession).toHaveBeenCalledTimes(2);
    expect(screen.getByTestId('token-display').textContent).toBe(
      'new-refreshed-jwt-from-focus'
    );
  });
});
