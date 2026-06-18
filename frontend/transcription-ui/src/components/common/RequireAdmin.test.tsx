// @vitest-environment jsdom
import { RouterProvider, createMemoryRouter } from 'react-router';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { act, cleanup, render, screen } from '@testing-library/react';

import { useAuth } from '../../context/AuthContext';
import { getUserInfo } from '../../service/getUserInfo';
import { RequireAdmin } from './RequireAdmin';

// Mock getUserInfo with a robust implementation based on token value
vi.mock('../../service/getUserInfo', () => ({
  getUserInfo: vi.fn().mockImplementation(async (token: string) => {
    if (token === 'test-token-admin') {
      return { email: 'admin@watchduty.org', isAdmin: true };
    }
    if (token === 'test-token-user') {
      return { email: 'user@watchduty.org', isAdmin: false };
    }
    if (token === 'test-token-error') {
      throw new Error('API error');
    }
    return undefined;
  }),
}));

// Mock useAuth
const mockSetIsAdmin = vi.fn();
let mockToken: string | null = null;
let mockIsAdmin = false;

vi.mock('../../context/AuthContext', () => ({
  useAuth: vi.fn(() => ({
    token: mockToken,
    isAdmin: mockIsAdmin,
    setIsAdmin: mockSetIsAdmin,
  })),
}));

describe('RequireAdmin component', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockToken = null;
    mockIsAdmin = false;
  });

  afterEach(() => {
    cleanup();
  });

  it('redirects to /login if no token is present', () => {
    const router = createMemoryRouter(
      [
        {
          path: '/admin',
          element: (
            <RequireAdmin>
              <div>Admin Page</div>
            </RequireAdmin>
          ),
        },
        {
          path: '/login',
          element: <div>Login Page</div>,
        },
      ],
      { initialEntries: ['/admin'] }
    );

    render(<RouterProvider router={router} />);

    expect(router.state.location.pathname).toBe('/login');
  });

  it('fetches user status and renders children on success', async () => {
    mockToken = 'test-token-admin';

    // Mock useAuth to change isAdmin state to true after verify completes
    vi.mocked(useAuth).mockImplementation(() => ({
      token: 'test-token-admin',
      isAdmin: mockIsAdmin,
      setIsAdmin: (val) => {
        mockIsAdmin = val;
        mockSetIsAdmin(val);
      },
    }));

    const router = createMemoryRouter(
      [
        {
          path: '/admin',
          element: (
            <RequireAdmin>
              <div>Admin Page</div>
            </RequireAdmin>
          ),
        },
      ],
      { initialEntries: ['/admin'] }
    );

    await act(async () => {
      render(<RouterProvider router={router} />);
    });

    // Wait for the verification promise to resolve
    await act(async () => {
      await Promise.resolve();
    });

    expect(getUserInfo).toHaveBeenCalledWith('test-token-admin');
    expect(mockSetIsAdmin).toHaveBeenCalledWith(true);
    expect(screen.queryByRole('progressbar')).toBeNull();
    expect(screen.getByText('Admin Page')).toBeTruthy();
  });

  it('redirects to / if verification determines user is not admin', async () => {
    mockToken = 'test-token-user';

    // Mock useAuth
    vi.mocked(useAuth).mockImplementation(() => ({
      token: 'test-token-user',
      isAdmin: mockIsAdmin,
      setIsAdmin: (val) => {
        mockIsAdmin = val;
        mockSetIsAdmin(val);
      },
    }));

    const router = createMemoryRouter(
      [
        {
          path: '/admin',
          element: (
            <RequireAdmin>
              <div>Admin Page</div>
            </RequireAdmin>
          ),
        },
        {
          path: '/',
          element: <div>Home Page</div>,
        },
      ],
      { initialEntries: ['/admin'] }
    );

    await act(async () => {
      render(<RouterProvider router={router} />);
    });

    // Wait for promise
    await act(async () => {
      await Promise.resolve();
    });

    expect(getUserInfo).toHaveBeenCalledWith('test-token-user');
    expect(mockSetIsAdmin).toHaveBeenCalledWith(false);
    expect(router.state.location.pathname).toBe('/');
  });

  it('redirects to / and sets isAdmin=false if getUserInfo API throws', async () => {
    mockToken = 'test-token-error';

    // Mock useAuth
    vi.mocked(useAuth).mockImplementation(() => ({
      token: 'test-token-error',
      isAdmin: mockIsAdmin,
      setIsAdmin: (val) => {
        mockIsAdmin = val;
        mockSetIsAdmin(val);
      },
    }));

    const router = createMemoryRouter(
      [
        {
          path: '/admin',
          element: (
            <RequireAdmin>
              <div>Admin Page</div>
            </RequireAdmin>
          ),
        },
        {
          path: '/',
          element: <div>Home Page</div>,
        },
      ],
      { initialEntries: ['/admin'] }
    );

    await act(async () => {
      render(<RouterProvider router={router} />);
    });

    // Wait for promise
    await act(async () => {
      await Promise.resolve();
    });

    expect(getUserInfo).toHaveBeenCalledWith('test-token-error');
    expect(mockSetIsAdmin).toHaveBeenCalledWith(false);
    expect(router.state.location.pathname).toBe('/');
  });
});
