// @vitest-environment jsdom
import { RouterProvider, createMemoryRouter } from 'react-router';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { cleanup, render, screen } from '@testing-library/react';

import { RequireAdmin } from './RequireAdmin';

// Mock useAuth
const mockSetToken = vi.fn();
const mockSetIsAdmin = vi.fn();
let mockToken: string | null = null;
let mockIsAdmin = false;
let mockIsLoading = false;
let mockIsError = false;

vi.mock('../../context/AuthContext', () => ({
  useAuth: vi.fn(() => ({
    token: mockToken,
    setToken: mockSetToken,
    isAdmin: mockIsAdmin,
    setIsAdmin: mockSetIsAdmin,
    isLoading: mockIsLoading,
    isError: mockIsError,
  })),
}));

describe('RequireAdmin component', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockToken = null;
    mockIsAdmin = false;
    mockIsLoading = false;
    mockIsError = false;
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

  it('renders loading progressbar when auth is loading', () => {
    mockToken = 'test-token';
    mockIsLoading = true;

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

    render(<RouterProvider router={router} />);

    expect(screen.getByRole('progressbar')).toBeTruthy();
    expect(screen.queryByText('Admin Page')).toBeNull();
  });

  it('renders children when auth is not loading and user is admin', () => {
    mockToken = 'test-token';
    mockIsLoading = false;
    mockIsAdmin = true;

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

    render(<RouterProvider router={router} />);

    expect(screen.getByText('Admin Page')).toBeTruthy();
    expect(screen.queryByRole('progressbar')).toBeNull();
  });

  it('redirects to / if user is not admin', () => {
    mockToken = 'test-token';
    mockIsLoading = false;
    mockIsAdmin = false;

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

    render(<RouterProvider router={router} />);

    expect(router.state.location.pathname).toBe('/');
  });

  it('renders error alert when auth query fails with error', () => {
    mockToken = 'test-token';
    mockIsLoading = false;
    mockIsError = true;
    mockIsAdmin = false;

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

    render(<RouterProvider router={router} />);

    expect(
      screen.getByText('Failed to load user permissions. Please try again.')
    ).toBeTruthy();
    expect(router.state.location.pathname).toBe('/admin');
  });
});
