// @vitest-environment jsdom
import { RouterProvider, createMemoryRouter } from 'react-router';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from '@testing-library/react';

import { authLogin } from '../service/authLogin';
import Login from './Login';

// Mock authLogin
vi.mock('../service/authLogin', () => ({
  authLogin: vi.fn(),
}));

// Mock useAuth
const mockSetToken = vi.fn();
vi.mock('../context/AuthContext', () => ({
  useAuth: () => ({
    token: null,
    setToken: mockSetToken,
  }),
}));

// Mock useGoogleLogin from @react-oauth/google
interface UseGoogleLoginOptions {
  onSuccess: (response: { code: string }) => Promise<void> | void;
  flow?: string;
}

const mockLoginFn = vi.fn();
let capturedOptions: UseGoogleLoginOptions | null = null;

vi.mock('@react-oauth/google', () => ({
  useGoogleLogin: vi.fn((opts: UseGoogleLoginOptions) => {
    capturedOptions = opts;
    return mockLoginFn;
  }),
}));

describe('Login component', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    capturedOptions = null;
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it('renders login component and provides google signIn provider', () => {
    const router = createMemoryRouter(
      [{ path: '/login', element: <Login /> }],
      { initialEntries: ['/login'] }
    );

    render(<RouterProvider router={router} />);

    const button = screen.getByRole('button', { name: /google/i });
    expect(button).toBeTruthy();
  });

  it('triggers signin callback when clicked', () => {
    const router = createMemoryRouter(
      [{ path: '/login', element: <Login /> }],
      { initialEntries: ['/login'] }
    );

    render(<RouterProvider router={router} />);

    const button = screen.getByRole('button', { name: /google/i });
    fireEvent.click(button);

    expect(mockLoginFn).toHaveBeenCalled();
  });

  it('navigates backward when document referrer includes host', async () => {
    vi.stubGlobal('location', {
      host: 'localhost:3000',
    });

    Object.defineProperty(document, 'referrer', {
      value: 'http://localhost:3000/transcripts',
      configurable: true,
    });

    vi.mocked(authLogin).mockResolvedValueOnce('mocked-jwt-token');

    const router = createMemoryRouter(
      [
        { path: '/transcripts', element: <div>Transcripts</div> },
        { path: '/login', element: <Login /> },
      ],
      {
        initialEntries: ['/transcripts', '/login'],
        initialIndex: 1,
      }
    );

    render(<RouterProvider router={router} />);

    await act(async () => {
      await capturedOptions!.onSuccess({ code: 'test-code' });
    });

    expect(authLogin).toHaveBeenCalledWith('test-code');
    expect(mockSetToken).toHaveBeenCalledWith('mocked-jwt-token');
    expect(router.state.location.pathname).toBe('/transcripts');
  });

  it('navigates to root directory when referrer does not match host', async () => {
    vi.stubGlobal('location', {
      host: 'localhost:3000',
    });

    Object.defineProperty(document, 'referrer', {
      value: 'http://external-site.com/',
      configurable: true,
    });

    vi.mocked(authLogin).mockResolvedValueOnce('mocked-jwt-token');

    const router = createMemoryRouter(
      [
        { path: '/', element: <div>Root</div> },
        { path: '/login', element: <Login /> },
      ],
      {
        initialEntries: ['/login'],
        initialIndex: 0,
      }
    );

    render(<RouterProvider router={router} />);

    await act(async () => {
      await capturedOptions!.onSuccess({ code: 'test-code' });
    });

    expect(authLogin).toHaveBeenCalledWith('test-code');
    expect(mockSetToken).toHaveBeenCalledWith('mocked-jwt-token');
    expect(router.state.location.pathname).toBe('/');
  });

  it('navigates to root directory when referrer is empty', async () => {
    vi.stubGlobal('location', {
      host: 'localhost:3000',
    });

    Object.defineProperty(document, 'referrer', {
      value: '',
      configurable: true,
    });

    vi.mocked(authLogin).mockResolvedValueOnce('mocked-jwt-token');

    const router = createMemoryRouter(
      [
        { path: '/', element: <div>Root</div> },
        { path: '/login', element: <Login /> },
      ],
      {
        initialEntries: ['/login'],
        initialIndex: 0,
      }
    );

    render(<RouterProvider router={router} />);

    await act(async () => {
      await capturedOptions!.onSuccess({ code: 'test-code' });
    });

    expect(authLogin).toHaveBeenCalledWith('test-code');
    expect(mockSetToken).toHaveBeenCalledWith('mocked-jwt-token');
    expect(router.state.location.pathname).toBe('/');
  });

  it('shows error alert on authentication failure', async () => {
    const errorInstance = new Error('API error');
    vi.mocked(authLogin).mockRejectedValueOnce(errorInstance);

    const router = createMemoryRouter(
      [{ path: '/login', element: <Login /> }],
      { initialEntries: ['/login'] }
    );

    render(<RouterProvider router={router} />);

    await act(async () => {
      await capturedOptions!.onSuccess({ code: 'test-code' });
    });

    expect(screen.getByText('Unable to sign in. Please try again.')).toBeTruthy();
    expect(mockSetToken).not.toHaveBeenCalled();
    expect(router.state.location.pathname).toBe('/login');
  });
});
