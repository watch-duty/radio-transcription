// @vitest-environment jsdom
import { MemoryRouter } from 'react-router';

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

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router', async (importOriginal) => {
  const actual = await importOriginal<typeof import('react-router')>();
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

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
    render(
      <MemoryRouter>
        <Login />
      </MemoryRouter>
    );

    // Check for the presence of a button that initiates signIn for provider 'Google'
    const button = screen.getByRole('button', { name: /google/i });
    expect(button).toBeTruthy();
  });

  it('triggers signin callback when clicked', () => {
    render(
      <MemoryRouter>
        <Login />
      </MemoryRouter>
    );

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

    render(
      <MemoryRouter>
        <Login />
      </MemoryRouter>
    );

    await act(async () => {
      await capturedOptions!.onSuccess({ code: 'test-code' });
    });

    expect(authLogin).toHaveBeenCalledWith('test-code');
    expect(mockSetToken).toHaveBeenCalledWith('mocked-jwt-token');
    expect(mockNavigate).toHaveBeenCalledWith(-1);
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

    render(
      <MemoryRouter>
        <Login />
      </MemoryRouter>
    );

    await act(async () => {
      await capturedOptions!.onSuccess({ code: 'test-code' });
    });

    expect(authLogin).toHaveBeenCalledWith('test-code');
    expect(mockSetToken).toHaveBeenCalledWith('mocked-jwt-token');
    expect(mockNavigate).toHaveBeenCalledWith('/', { replace: true });
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

    render(
      <MemoryRouter>
        <Login />
      </MemoryRouter>
    );

    await act(async () => {
      await capturedOptions!.onSuccess({ code: 'test-code' });
    });

    expect(authLogin).toHaveBeenCalledWith('test-code');
    expect(mockSetToken).toHaveBeenCalledWith('mocked-jwt-token');
    expect(mockNavigate).toHaveBeenCalledWith('/', { replace: true });
  });

  it('logs console.error on authentication failure', async () => {
    const errorInstance = new Error('API error');
    vi.mocked(authLogin).mockRejectedValueOnce(errorInstance);

    render(
      <MemoryRouter>
        <Login />
      </MemoryRouter>
    );

    await act(async () => {
      await capturedOptions!.onSuccess({ code: 'test-code' });
    });

    expect(console.error).toHaveBeenCalledWith('Login failed:', errorInstance);
    expect(mockSetToken).not.toHaveBeenCalled();
    expect(mockNavigate).not.toHaveBeenCalled();
  });
});
