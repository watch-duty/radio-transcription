import { apiFetch } from '../utils/apiUtils';

/**
 * Requests an ID token from the backend.
 * The refresh_token is stored as an HttpOnly cookie, which is automatically
 * passed by the browser.
 *
 * @returns the ID token of the user
 */
export async function authSession(): Promise<string> {
  const data = await apiFetch<{ idToken: string }>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/auth/session`,
    {
      credentials: 'include',
    }
  );

  return data.idToken;
}
