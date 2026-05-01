import { apiFetch } from '../utils/apiUtils';

/**
 * Authenticates the user with an authorization code provided by Google OAuth.
 *
 * @param code The authorization code provided by Google OAuth.
 * @returns The ID token for the authenticated user.
 */
export async function authLogin(code: string): Promise<string> {
  const data = await apiFetch<{ idToken: string }>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/auth/google`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
      body: JSON.stringify({ code }),
    }
  );

  return data.idToken;
}
