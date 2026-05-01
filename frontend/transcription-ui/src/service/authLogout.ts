import { apiFetch } from '../utils/apiUtils';

/**
 * Logs the user out by revoking the refresh token.
 *
 * @returns {void}
 */
export async function authLogout(): Promise<void> {
  await apiFetch<void>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/auth/logout`,
    {
      method: 'POST',
      credentials: 'include',
    }
  );
}
