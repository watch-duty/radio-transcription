/**
 * Logs the user out by revoking the refresh token.
 *
 * @returns {void}
 */
export async function authLogout(): Promise<void> {
  const response = await fetch(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/auth/logout`,
    {
      method: 'POST',
      credentials: 'include',
    }
  );

  if (!response.ok) {
    throw new Error('Logout failed');
  }
}
