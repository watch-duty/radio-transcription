/**
 * Requests an ID token from the backend.
 * The refresh_token is stored as an HttpOnly cookie, which is automatically
 * passed by the browser.
 *
 * @returns the ID token of the user
 */
export async function authSession(): Promise<string> {
  // TODO: improve error handling for all APIs: https://linear.app/watchduty/issue/GOO-369/improve-ui-error-handling-for-api-requests
  const response = await fetch(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/auth/session`,
    {
      credentials: 'include',
    }
  );

  if (!response.ok) {
    throw new Error('Session failed');
  }

  const data = await response.json();
  return data.idToken;
}
