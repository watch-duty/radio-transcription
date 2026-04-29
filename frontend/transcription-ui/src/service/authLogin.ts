/**
 * Authenticates the user with an authorization code provided by Google OAuth.
 *
 * @param code The authorization code provided by Google OAuth.
 * @returns The ID token for the authenticated user.
 */
export async function authLogin(code: string) {
  try {
    const response = await fetch(
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

    if (!response.ok) {
      throw new Error('Login failed');
    }

    const data = await response.json();
    return data.idToken;
  } catch (error) {
    console.error('Login failed:', error);
    throw error;
  }
}
