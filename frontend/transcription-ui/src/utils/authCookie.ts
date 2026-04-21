const COOKIE_NAME = 'auth_token';

/**
 * Decodes the expiry timestamp from a JWT's payload.
 *
 * @param token The JWT string to decode.
 * @returns The `exp` claim as a Unix timestamp (seconds), or null if the token
 *   is malformed or has no `exp` claim.
 */
function getJwtExpiry(token: string): number | null {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return null;
    // base64url → base64 before passing to atob
    const base64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const payload = JSON.parse(atob(base64)) as Record<string, unknown>;
    return typeof payload['exp'] === 'number' ? payload['exp'] : null;
  } catch {
    return null;
  }
}

/**
 * Reads the persisted auth token from the `auth_token` cookie.
 * Validates the JWT expiry; clears and returns null if expired or malformed.
 *
 * @returns The token string, or null if absent, expired, or malformed.
 */
export function getAuthToken(): string | null {
  const match = document.cookie
    .split('; ')
    .find((row) => row.startsWith(`${COOKIE_NAME}=`));
  if (!match) return null;

  const token = match.slice(COOKIE_NAME.length + 1);
  const exp = getJwtExpiry(token);

  if (exp === null || Date.now() / 1000 >= exp) {
    clearAuthToken();
    return null;
  }

  return token;
}

/**
 * Persists the auth token in the `auth_token` cookie.
 * Sets the cookie `expires` attribute to the JWT's own `exp` timestamp so
 * the browser auto-expires the cookie when the token becomes invalid.
 *
 * @param token The JWT string to persist.
 */
export function setAuthToken(token: string): void {
  const exp = getJwtExpiry(token);
  const expiresAttr = exp
    ? `; expires=${new Date(exp * 1000).toUTCString()}`
    : '';
  document.cookie = `${COOKIE_NAME}=${token}; path=/; SameSite=Strict; Secure${expiresAttr}`;
}

/**
 * Removes the `auth_token` cookie.
 */
export function clearAuthToken(): void {
  document.cookie = `${COOKIE_NAME}=; path=/; SameSite=Strict; Secure; max-age=0`;
}
