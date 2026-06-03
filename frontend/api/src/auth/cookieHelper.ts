import * as express from 'express';

import { ALLOWED_ORIGIN } from '../config.js';

export function setRefreshTokenCookie(
  res: express.Response,
  refreshToken: string
): void {
  // Allow http://localhost for development.
  const isLocal =
    ALLOWED_ORIGIN.includes('localhost') ||
    ALLOWED_ORIGIN.includes('127.0.0.1');
  res.cookie('refresh_token', refreshToken, {
    // Tells the browser that this cookie should only be accessed via HTTP(S) requests
    // and should not be accessible by client-side scripts (like JavaScript).
    httpOnly: true,
    // Tells the browser that this cookie should only be sent with secure (HTTPS) requests.
    secure: !isLocal,
    // Allows the cookie to be sent with cross-site requests, which happens when the UI
    // and API are deployed to different domains.
    sameSite: isLocal ? 'lax' : 'none',
    // Lifetime of the cookie in milliseconds.
    maxAge: 30 * 24 * 60 * 60 * 1000, // 30 days
  });
}
