import * as express from 'express';
import * as jose from 'jose';

import { HttpError } from '../utils.js';
import { AuthService, LoginResponse } from './authService.js';
import { setRefreshTokenCookie } from './cookieHelper.js';

export class MockAuthService implements AuthService {
  private async generateMockIdToken(): Promise<string> {
    return await new jose.SignJWT({
      email: 'test@example.com',
      email_verified: true,
      sub: '12345',
      aud: 'dummy_aud',
      iss: 'https://accounts.google.com',
    })
      .setProtectedHeader({ alg: 'HS256' })
      .sign(new TextEncoder().encode('dummy_secret'));
  }

  public async login(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    code: string,
    req: express.Request
  ): Promise<LoginResponse> {
    if (req.res) {
      setRefreshTokenCookie(req.res, 'mock_refresh_token');
    }
    const idToken = await this.generateMockIdToken();
    return { idToken };
  }

  public async refresh(req: express.Request): Promise<LoginResponse> {
    const refreshToken = req.cookies?.refresh_token;

    if (!refreshToken) {
      throw new HttpError(401, 'No refresh token in session');
    }

    if (req.res) {
      setRefreshTokenCookie(req.res, 'mock_refresh_token_rotated');
    }
    const idToken = await this.generateMockIdToken();
    return { idToken };
  }
}
