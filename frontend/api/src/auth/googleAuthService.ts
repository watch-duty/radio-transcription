import * as express from 'express';
import { OAuth2Client } from 'google-auth-library';

import {
  GOOGLE_AUTH_CLIENT_ID,
  GOOGLE_AUTH_CLIENT_SECRET,
} from '../config.js';
import { HttpError } from '../utils.js';
import { AuthService, LoginResponse } from './authService.js';
import { setRefreshTokenCookie } from './cookieHelper.js';

export class GoogleAuthService implements AuthService {
  private client = new OAuth2Client(
    GOOGLE_AUTH_CLIENT_ID,
    GOOGLE_AUTH_CLIENT_SECRET,
    'postmessage'
  );

  public async login(
    code: string,
    req: express.Request
  ): Promise<LoginResponse> {
    const { tokens } = await this.client.getToken(code);

    if (tokens.refresh_token && req.res) {
      setRefreshTokenCookie(req.res, tokens.refresh_token);
    }

    if (!tokens.id_token) {
      throw new HttpError(400, 'No ID token returned from Google');
    }

    return { idToken: tokens.id_token };
  }

  public async refresh(req: express.Request): Promise<LoginResponse> {
    const refreshToken = req.cookies?.refresh_token;

    if (!refreshToken) {
      throw new HttpError(401, 'No refresh token in session');
    }

    this.client.setCredentials({ refresh_token: refreshToken });
    const res = await this.client.refreshAccessToken();

    if (!res.credentials.id_token) {
      throw new HttpError(400, 'Failed to refresh ID token');
    }

    if (res.credentials.refresh_token && req.res) {
      setRefreshTokenCookie(req.res, res.credentials.refresh_token);
    }

    return { idToken: res.credentials.id_token };
  }
}
