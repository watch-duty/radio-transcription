import * as express from 'express';

import { UserResponse } from '@transcription/common';
import { OAuth2Client } from 'google-auth-library';
import {
  Body,
  Controller,
  Extension,
  Get,
  Post,
  Request,
  Response,
  Route,
  Security,
  Tags,
} from 'tsoa';

import { AuthenticatedRequest } from '../authentication.js';
import {
  ALLOWED_ORIGIN,
  GOOGLE_AUTH_CLIENT_ID,
  GOOGLE_AUTH_CLIENT_SECRET,
} from '../config.js';
import { HttpError, handleBackendError } from '../utils.js';

interface LoginRequest {
  code: string;
}

interface LoginResponse {
  idToken: string;
}

const client = new OAuth2Client(
  GOOGLE_AUTH_CLIENT_ID,
  GOOGLE_AUTH_CLIENT_SECRET,
  'postmessage'
);

function setRefreshTokenCookie(
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

@Route('api/v1/auth')
@Tags('Auth')
export class AuthController extends Controller {
  @Post('google')
  @Response<{ message: string }>(400, 'Bad Request')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async login(
    @Body() requestBody: LoginRequest,
    @Request() request: express.Request
  ): Promise<LoginResponse> {
    try {
      const { tokens } = await client.getToken(requestBody.code);

      if (tokens.refresh_token && request.res) {
        setRefreshTokenCookie(request.res, tokens.refresh_token);
      }

      if (!tokens.id_token) {
        throw new HttpError(400, 'No ID token returned from Google');
      }

      return { idToken: tokens.id_token };
    } catch (error: unknown) {
      if (error instanceof HttpError) throw error;
      const { status, message } = handleBackendError(error, 'Login failed');
      throw new HttpError(status, message);
    }
  }

  @Get('session')
  @Response<{ message: string }>(400, 'Bad Request')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async refresh(
    @Request() request: express.Request
  ): Promise<LoginResponse> {
    const refreshToken = request.cookies?.refresh_token;

    if (!refreshToken) {
      throw new HttpError(401, 'No refresh token in session');
    }

    try {
      client.setCredentials({ refresh_token: refreshToken });
      const res = await client.refreshAccessToken();

      if (!res.credentials.id_token) {
        throw new HttpError(400, 'Failed to refresh ID token');
      }

      // Rotate token if your authentication system issues fresh token credentials
      if (res.credentials.refresh_token && request.res) {
        setRefreshTokenCookie(request.res, res.credentials.refresh_token);
      }

      return { idToken: res.credentials.id_token };
    } catch (error: unknown) {
      if (error instanceof HttpError) throw error;
      const { status, message } = handleBackendError(
        error,
        'Failed to refresh session'
      );
      throw new HttpError(status, message);
    }
  }

  @Post('logout')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async logout(@Request() request: express.Request): Promise<void> {
    request.res?.clearCookie('refresh_token');
    this.setStatus(204);
  }
}

@Route('api/v1/users')
@Tags('Users')
export class UsersController extends Controller {
  @Get('')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async getUserInfo(
    @Request() request: AuthenticatedRequest
  ): Promise<UserResponse> {
    if (!request.user) {
      throw new HttpError(401, 'Unauthorized');
    }

    return {
      email: request.user.email,
      isAdmin: !!request.user.isAdmin,
    };
  }
}
