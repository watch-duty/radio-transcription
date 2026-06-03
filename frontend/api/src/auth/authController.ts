import * as express from 'express';

import {
  Body,
  Controller,
  Extension,
  Get,
  Post,
  Request,
  Response,
  Route,
  Tags,
} from 'tsoa';

import { AUTH_BACKEND } from '../config.js';
import { HttpError, handleBackendError } from '../utils.js';
import { AuthService } from './authService.js';
import { GoogleAuthService } from './googleAuthService.js';
import { MockAuthService } from './mockAuthService.js';

interface LoginRequest {
  code: string;
}

interface LoginResponse {
  idToken: string;
}

const authService: AuthService = AUTH_BACKEND === 'none'
  ? new MockAuthService()
  : new GoogleAuthService();

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
      return await authService.login(requestBody.code, request);
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
    try {
      return await authService.refresh(request);
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
