import * as express from 'express';

export interface LoginResponse {
  idToken: string;
}

export interface AuthService {
  login(code: string, req: express.Request): Promise<LoginResponse>;
  refresh(req: express.Request): Promise<LoginResponse>;
}
