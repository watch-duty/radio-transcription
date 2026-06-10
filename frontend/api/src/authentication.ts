import * as express from 'express';

import * as jose from 'jose';

import { checkIsAdmin } from './config.js';

export interface GoogleUser {
  email: string;
  email_verified: boolean;
  sub: string; // The user's unique Google ID
  aud: string; // The client ID or audience
  iss: string; // The issuer (e.g., https://accounts.google.com)
  isAdmin?: boolean; // Special admin user flag
}

export async function expressAuthentication(
  request: express.Request,
  securityName: string,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  scopes?: string[]
): Promise<GoogleUser> {
  // Match the name defined in tsoa.json and @Security decorators
  if (securityName === 'google_id_token') {
    const authHeader = request.headers.authorization;

    if (!authHeader) {
      return Promise.reject(new Error('No authorization header provided'));
    }
    const token = authHeader.split(' ')[1];

    if (!token) {
      return Promise.reject(
        new Error('No token provided in Authorization header')
      );
    }
    // API Gateway already verified the signature at this point
    const decoded = jose.decodeJwt<GoogleUser>(token);

    if (!decoded) {
      return Promise.reject(new Error('Invalid JWT token'));
    }

    if (decoded.email) {
      decoded.isAdmin = await checkIsAdmin(decoded.email);
    } else {
      decoded.isAdmin = false;
    }

    // The resolved value is what gets injected into your controllers
    return Promise.resolve(decoded);
  }

  return Promise.reject(new Error(`Unknown security name: ${securityName}`));
}
