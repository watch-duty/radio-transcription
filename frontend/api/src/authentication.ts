import * as express from 'express';

import { GoogleUser } from '@transcription/common';
import * as jose from 'jose';

import { checkIsAdmin } from './config.js';

export { GoogleUser };

export interface AuthenticatedRequest extends express.Request {
  user?: GoogleUser;
}

export async function expressAuthentication(
  request: express.Request,
  securityName: string,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  scopes?: string[]
): Promise<GoogleUser> {
  // Match the name defined in tsoa.json and @Security decorators
  if (securityName === 'google_id_token') {
    const userInfoHeader =
      // Google Cloud API Gateway injects the authenticated user metadata in this header
      request.headers['x-apigateway-api-userinfo'] ||
      // Google Cloud Endpoints / ESPv2 uses this header as an alternative or older name
      request.headers['x-endpoint-api-userinfo'];

    let decoded: GoogleUser;

    if (userInfoHeader) {
      try {
        const decodedPayload = Buffer.from(
          userInfoHeader as string,
          'base64url'
        ).toString('utf8');
        decoded = JSON.parse(decodedPayload);
      } catch (error) {
        console.error('Failed to parse gateway userinfo header:', error);
        return Promise.reject(
          new Error('Failed to parse gateway userinfo header')
        );
      }
    } else {
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
      decoded = jose.decodeJwt<GoogleUser>(token);
    }

    if (!decoded) {
      return Promise.reject(new Error('Invalid JWT token or userinfo'));
    }

    if (!decoded.email) {
      console.warn(
        'Authentication token or userinfo payload does not contain an email address.'
      );
    }

    decoded.isAdmin = decoded.email ? await checkIsAdmin(decoded.email) : false;

    // The resolved value is what gets injected into your controllers
    return Promise.resolve(decoded);
  }

  return Promise.reject(new Error(`Unknown security name: ${securityName}`));
}
