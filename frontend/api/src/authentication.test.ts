import type * as express from 'express';

import * as jose from 'jose';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { expressAuthentication } from './authentication.js';

const { mockState } = vi.hoisted(() => ({
  mockState: {
    adminEmails: ['admin1@example.com', 'admin2@example.com'],
  },
}));

vi.mock('./config.js', () => ({
  checkIsAdmin: vi.fn((email: string) =>
    Promise.resolve(mockState.adminEmails.includes(email.toLowerCase()))
  ),
}));

vi.mock('jose', () => ({
  decodeJwt: vi.fn(),
}));

describe('expressAuthentication', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should reject if security name is not google_id_token', async () => {
    const mockReq = {
      headers: {
        authorization: 'Bearer valid-token',
      },
    } as unknown as express.Request;

    await expect(
      expressAuthentication(mockReq, 'unknown_security')
    ).rejects.toThrow('Unknown security name: unknown_security');
  });

  it('should reject if no authorization header is provided', async () => {
    const mockReq = {
      headers: {},
    } as unknown as express.Request;

    await expect(
      expressAuthentication(mockReq, 'google_id_token')
    ).rejects.toThrow('No authorization header provided');
  });

  it('should reject if authorization header is empty or missing token', async () => {
    const mockReq = {
      headers: {
        authorization: 'Bearer ',
      },
    } as unknown as express.Request;

    await expect(
      expressAuthentication(mockReq, 'google_id_token')
    ).rejects.toThrow('No token provided in Authorization header');
  });

  it('should reject if JWT token is invalid or cannot be decoded', async () => {
    const mockReq = {
      headers: {
        authorization: 'Bearer invalid-jwt-token',
      },
    } as unknown as express.Request;

    vi.mocked(jose.decodeJwt).mockReturnValueOnce(
      null as unknown as jose.JWTPayload
    );

    await expect(
      expressAuthentication(mockReq, 'google_id_token')
    ).rejects.toThrow('Invalid JWT token');
  });

  it('should set isAdmin to true if the user email is in ADMIN_EMAILS', async () => {
    const mockReq = {
      headers: {
        authorization: 'Bearer tokenXYZ',
      },
    } as unknown as express.Request;

    const decodedPayload = {
      email: 'ADMIN1@example.com', // test case insensitivity (since config parses emails lowercased)
      email_verified: true,
      sub: 'google-id-123',
      aud: 'audience-client-id',
      iss: 'https://accounts.google.com',
    };

    vi.mocked(jose.decodeJwt).mockReturnValueOnce(
      decodedPayload as unknown as jose.JWTPayload
    );

    const user = await expressAuthentication(mockReq, 'google_id_token');

    expect(user).toEqual({
      ...decodedPayload,
      isAdmin: true,
    });
  });

  it('should set isAdmin to false if the user email is NOT in ADMIN_EMAILS', async () => {
    const mockReq = {
      headers: {
        authorization: 'Bearer tokenXYZ',
      },
    } as unknown as express.Request;

    const decodedPayload = {
      email: 'regularuser@example.com',
      email_verified: true,
      sub: 'google-id-456',
      aud: 'audience-client-id',
      iss: 'https://accounts.google.com',
    };

    vi.mocked(jose.decodeJwt).mockReturnValueOnce(
      decodedPayload as unknown as jose.JWTPayload
    );

    const user = await expressAuthentication(mockReq, 'google_id_token');

    expect(user).toEqual({
      ...decodedPayload,
      isAdmin: false,
    });
  });

  it('should set isAdmin to false if email is missing in the decoded token', async () => {
    const mockReq = {
      headers: {
        authorization: 'Bearer tokenXYZ',
      },
    } as unknown as express.Request;

    const decodedPayload = {
      email_verified: true,
      sub: 'google-id-789',
      aud: 'audience-client-id',
      iss: 'https://accounts.google.com',
    };

    vi.mocked(jose.decodeJwt).mockReturnValueOnce(
      decodedPayload as unknown as jose.JWTPayload
    );

    const user = await expressAuthentication(mockReq, 'google_id_token');

    expect(user).toEqual({
      ...decodedPayload,
      isAdmin: false,
    });
  });

  it('should decode from x-apigateway-api-userinfo when present', async () => {
    const payload = {
      email: 'admin1@example.com',
      email_verified: true,
      sub: 'google-id-123',
      aud: 'audience-client-id',
      iss: 'https://accounts.google.com',
    };
    const encodedPayload = Buffer.from(JSON.stringify(payload)).toString(
      'base64url'
    );

    const mockReq = {
      headers: {
        'x-apigateway-api-userinfo': encodedPayload,
      },
    } as unknown as express.Request;

    const user = await expressAuthentication(mockReq, 'google_id_token');

    expect(user).toEqual({
      ...payload,
      isAdmin: true,
    });
  });

  it('should reject if x-apigateway-api-userinfo contains invalid json', async () => {
    const mockReq = {
      headers: {
        'x-apigateway-api-userinfo': 'invalid-base64url-string',
      },
    } as unknown as express.Request;

    await expect(
      expressAuthentication(mockReq, 'google_id_token')
    ).rejects.toThrow('Failed to parse gateway userinfo header');
  });
});
