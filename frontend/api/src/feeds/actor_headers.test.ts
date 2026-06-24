import type * as express from 'express';

import { describe, expect, it, vi } from 'vitest';

import { feedMutationActorHeaders } from './actor_headers.js';

vi.mock('../config.js', () => ({
  AUTH_BACKEND: 'google',
  FEEDS_STORE_API_URL: 'http://feeds-api.example.com',
}));

function requestWithEmail(email: unknown): express.Request {
  return {
    user: { isAdmin: true, email },
  } as unknown as express.Request;
}

describe('feedMutationActorHeaders', () => {
  it('returns actor header from authenticated user email', () => {
    expect(
      feedMutationActorHeaders(requestWithEmail(' Admin@Example.com '))
    ).toEqual({
      'X-WD-Actor-Id': 'user:google:admin@example.com',
    });
  });

  it.each([
    ['missing user', {}],
    ['missing email', { user: { isAdmin: true } }],
    ['empty email', requestWithEmail('')],
    ['blank email', requestWithEmail('   ')],
    ['space in email', requestWithEmail('admin @example.com')],
    ['newline in email', requestWithEmail('admin\n@example.com')],
  ])('throws 403 Forbidden for %s', (_label, request) => {
    try {
      feedMutationActorHeaders(request as express.Request);
      throw new Error('expected helper to throw');
    } catch (error) {
      expect(error).toMatchObject({
        status: 403,
        message: 'Forbidden',
      });
    }
  });
});
