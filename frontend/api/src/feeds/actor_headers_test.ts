import type * as express from 'express';

import { describe, expect, it, vi } from 'vitest';

import { feedMutationActorHeaders } from './actor_headers.js';

vi.mock('../config.js', () => ({
  AUTH_BACKEND: 'google',
  FEEDS_STORE_API_URL: 'http://feeds-api.example.com',
}));

function requestWithSub(sub: unknown): express.Request {
  return {
    user: { isAdmin: true, sub },
  } as unknown as express.Request;
}

describe('feedMutationActorHeaders', () => {
  it('returns actor header from authenticated user sub', () => {
    expect(feedMutationActorHeaders(requestWithSub('admin-sub-123'))).toEqual({
      'X-WD-Actor-Id': 'user:google:admin-sub-123',
    });
  });

  it.each([
    ['missing user', {}],
    ['missing sub', { user: { isAdmin: true } }],
    ['empty sub', requestWithSub('')],
    ['blank sub', requestWithSub('   ')],
    ['space in sub', requestWithSub('admin sub')],
    ['leading whitespace', requestWithSub(' admin-sub-123')],
    ['trailing whitespace', requestWithSub('admin-sub-123 ')],
    ['newline in sub', requestWithSub('admin\nsub')],
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
