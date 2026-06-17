import { Buffer } from 'node:buffer';

import { expect, test } from '@playwright/test';

test.describe('Feeds API Integration Tests', () => {
  test('should reject request with 401 if no auth is provided', async ({
    request,
  }) => {
    const response = await request.get('/api/v1/feeds');
    expect(response.status()).toBe(401);

    const body = await response.json();
    expect(body.message).toBe('No authorization header provided');
  });

  test('should allow listing feeds with a valid user info header', async ({
    request,
  }) => {
    const mockUserPayload = {
      email: 'test-user@watchduty.org',
      email_verified: true,
      sub: 'test-google-id-123',
      aud: 'test-audience',
      iss: 'https://accounts.google.com',
    };

    const base64UrlPayload = Buffer.from(
      JSON.stringify(mockUserPayload)
    ).toString('base64url');

    const response = await request.get('/api/v1/feeds', {
      headers: {
        'x-apigateway-api-userinfo': base64UrlPayload,
      },
    });

    expect(response.status()).toBe(200);

    const data = await response.json();
    if (Array.isArray(data)) {
      expect(Array.isArray(data)).toBe(true);
    } else {
      expect(data).toHaveProperty('feeds');
      expect(Array.isArray(data.feeds)).toBe(true);
    }
  });
});
