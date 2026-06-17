import axios from 'axios';
import { beforeEach, describe, expect, it, vi } from 'vitest';

// Now import checkIsAdmin and WORKSPACE_ADMIN_GROUP_EMAIL
import { checkIsAdmin } from './config.js';

vi.hoisted(() => {
  process.env.ALLOWED_ORIGIN = 'http://localhost:3000';
  process.env.TRANSCRIPTS_API_URL = 'http://localhost:3001';
  process.env.RULES_API_URL = 'http://localhost:3002';
  process.env.FEEDS_STORE_API_URL = 'http://localhost:3003';
  process.env.AUDIO_SEGMENTS_API_URL = 'http://localhost:3004';
  process.env.PROJECT_ID = 'test-project';
  process.env.API_PUBLIC_URL = 'http://localhost:3005';
  process.env.GOOGLE_AUTH_CLIENT_ID = 'client-id';
  process.env.GOOGLE_AUTH_CLIENT_SECRET = 'client-secret';
  process.env.WORKSPACE_ADMIN_GROUP_EMAIL = 'admin-group@example.com';
});

const mockGetAccessToken = vi.fn();
const mockGetClient = vi.fn().mockResolvedValue({
  getAccessToken: mockGetAccessToken,
});

vi.mock('google-auth-library', () => {
  class MockGoogleAuth {
    scopes: string[] = [];
    constructor(options: { scopes: string[] }) {
      this.scopes = options.scopes;
    }
    getClient = mockGetClient;
  }
  return {
    GoogleAuth: MockGoogleAuth,
  };
});

vi.mock('axios');

const mockAxiosGet = vi.mocked(axios.get);

describe('checkIsAdmin', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should return true if membership is verified and cache the result', async () => {
    mockGetAccessToken.mockResolvedValueOnce({ token: 'mock-token' });

    // Step 1: lookup group name response
    mockAxiosGet.mockResolvedValueOnce({
      data: { name: 'groups/resolved-group-id' },
    });

    // Step 2: checkTransitiveMembership response
    mockAxiosGet.mockResolvedValueOnce({
      data: { hasMembership: true },
    });

    const result = await checkIsAdmin('test@example.com');
    expect(result).toBe(true);

    expect(mockGetClient).toHaveBeenCalledTimes(1);
    expect(mockGetAccessToken).toHaveBeenCalledTimes(1);
    expect(mockAxiosGet).toHaveBeenCalledTimes(2);

    expect(mockAxiosGet).toHaveBeenNthCalledWith(
      1,
      'https://cloudidentity.googleapis.com/v1/groups:lookup?groupKey.id=admin-group%40example.com',
      { headers: { Authorization: 'Bearer mock-token' } }
    );

    expect(mockAxiosGet).toHaveBeenNthCalledWith(
      2,
      'https://cloudidentity.googleapis.com/v1/groups/resolved-group-id/memberships:checkTransitiveMembership',
      {
        headers: { Authorization: 'Bearer mock-token' },
        params: { query: "member_key_id == 'test@example.com'" },
      }
    );

    // Call again to verify cache (should not call APIs)
    const secondResult = await checkIsAdmin('test@example.com');
    expect(secondResult).toBe(true);
    expect(mockAxiosGet).toHaveBeenCalledTimes(2); // Still 2 calls, not 4
  });

  it('should return false if membership is not verified', async () => {
    mockGetAccessToken.mockResolvedValueOnce({ token: 'mock-token-2' });

    mockAxiosGet.mockResolvedValueOnce({
      data: { hasMembership: false },
    });

    const result = await checkIsAdmin('non-admin@example.com');
    expect(result).toBe(false);

    expect(mockAxiosGet).toHaveBeenCalledTimes(1); // Only checkTransitiveMembership called
    expect(mockAxiosGet).toHaveBeenCalledWith(
      'https://cloudidentity.googleapis.com/v1/groups/resolved-group-id/memberships:checkTransitiveMembership',
      {
        headers: { Authorization: 'Bearer mock-token-2' },
        params: { query: "member_key_id == 'non-admin@example.com'" },
      }
    );
  });

  it('should return false (fail closed) if Cloud Identity API throws an error', async () => {
    mockGetAccessToken.mockResolvedValueOnce({ token: 'mock-token-3' });
    mockAxiosGet.mockRejectedValueOnce(new Error('Cloud Identity API error'));

    const result = await checkIsAdmin('error-user@example.com');
    expect(result).toBe(false);
  });
});
