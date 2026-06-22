import axios from 'axios';
import { GoogleAuth } from 'google-auth-library';

/**
 * Environment variables for the API. Keeping this in a centralized file to
 * alert at build time if an environment variable is missing.
 */

const allowedOrigin = process.env.ALLOWED_ORIGIN;
const rulesApiUrl = process.env.RULES_API_URL;
const feedsStoreApiUrl = process.env.FEEDS_STORE_API_URL;
const audioSegmentsApiUrl = process.env.AUDIO_SEGMENTS_API_URL;
const projectId = process.env.PROJECT_ID;
const apiPublicUrl = process.env.API_PUBLIC_URL;
const googleClientId = process.env.GOOGLE_AUTH_CLIENT_ID;
const googleClientSecret = process.env.GOOGLE_AUTH_CLIENT_SECRET;

if (!allowedOrigin) {
  throw new Error('ALLOWED_ORIGIN environment variable is not set');
}

if (!rulesApiUrl) {
  throw new Error('RULES_API_URL environment variable is not set');
}

if (!feedsStoreApiUrl) {
  throw new Error('FEEDS_STORE_API_URL environment variable is not set');
}

if (!audioSegmentsApiUrl) {
  throw new Error('AUDIO_SEGMENTS_API_URL environment variable is not set');
}

if (!projectId) {
  console.error('PROJECT_ID environment variable is not set');
}

if (!apiPublicUrl) {
  console.error('API_PUBLIC_URL environment variable is not set');
}

if (!googleClientId) {
  console.error('GOOGLE_CLIENT_ID environment variable is not set');
}

if (!googleClientSecret) {
  console.error('GOOGLE_CLIENT_SECRET environment variable is not set');
}

export const ALLOWED_ORIGIN = allowedOrigin;
export const RULES_API_URL = rulesApiUrl;
export const FEEDS_STORE_API_URL = feedsStoreApiUrl;
export const AUDIO_SEGMENTS_API_URL = audioSegmentsApiUrl;
export const PROJECT_ID = projectId;
export const API_PUBLIC_URL = apiPublicUrl;
export const GOOGLE_AUTH_CLIENT_ID = googleClientId;
export const GOOGLE_AUTH_CLIENT_SECRET = googleClientSecret;
export const AUTH_BACKEND = process.env.AUTH_BACKEND || 'google';

export const WORKSPACE_ADMIN_GROUP_EMAIL =
  process.env.WORKSPACE_ADMIN_GROUP_EMAIL;

// Cache structure for user admin status
const adminCache = new Map<string, { isAdmin: boolean; expiresAt: number }>();
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes cache TTL

// Cache for group resource name (groups/{group_id}) to avoid repeating the lookup
let cachedGroupId: string | null = null;

async function getGoogleAccessToken(): Promise<string> {
  const auth = new GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/cloud-platform'],
  });
  const client = await auth.getClient();
  const tokenResponse = await client.getAccessToken();
  const token = tokenResponse.token;
  if (!token) {
    throw new Error('Failed to obtain Google OAuth token');
  }
  return token;
}

async function getCloudIdentityGroupId(
  groupEmail: string,
  token: string
): Promise<string> {
  const lookupUrl = `https://cloudidentity.googleapis.com/v1/groups:lookup?groupKey.id=${encodeURIComponent(
    groupEmail
  )}`;
  const lookupResponse = await axios.get(lookupUrl, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const groupName = lookupResponse.data.name;
  if (!groupName) {
    throw new Error(`Failed to resolve group ID for ${groupEmail}`);
  }
  return groupName;
}

async function checkCloudIdentityMembership(
  groupId: string,
  memberEmail: string,
  token: string
): Promise<boolean> {
  console.log(`Looking up membership for ${memberEmail} in ${groupId}`);
  const checkUrl = `https://cloudidentity.googleapis.com/v1/${groupId}/memberships:checkTransitiveMembership`;
  const checkResponse = await axios.get(checkUrl, {
    headers: { Authorization: `Bearer ${token}` },
    params: {
      query: `member_key_id == '${memberEmail}'`,
    },
  });
  return !!checkResponse.data.hasMembership;
}

export async function checkIsAdmin(email: string): Promise<boolean> {
  const normalizedEmail = email.trim().toLowerCase();
  const now = Date.now();

  // Check cache first
  const cached = adminCache.get(normalizedEmail);
  if (cached && cached.expiresAt > now) {
    return cached.isAdmin;
  }

  // If no admin group is configured, allow all users access.
  if (!WORKSPACE_ADMIN_GROUP_EMAIL) {
    return true;
  }

  try {
    const token = await getGoogleAccessToken();

    // Step 1: Look up the group unique ID if not already cached
    if (!cachedGroupId) {
      cachedGroupId = await getCloudIdentityGroupId(
        WORKSPACE_ADMIN_GROUP_EMAIL,
        token
      );
    }

    // Step 2: Check transitive membership
    const isAdmin = await checkCloudIdentityMembership(
      cachedGroupId,
      normalizedEmail,
      token
    );

    // Save in cache
    adminCache.set(normalizedEmail, {
      isAdmin,
      expiresAt: Date.now() + CACHE_TTL_MS,
    });

    return isAdmin;
  } catch (error: unknown) {
    const message =
      axios.isAxiosError(error) && error.response?.data
        ? JSON.stringify(error.response.data)
        : error instanceof Error
          ? error.message
          : String(error);

    console.error(
      `Error querying Cloud Identity API for ${normalizedEmail}:`,
      message
    );

    // Fall back to expired cache (stale-on-error) if available to prevent lockout during API outages.
    // Otherwise, deny admin access by defaulting to false.
    return cached ? cached.isAdmin : false;
  }
}
