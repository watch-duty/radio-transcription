import axios from 'axios';
import { GoogleAuth } from 'google-auth-library';

/**
 * Environment variables for the API. Keeping this in a centralized file to
 * alert at build time if an environment variable is missing.
 */

const allowedOrigin = process.env.ALLOWED_ORIGIN;
const transcriptsApiUrl = process.env.TRANSCRIPTS_API_URL;
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

if (!transcriptsApiUrl) {
  throw new Error('TRANSCRIPTS_API_URL environment variable is not set');
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
export const TRANSCRIPTS_API_URL = transcriptsApiUrl;
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
export const WORKSPACE_IMPERSONATION_EMAIL =
  process.env.WORKSPACE_IMPERSONATION_EMAIL;

// Cache structure for user admin status
const adminCache = new Map<string, { isAdmin: boolean; expiresAt: number }>();
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes cache TTL

export async function checkIsAdmin(email: string): Promise<boolean> {
  const normalizedEmail = email.trim().toLowerCase();
  const now = Date.now();

  // Check cache first
  const cached = adminCache.get(normalizedEmail);
  if (cached && cached.expiresAt > now) {
    return cached.isAdmin;
  }

  if (!WORKSPACE_ADMIN_GROUP_EMAIL) {
    console.warn(
      'WORKSPACE_ADMIN_GROUP_EMAIL environment variable is not set. Defaulting to granting admin access to all authenticated users.'
    );
    return true;
  }

  if (!WORKSPACE_IMPERSONATION_EMAIL) {
    console.error(
      'WORKSPACE_IMPERSONATION_EMAIL environment variable is not set, but WORKSPACE_ADMIN_GROUP_EMAIL is configured. Cannot perform group membership lookup.'
    );
    return false;
  }

  try {
    const auth = new GoogleAuth({
      scopes: [
        'https://www.googleapis.com/auth/admin.directory.group.member.readonly',
      ],
      clientOptions: {
        subject: WORKSPACE_IMPERSONATION_EMAIL, // Impersonate the workspace admin
      },
    });

    const client = await auth.getClient();
    const tokenResponse = await client.getAccessToken();
    const token = tokenResponse.token;

    if (!token) {
      throw new Error('Failed to obtain Google OAuth token');
    }

    const url = `https://admin.googleapis.com/admin/directory/v1/groups/${encodeURIComponent(
      WORKSPACE_ADMIN_GROUP_EMAIL
    )}/hasMember/${encodeURIComponent(normalizedEmail)}`;

    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${token}` },
    });

    const isAdmin = !!response.data.isMember;

    // Save in cache
    adminCache.set(normalizedEmail, {
      isAdmin,
      expiresAt: Date.now() + CACHE_TTL_MS,
    });

    return isAdmin;
  } catch (error: any) {
    // Note: the hasMember API returns 404 if the user is not found or not a member.
    if (error.response && error.response.status === 404) {
      adminCache.set(normalizedEmail, {
        isAdmin: false,
        expiresAt: Date.now() + CACHE_TTL_MS,
      });
      return false;
    }

    console.error(
      `Error querying Google Directory API for ${normalizedEmail}:`,
      error.message || error
    );

    // Fail closed, or fallback to expired cache entry if available
    return cached ? cached.isAdmin : false;
  }
}
