import * as fs from 'fs';

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

// Keep track of cached emails, last fetch timestamp, and default TTL (10 seconds)
let cachedAdminEmails: string[] | null = null;
let lastCacheFetch = 0;
const CACHE_TTL_MS = 15000; // 15 seconds

export async function getAdminEmails(): Promise<string[]> {
  const now = Date.now();
  if (cachedAdminEmails && now - lastCacheFetch < CACHE_TTL_MS) {
    return cachedAdminEmails;
  }

  const adminEmailsPath = process.env.ADMIN_EMAILS_PATH;
  if (adminEmailsPath) {
    try {
      if (fs.existsSync(adminEmailsPath)) {
        const content = await fs.promises.readFile(adminEmailsPath, 'utf8');
        const emails = content
          .split(',')
          .map((email) => email.trim().toLowerCase())
          .filter((email) => email.length > 0);

        cachedAdminEmails = emails;
        lastCacheFetch = now;
        return emails;
      }
    } catch (err) {
      console.error(
        `Error reading admin emails secret file at ${adminEmailsPath}:`,
        err
      );
    }
  }

  lastCacheFetch = now;
  // If no file was found, return an empty list.
  return [];
}
