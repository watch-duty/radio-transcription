/**
 * Environment variables for the API. Keeping this in a centralized file to
 * alert at build time if an environment variable is missing.
 */

const allowedOrigin = process.env.ALLOWED_ORIGIN;
const transcriptsApiUrl = process.env.TRANSCRIPTS_API_URL;
const rulesApiUrl = process.env.RULES_API_URL;
const feedsStoreApiUrl = process.env.FEEDS_STORE_API_URL;
const projectId = process.env.PROJECT_ID;

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

if (!projectId) {
  console.error('PROJECT_ID environment variable is not set');
}

export const ALLOWED_ORIGIN = allowedOrigin;
export const TRANSCRIPTS_API_URL = transcriptsApiUrl;
export const RULES_API_URL = rulesApiUrl;
export const FEEDS_STORE_API_URL = feedsStoreApiUrl;
export const PROJECT_ID = projectId;
