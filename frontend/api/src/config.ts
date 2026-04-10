/**
 * Environment variables for the API. Keeping this in a centralized file to
 * alert at build time if an environment variable is missing.
 */

export const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN;
export const TRANSCRIPTS_API_URL = process.env.FRONTEND_TRANSCRIPTS_API_URL || process.env.TRANSCRIPTS_API_URL;
export const RULES_API_URL = process.env.FRONTEND_RULES_API_URL || process.env.RULES_API_URL;

if (!ALLOWED_ORIGIN) {
  throw new Error('ALLOWED_ORIGIN environment variable is not set');
}

if (!TRANSCRIPTS_API_URL) {
  throw new Error('TRANSCRIPTS_API_URL environment variable is not set');
}

if (!RULES_API_URL) {
  throw new Error('RULES_API_URL environment variable is not set');
}
