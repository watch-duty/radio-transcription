/**
 * Environment variables for the API. Keeping this in a centralized file to
 * alert at build time if an environment variable is missing.
 */

export const WEB_URL = process.env.WEB_URL;
export const TRANSCRIPTS_API_URL = process.env.TRANSCRIPTS_API_URL;
export const RULES_API_URL = process.env.RULES_API_URL;

if (!WEB_URL) {
  throw new Error('WEB_URL environment variable is not set');
}

if (!TRANSCRIPTS_API_URL) {
  throw new Error('TRANSCRIPTS_API_URL environment variable is not set');
}

if (!RULES_API_URL) {
  throw new Error('RULES_API_URL environment variable is not set');
}
