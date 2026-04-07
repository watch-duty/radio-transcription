/**
 * Environment variables for the API. Keeping this in a centralized file to
 * alert at build time if an environment variable is missing.
 */

export const TRANSCRIPTS_API_URL = process.env.TRANSCRIPTS_API_URL;

if (!TRANSCRIPTS_API_URL) {
  throw new Error('TRANSCRIPTS_API_URL environment variable is not set');
}

export const RULES_MANAGEMENT_API_URL = process.env.RULES_MANAGEMENT_API_URL;

if (!RULES_MANAGEMENT_API_URL) {
  throw new Error('RULES_MANAGEMENT_API_URL environment variable is not set');
}
