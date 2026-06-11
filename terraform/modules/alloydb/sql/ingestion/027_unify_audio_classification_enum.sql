-- AUTOCOMMIT
-- Implement unified AudioClassification enum options and migrate historical records.

-- Idempotent type additions: psql (in Cloud Run migration jobs) executes these outside
-- a transaction block. Python test setup runners (using asyncpg) filter these out
-- because 022 already defines the complete canonical enum for fresh test databases.

ALTER TYPE audio_classification ADD VALUE IF NOT EXISTS 'UNSPECIFIED';
ALTER TYPE audio_classification ADD VALUE IF NOT EXISTS 'SPEECH';
ALTER TYPE audio_classification ADD VALUE IF NOT EXISTS 'OTHER';

-- Migrate existing historical rows to the new canonical enumeration terms
UPDATE audio_segments
SET classification = 'SPEECH'
WHERE classification = 'SPEECH_DETECTED';

UPDATE audio_segments
SET classification = 'OTHER'
WHERE classification = 'UNCLASSIFIED';
