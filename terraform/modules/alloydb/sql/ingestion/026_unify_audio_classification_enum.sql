-- Implement unified AudioClassification enum options and migrate historical records.

DO $$
BEGIN
    ALTER TYPE audio_classification ADD VALUE IF NOT EXISTS 'UNSPECIFIED';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TYPE audio_classification ADD VALUE IF NOT EXISTS 'SPEECH';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TYPE audio_classification ADD VALUE IF NOT EXISTS 'OTHER';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- Migrate existing historical rows to the new canonical enumeration terms
UPDATE audio_segments
SET classification = 'SPEECH'
WHERE classification = 'SPEECH_DETECTED';

UPDATE audio_segments
SET classification = 'OTHER'
WHERE classification = 'UNCLASSIFIED';
