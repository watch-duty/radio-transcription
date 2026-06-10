-- Implement unified AudioClassification enum options and migrate historical records.

-- In brand new environments and local/CI test databases, 022 creates the complete
-- canonical enum. For legacy production environments where 022 only created legacy options,
-- Terraform migration runners execute the individual ALTER TYPE statements in an autocommit session:
--
-- ALTER TYPE audio_classification ADD VALUE IF NOT EXISTS 'UNSPECIFIED';
-- ALTER TYPE audio_classification ADD VALUE IF NOT EXISTS 'SPEECH';
-- ALTER TYPE audio_classification ADD VALUE IF NOT EXISTS 'OTHER';

-- Migrate existing historical rows to the new canonical enumeration terms
UPDATE audio_segments
SET classification = 'SPEECH'
WHERE classification = 'SPEECH_DETECTED';

UPDATE audio_segments
SET classification = 'OTHER'
WHERE classification = 'UNCLASSIFIED';
