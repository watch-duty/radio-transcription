-- Idempotent migration to rename transmission_id to segment_id in transcripts table.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'transcripts'
          AND column_name = 'transmission_id'
    ) THEN
        ALTER TABLE transcripts RENAME COLUMN transmission_id TO segment_id;
    END IF;
END
$$;
