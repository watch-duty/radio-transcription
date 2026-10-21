-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
-- Add source_type to feed_properties for composite uniqueness per source.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'feed_properties' AND column_name = 'source_type'
    ) THEN
        ALTER TABLE feed_properties ADD COLUMN source_type TEXT NOT NULL DEFAULT '';
    END IF;
END $$;
-- Temporarily dropped for a load test (#1017), restored by revert (#1080).
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_properties_source_lookup
ON feed_properties(source_type, source_feed_id);
