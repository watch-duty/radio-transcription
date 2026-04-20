-- Add source_type to feed_properties for composite uniqueness per source.
ALTER TABLE feed_properties ADD COLUMN source_type TEXT NOT NULL DEFAULT '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_properties_source_lookup
ON feed_properties(source_type, source_feed_id);
