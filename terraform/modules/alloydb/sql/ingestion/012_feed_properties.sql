-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
CREATE TABLE IF NOT EXISTS feed_properties (
    feed_id        UUID PRIMARY KEY REFERENCES feeds(id) ON DELETE CASCADE,
    source_type    TEXT NOT NULL,
    source_feed_id TEXT NOT NULL,
    external_id    TEXT NOT NULL -- The ID used for mapping feed ID within application
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_properties_source_lookup
ON feed_properties(source_type, source_feed_id);
