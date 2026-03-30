-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
CREATE TABLE IF NOT EXISTS feed_properties (
    feed_id     UUID PRIMARY KEY REFERENCES feeds(id) ON DELETE CASCADE,
    source_feed_id  TEXT NOT NULL,
    external_id TEXT NOT NULL
);
