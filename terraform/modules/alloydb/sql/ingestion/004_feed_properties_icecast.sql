-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
-- DEPRECATED in favor of feed_properties
CREATE TABLE IF NOT EXISTS feed_properties_icecast (
    feed_id    UUID PRIMARY KEY REFERENCES feeds(id) ON DELETE CASCADE,
    stream_url TEXT NOT NULL
);
