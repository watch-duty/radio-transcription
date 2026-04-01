-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
CREATE TABLE IF NOT EXISTS feed_properties_echo (
    feed_id      UUID PRIMARY KEY REFERENCES feeds(id) ON DELETE CASCADE,
    channel_name TEXT NOT NULL  -- full first GCS path component, e.g. 'fire-ca_almaden_valley'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_properties_echo_channel
ON feed_properties_echo(channel_name);
