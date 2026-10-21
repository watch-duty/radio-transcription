-- Idempotent: ON CONFLICT DO NOTHING skips rows that already exist,
-- allowing safe re-application during Terraform runs.
--
-- IMPORTANT: The slugs here must be kept in sync with the SourceType StrEnum
-- in backend/pipeline/storage/feed_store.py.
-- When adding a new source type, add a row here AND a new enum member there.
INSERT INTO source_types (slug, description) VALUES
    ('bcfy_feeds', 'Broadcastify Icecast live streams'),
    ('generic_icecast', 'Self-hosted Icecast live streams (anonymous)'),
    ('bcfy_calls', 'Broadcastify API-polled call recordings'),
    ('echo', 'Watch Duty Echo device recordings'),
    ('openmhz', 'OpenMHZ radio system call recordings'),
    ('fire_notifications', 'Fire Notifications HTTP polling')
ON CONFLICT (slug) DO NOTHING;
