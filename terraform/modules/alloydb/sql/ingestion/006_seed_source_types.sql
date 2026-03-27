-- Idempotent: ON CONFLICT DO NOTHING skips rows that already exist,
-- allowing safe re-application during Terraform runs.
INSERT INTO source_types (slug, description) VALUES
    ('bcfy_feeds', 'Broadcastify Icecast live streams'),
    ('bcfy_calls', 'Broadcastify API-polled call recordings')
ON CONFLICT (slug) DO NOTHING;
