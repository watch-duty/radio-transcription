-- Idempotent: ON CONFLICT DO NOTHING skips rows that already exist,
-- allowing safe re-application during Terraform runs.
-- Note: Updates to this file should be in sync with the source_types.proto definition.
INSERT INTO source_types (slug, description) VALUES
    ('BCFY_FEEDS', 'Broadcastify Icecast live streams'),
    ('BCFY_CALLS', 'Broadcastify API-polled call recordings'),
    ('FIRE_NOTIFICATIONS', 'Fire department dispatch alerts'),
    ('OPEN_MHZ', 'Police and Fire radios from OpenMHz')
ON CONFLICT (slug) DO NOTHING;
