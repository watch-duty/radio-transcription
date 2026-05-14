INSERT INTO feeds (id, name, source_type, status) VALUES
    ('12345678-1234-5678-1234-567812345678', 'mock-icecast-feed', 'bcfy_feeds', 'unclaimed')
ON CONFLICT (name) DO NOTHING;

INSERT INTO feed_properties (feed_id, source_feed_id, external_id) VALUES
    ('12345678-1234-5678-1234-567812345678', 'test-feed', 'ext-test-feed')
ON CONFLICT (feed_id) DO NOTHING;
