INSERT INTO feeds (id, name, source_type, status) VALUES
    ('12345678-1234-5678-1234-567812345678', 'mock-icecast-feed', 'bcfy_feeds', 'unclaimed')
ON CONFLICT (name) DO NOTHING;

INSERT INTO feed_properties (feed_id, source_feed_id) VALUES
    ('12345678-1234-5678-1234-567812345678', 'test-feed')
ON CONFLICT (feed_id) DO NOTHING;

INSERT INTO feeds (id, name, source_type, status) VALUES
    ('11111111-2222-3333-4444-555555555555', 'mock-fn-feed', 'fire_notifications', 'unclaimed')
ON CONFLICT (name) DO NOTHING;

INSERT INTO feed_properties (feed_id, source_feed_id) VALUES
    ('11111111-2222-3333-4444-555555555555', 'RECORDINGS/SAN-JOSE-DISP')
ON CONFLICT (feed_id) DO NOTHING;

INSERT INTO feeds (id, name, source_type, status) VALUES
    ('22222222-3333-4444-5555-666666666666', 'mock-bcfy-calls-feed', 'bcfy_calls', 'unclaimed')
ON CONFLICT (name) DO NOTHING;

INSERT INTO feed_properties (feed_id, source_feed_id) VALUES
    ('22222222-3333-4444-5555-666666666666', '2912')
ON CONFLICT (feed_id) DO NOTHING;


INSERT INTO rules (id, rule_name, description, is_active, scope, conditions, created_by) VALUES
    ('87654321-4321-8765-4321-876543210987', 'global_match_all', 'Passthrough rule', true, '{"level": "GLOBAL"}'::jsonb, '{"evaluation_type": "REGEX_MATCH", "expression": ".*", "flags": "i"}'::jsonb, 'local_dev')
ON CONFLICT (id) DO NOTHING;
