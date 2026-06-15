-- Demo seed: four feeds with distinct 24h density profiles, for exercising the
-- timeline overview / histogram locally (the default test_data.sql has no
-- segments). All timestamps are relative to now() so the data always lands in
-- the live 24h window, whenever this is applied. Re-runnable: it clears its own
-- segments first. Apply with either:
--   docker compose exec -T postgres psql -U postgres -d postgres < local_dev/seed_demo_segments.sql
-- or mount it after test_data.sql and recreate the postgres volume.
--
-- classification 'OTHER' = silence (consolidated into bundles in the UI);
-- 'SPEECH' = active, given a TRANSCRIPT annotation; a subset of SPEECH segments
-- also get an EVALUATION annotation with decisions, which marks them as alerts.

BEGIN;

-- Four demo feeds (idempotent). source_type 'bcfy_calls' exists in source_types.
INSERT INTO feeds (id, name, source_type, status) VALUES
    ('5eed0001-0000-4000-8000-000000000001', 'seed-sparse', 'bcfy_calls', 'unclaimed'),
    ('5eed0002-0000-4000-8000-000000000002', 'seed-busy', 'bcfy_calls', 'unclaimed'),
    ('5eed0003-0000-4000-8000-000000000003', 'seed-back-to-back', 'bcfy_calls', 'unclaimed'),
    ('5eed0004-0000-4000-8000-000000000004', 'seed-lumpy', 'bcfy_calls', 'unclaimed')
ON CONFLICT (name) DO NOTHING;

INSERT INTO feed_properties (feed_id, source_feed_id) VALUES
    ('5eed0001-0000-4000-8000-000000000001', 'seed-sparse'),
    ('5eed0002-0000-4000-8000-000000000002', 'seed-busy'),
    ('5eed0003-0000-4000-8000-000000000003', 'seed-back-to-back'),
    ('5eed0004-0000-4000-8000-000000000004', 'seed-lumpy')
ON CONFLICT (feed_id) DO NOTHING;

-- Clear any prior seed run (annotations cascade on segment delete).
DELETE FROM audio_segments
WHERE feed_id IN (
    '5eed0001-0000-4000-8000-000000000001',
    '5eed0002-0000-4000-8000-000000000002',
    '5eed0003-0000-4000-8000-000000000003',
    '5eed0004-0000-4000-8000-000000000004'
);

-- Feed 1 — SPARSE: 200 clips of 2-10s scattered randomly across the 24h.
INSERT INTO audio_segments (id, feed_id, classification, start_timestamp, end_timestamp)
SELECT
    gen_random_uuid(),
    '5eed0001-0000-4000-8000-000000000001',
    (CASE WHEN random() < 0.3 THEN 'OTHER' ELSE 'SPEECH' END)::audio_classification,
    s.start_ts,
    s.start_ts + make_interval(secs => 2 + random() * 8)
FROM (
    SELECT now() - make_interval(secs => random() * 86400) AS start_ts
    FROM generate_series(1, 200)
) s;

-- Feed 2 — BUSY: 3000 clips of 2-60s scattered randomly across the 24h.
INSERT INTO audio_segments (id, feed_id, classification, start_timestamp, end_timestamp)
SELECT
    gen_random_uuid(),
    '5eed0002-0000-4000-8000-000000000002',
    (CASE WHEN random() < 0.25 THEN 'OTHER' ELSE 'SPEECH' END)::audio_classification,
    s.start_ts,
    s.start_ts + make_interval(secs => 2 + random() * 58)
FROM (
    SELECT now() - make_interval(secs => random() * 86400) AS start_ts
    FROM generate_series(1, 3000)
) s;

-- Feed 3 — BACK-TO-BACK: contiguous 45s clips wall-to-wall across the 24h
-- (86400 / 45 = 1920 clips, no gaps). Silence and speech interleave in runs.
INSERT INTO audio_segments (id, feed_id, classification, start_timestamp, end_timestamp)
SELECT
    gen_random_uuid(),
    '5eed0003-0000-4000-8000-000000000003',
    (CASE WHEN random() < 0.25 THEN 'OTHER' ELSE 'SPEECH' END)::audio_classification,
    now() - make_interval(secs => 86400) + make_interval(secs => (i - 1) * 45),
    now() - make_interval(secs => 86400) + make_interval(secs => i * 45)
FROM generate_series(1, 1920) AS i;

-- Feed 4 — LUMPY: busy in the early morning (05-08) and early evening (17-20),
-- sparse otherwise. Hour-of-day taken in Pacific time so the busy stretches land
-- at local morning/evening on the mini-map. ~30s candidate grid, kept by hour.
INSERT INTO audio_segments (id, feed_id, classification, start_timestamp, end_timestamp)
SELECT
    gen_random_uuid(),
    '5eed0004-0000-4000-8000-000000000004',
    (CASE WHEN random() < 0.3 THEN 'OTHER' ELSE 'SPEECH' END)::audio_classification,
    c.start_ts,
    c.start_ts + make_interval(secs => 2 + random() * 40)
FROM (
    SELECT now() - make_interval(secs => g * 30) AS start_ts
    FROM generate_series(1, 2880) AS g
) c
WHERE random() < (
    CASE
        WHEN EXTRACT(hour FROM c.start_ts AT TIME ZONE 'America/Los_Angeles') BETWEEN 5 AND 7 THEN 0.85
        WHEN EXTRACT(hour FROM c.start_ts AT TIME ZONE 'America/Los_Angeles') BETWEEN 17 AND 19 THEN 0.85
        ELSE 0.06
    END
);

-- A transcript on every SPEECH segment so the list renders real text.
INSERT INTO annotations (audio_segment_id, type, data)
SELECT
    a.id,
    'TRANSCRIPT',
    jsonb_build_object(
        'text',
        (ARRAY[
            'Engine 21 responding to the structure fire on Maple Street.',
            'Dispatch, show us on scene, heavy smoke showing.',
            'Copy that, we have water on the fire.',
            'All units clear, returning to quarters.',
            'Requesting a second alarm for the warehouse.',
            'Air support is overhead, dropping on the north flank.'
        ])[1 + floor(random() * 6)::int],
        'errors', '[]'::jsonb
    )
FROM audio_segments a
WHERE a.feed_id IN (
    '5eed0001-0000-4000-8000-000000000001',
    '5eed0002-0000-4000-8000-000000000002',
    '5eed0003-0000-4000-8000-000000000003',
    '5eed0004-0000-4000-8000-000000000004'
)
AND a.classification = 'SPEECH'
ON CONFLICT (audio_segment_id, type) DO NOTHING;

-- Mark ~12% of SPEECH segments as alerts (EVALUATION with a decision). Silence
-- bundles are never alerts.
INSERT INTO annotations (audio_segment_id, type, data)
SELECT
    a.id,
    'EVALUATION',
    jsonb_build_object(
        'decisions',
        jsonb_build_array(
            (ARRAY[
                'Wildfire keyword match: smoke showing',
                'Structure fire detected',
                'Evacuation language detected'
            ])[1 + floor(random() * 3)::int]
        ),
        'errors', '[]'::jsonb
    )
FROM audio_segments a
WHERE a.feed_id IN (
    '5eed0001-0000-4000-8000-000000000001',
    '5eed0002-0000-4000-8000-000000000002',
    '5eed0003-0000-4000-8000-000000000003',
    '5eed0004-0000-4000-8000-000000000004'
)
AND a.classification = 'SPEECH'
AND random() < 0.12
ON CONFLICT (audio_segment_id, type) DO NOTHING;

COMMIT;
