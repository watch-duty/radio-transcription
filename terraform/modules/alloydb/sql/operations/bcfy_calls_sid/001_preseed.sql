\set ON_ERROR_STOP on

-- Safe while legacy workers remain live: create only dormant parent identity.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Global controlled-mutation order: Lease -> Feed -> properties.
LOCK TABLE public.ingestion_leases IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.feeds IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.feed_properties IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMPORARY TABLE phase7_expected_membership ON COMMIT DROP AS
SELECT
    properties.feed_id,
    properties.bcfy_calls_sid AS sid,
    properties.bcfy_calls_group_id AS group_id,
    feeds.status::TEXT AS status,
    feeds.fencing_token
FROM public.feed_properties AS properties
JOIN public.feeds AS feeds ON feeds.id = properties.feed_id
WHERE properties.source_type = 'bcfy_calls'
  AND feeds.source_type = 'bcfy_calls'
  AND properties.bcfy_calls_is_trunked IS TRUE
  AND properties.bcfy_calls_sid IS NOT NULL
  AND properties.bcfy_calls_group_id IS NOT NULL
  AND feeds.status <> 'deactivated'::public.feed_status
ORDER BY properties.bcfy_calls_sid,
         properties.bcfy_calls_group_id,
         properties.feed_id;

DO $preflight$
DECLARE
    incomplete_count INTEGER;
    duplicate_count INTEGER;
    expected_feed_count INTEGER;
    expected_sid_count INTEGER;
    unsafe_existing_count INTEGER;
BEGIN
    -- This is deliberately separate from the expected-set count.  An empty or
    -- partial maintained projection must never appear to be a valid set.
    SELECT COUNT(*)
      INTO incomplete_count
      FROM public.feeds AS feeds
      LEFT JOIN public.feed_properties AS properties
        ON properties.feed_id = feeds.id
     WHERE feeds.source_type = 'bcfy_calls'
       AND feeds.status <> 'deactivated'::public.feed_status
       AND (
           properties.feed_id IS NULL
           OR properties.source_type <> feeds.source_type
           OR properties.source_feed_id ~ '^[0-9]+-[0-9]+$'
              AND (
                  properties.bcfy_calls_sid IS NULL
                  OR properties.bcfy_calls_group_id IS NULL
                  OR properties.bcfy_calls_is_trunked IS DISTINCT FROM TRUE
                  OR properties.source_feed_id IS DISTINCT FROM
                     properties.bcfy_calls_sid || '-' ||
                     properties.bcfy_calls_group_id
              )
           OR properties.bcfy_calls_is_trunked IS TRUE
              AND (
                  properties.bcfy_calls_sid IS NULL
                  OR properties.bcfy_calls_group_id IS NULL
                  OR properties.source_feed_id IS DISTINCT FROM
                     properties.bcfy_calls_sid || '-' ||
                     properties.bcfy_calls_group_id
              )
       );

    IF incomplete_count <> 0 THEN
        RAISE EXCEPTION
            'enabled Calls projection is absent, partial, or inconsistent for % rows',
            incomplete_count;
    END IF;

    SELECT COUNT(*)
      INTO duplicate_count
      FROM (
          SELECT sid, group_id
          FROM phase7_expected_membership
          GROUP BY sid, group_id
          HAVING COUNT(*) <> 1
      ) AS duplicate_routes;

    IF duplicate_count <> 0 THEN
        RAISE EXCEPTION
            'maintained Calls projection contains % duplicate routes',
            duplicate_count;
    END IF;

    SELECT COUNT(*), COUNT(DISTINCT sid)
      INTO expected_feed_count, expected_sid_count
      FROM phase7_expected_membership;

    IF expected_sid_count <> 19 OR expected_feed_count <> 154 THEN
        RAISE EXCEPTION
            'review baseline mismatch: expected 19 SIDs/154 Feeds, found %/%',
            expected_sid_count,
            expected_feed_count;
    END IF;

    SELECT COUNT(*)
      INTO unsafe_existing_count
      FROM public.ingestion_leases AS leases
      JOIN (
          SELECT DISTINCT sid FROM phase7_expected_membership
      ) AS expected ON expected.sid = leases.lease_key
     WHERE leases.source_type = 'bcfy_calls'
       AND (
           leases.status <> 'deactivated'::public.feed_status
           OR leases.worker_id IS NOT NULL
           OR leases.last_heartbeat IS NOT NULL
       );

    IF unsafe_existing_count <> 0 THEN
        RAISE EXCEPTION
            'pre-seed found % expected Lease rows outside dormant state',
            unsafe_existing_count;
    END IF;
END
$preflight$;

WITH inserted AS (
    INSERT INTO public.ingestion_leases (
        source_type,
        lease_key,
        status,
        worker_id,
        last_heartbeat
    )
    SELECT
        'bcfy_calls',
        expected.sid,
        'deactivated'::public.feed_status,
        NULL,
        NULL
    FROM (
        SELECT DISTINCT sid FROM phase7_expected_membership
    ) AS expected
    ORDER BY expected.sid
    ON CONFLICT (source_type, lease_key) DO NOTHING
    RETURNING lease_key
)
SELECT COUNT(*) AS inserted_lease_count FROM inserted;

DO $postflight$
DECLARE
    expected_sid_count INTEGER;
    dormant_lease_count INTEGER;
BEGIN
    SELECT COUNT(DISTINCT sid)
      INTO expected_sid_count
      FROM phase7_expected_membership;

    SELECT COUNT(*)
      INTO dormant_lease_count
      FROM public.ingestion_leases AS leases
      JOIN (
          SELECT DISTINCT sid FROM phase7_expected_membership
      ) AS expected ON expected.sid = leases.lease_key
     WHERE leases.source_type = 'bcfy_calls'
       AND leases.status = 'deactivated'::public.feed_status
       AND leases.worker_id IS NULL
       AND leases.last_heartbeat IS NULL;

    IF dormant_lease_count <> expected_sid_count THEN
        RAISE EXCEPTION
            'pre-seed postflight found % dormant Leases, expected %',
            dormant_lease_count,
            expected_sid_count;
    END IF;
END
$postflight$;

SELECT
    sid,
    COUNT(*) AS feed_count,
    MAX(fencing_token) AS maximum_child_fence
FROM phase7_expected_membership
GROUP BY sid
ORDER BY sid;

SELECT
    COUNT(DISTINCT sid) AS sid_count,
    COUNT(*) AS feed_count,
    pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to(
                pg_catalog.string_agg(
                    feed_id::TEXT || '|' || sid || '|' || group_id,
                    E'\n' ORDER BY sid, group_id, feed_id
                ),
                'UTF8'
            )
        ),
        'hex'
    ) AS manifest_digest
FROM phase7_expected_membership;

COMMIT;
