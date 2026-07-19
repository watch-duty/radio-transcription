\set ON_ERROR_STOP on

-- Safe while legacy Feed workers are live. This operation only creates
-- dormant SID Lease identities and never mutates a child Feed.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- SID Lease identities are the only rows mutated by this operation.
LOCK TABLE public.ingestion_leases IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMPORARY TABLE bcfy_calls_cutover_rows ON COMMIT DROP AS
SELECT
    feeds.id AS feed_id,
    feeds.status::TEXT AS feed_status,
    properties.feed_id AS property_feed_id,
    properties.source_type AS property_source_type,
    properties.source_feed_id,
    properties.bcfy_calls_sid AS sid,
    properties.bcfy_calls_group_id AS group_id,
    properties.bcfy_calls_is_trunked
FROM public.feeds AS feeds
LEFT JOIN public.feed_properties AS properties
  ON properties.feed_id = feeds.id
WHERE feeds.source_type = 'bcfy_calls'
ORDER BY feeds.id;

DO $preflight$
DECLARE
    duplicate_count BIGINT;
    invalid_count BIGINT;
BEGIN
    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_cutover_rows AS calls
     WHERE calls.property_feed_id IS NULL
        OR calls.property_source_type IS DISTINCT FROM 'bcfy_calls'
        OR calls.source_feed_id IS NULL
        OR calls.source_feed_id !~ '^[0-9]+-[0-9]+$'
        OR calls.bcfy_calls_is_trunked IS DISTINCT FROM TRUE
        OR calls.sid IS NULL
        OR calls.sid !~ '^[0-9]+$'
        OR calls.group_id IS NULL
        OR calls.group_id !~ '^[0-9]+$'
        OR calls.source_feed_id IS DISTINCT FROM
           calls.sid || '-' || calls.group_id;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'Calls identity projection is missing or invalid for % Feeds',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM public.feed_properties AS properties
      LEFT JOIN public.feeds AS feeds ON feeds.id = properties.feed_id
     WHERE properties.source_type = 'bcfy_calls'
       AND (
           feeds.id IS NULL
           OR feeds.source_type IS DISTINCT FROM 'bcfy_calls'
           OR properties.source_feed_id IS NULL
           OR properties.source_feed_id !~ '^[0-9]+-[0-9]+$'
           OR properties.bcfy_calls_is_trunked IS DISTINCT FROM TRUE
           OR properties.bcfy_calls_sid IS NULL
           OR properties.bcfy_calls_sid !~ '^[0-9]+$'
           OR properties.bcfy_calls_group_id IS NULL
           OR properties.bcfy_calls_group_id !~ '^[0-9]+$'
           OR properties.source_feed_id IS DISTINCT FROM
              properties.bcfy_calls_sid || '-' ||
              properties.bcfy_calls_group_id
       );

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'Calls properties contain % orphaned or invalid rows',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO duplicate_count
      FROM (
          SELECT sid, group_id
          FROM bcfy_calls_cutover_rows
          GROUP BY sid, group_id
          HAVING COUNT(*) > 1
      ) AS duplicate_routes;

    IF duplicate_count <> 0 THEN
        RAISE EXCEPTION
            'Calls identity projection contains % duplicate SID routes',
            duplicate_count;
    END IF;
END
$preflight$;

CREATE TEMPORARY TABLE bcfy_calls_target_sids ON COMMIT DROP AS
SELECT DISTINCT sid
FROM bcfy_calls_cutover_rows
WHERE feed_status IN ('unclaimed', 'active', 'failing')
ORDER BY sid;

DO $preflight_existing_leases$
DECLARE
    invalid_count BIGINT;
BEGIN
    SELECT COUNT(*)
      INTO invalid_count
      FROM public.ingestion_leases AS leases
      JOIN bcfy_calls_target_sids AS target
        ON target.sid = leases.lease_key
     WHERE leases.source_type = 'bcfy_calls'
       AND (
           leases.status <> 'deactivated'::public.feed_status
           OR leases.worker_id IS NOT NULL
           OR leases.last_heartbeat IS NOT NULL
       );

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'preseed found % target SID Leases outside dormant state',
            invalid_count;
    END IF;
END
$preflight_existing_leases$;

CREATE TEMPORARY TABLE bcfy_calls_inserted_sids ON COMMIT DROP AS
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
        target.sid,
        'deactivated'::public.feed_status,
        NULL,
        NULL
    FROM bcfy_calls_target_sids AS target
    ON CONFLICT (source_type, lease_key) DO NOTHING
    RETURNING lease_key
)
SELECT lease_key AS sid
FROM inserted
ORDER BY lease_key;

DO $postflight$
DECLARE
    invalid_count BIGINT;
BEGIN
    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_target_sids AS target
      LEFT JOIN public.ingestion_leases AS leases
        ON leases.source_type = 'bcfy_calls'
       AND leases.lease_key = target.sid
     WHERE leases.lease_key IS NULL
        OR leases.status <> 'deactivated'::public.feed_status
        OR leases.worker_id IS NOT NULL
        OR leases.last_heartbeat IS NOT NULL;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'preseed failed to establish % dormant SID Leases',
            invalid_count;
    END IF;
END
$postflight$;

SELECT
    (SELECT COUNT(*) FROM bcfy_calls_target_sids) AS sid_count,
    (SELECT COUNT(*) FROM bcfy_calls_cutover_rows) AS feed_count,
    (SELECT COUNT(*) FROM bcfy_calls_inserted_sids) AS inserted_sid_count;

COMMIT;
