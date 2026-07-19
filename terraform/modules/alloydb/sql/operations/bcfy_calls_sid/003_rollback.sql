\set ON_ERROR_STOP on

\if :{?process_absence_confirmed}
\else
\echo 'process_absence_confirmed psql variable is required'
\quit 3
\endif

-- Run only after every SID worker has stopped. This transaction first fences
-- and deactivates parent authority, then restores legacy child authority.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TEMPORARY TABLE bcfy_calls_rollback_input (
    process_absence_confirmed TEXT NOT NULL
) ON COMMIT DROP;
INSERT INTO bcfy_calls_rollback_input
VALUES (:'process_absence_confirmed');

-- Global controlled-mutation order.
LOCK TABLE public.ingestion_leases IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.feeds IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.feed_properties IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMPORARY TABLE bcfy_calls_cutover_rows ON COMMIT DROP AS
SELECT
    feeds.id AS feed_id,
    feeds.status::TEXT AS feed_status,
    feeds.worker_id,
    feeds.last_heartbeat,
    feeds.fencing_token,
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

CREATE TEMPORARY TABLE bcfy_calls_managed_sids ON COMMIT DROP AS
SELECT
    leases.lease_key AS sid,
    leases.fencing_token,
    leases.membership_revision
FROM public.ingestion_leases AS leases
WHERE leases.source_type = 'bcfy_calls'
  AND leases.status <> 'deactivated'::public.feed_status
ORDER BY leases.lease_key;

-- Every mapped child under a managed SID receives a fence above the retired
-- parent, including failing, quarantined, and deactivated children.
CREATE TEMPORARY TABLE bcfy_calls_structural_membership ON COMMIT DROP AS
SELECT calls.*
FROM bcfy_calls_cutover_rows AS calls
JOIN bcfy_calls_managed_sids AS managed ON managed.sid = calls.sid
ORDER BY calls.sid, calls.group_id, calls.feed_id;

CREATE TEMPORARY TABLE bcfy_calls_lease_before ON COMMIT DROP AS
SELECT
    leases.source_type,
    leases.lease_key,
    leases.fencing_token,
    leases.membership_revision,
    to_jsonb(leases) - ARRAY[
        'status',
        'worker_id',
        'fencing_token',
        'last_heartbeat',
        'membership_revision',
        'updated_at'
    ]::TEXT[] AS preserved_state
FROM public.ingestion_leases AS leases
JOIN bcfy_calls_managed_sids AS managed
  ON managed.sid = leases.lease_key
WHERE leases.source_type = 'bcfy_calls'
ORDER BY leases.lease_key;

CREATE TEMPORARY TABLE bcfy_calls_child_before ON COMMIT DROP AS
SELECT
    feeds.id AS feed_id,
    feeds.status::TEXT AS feed_status,
    feeds.fencing_token,
    to_jsonb(feeds) - ARRAY[
        'status',
        'worker_id',
        'fencing_token',
        'last_heartbeat',
        'unclaimed_since'
    ]::TEXT[] AS preserved_state
FROM public.feeds AS feeds
JOIN bcfy_calls_structural_membership AS membership
  ON membership.feed_id = feeds.id
ORDER BY feeds.id;

DO $preflight$
DECLARE
    confirmation TEXT;
    duplicate_count BIGINT;
    invalid_count BIGINT;
BEGIN
    SELECT process_absence_confirmed
      INTO STRICT confirmation
      FROM bcfy_calls_rollback_input;

    IF confirmation <> 'CONFIRMED' THEN
        RAISE EXCEPTION
            'SID process absence has not been confirmed';
    END IF;

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

    -- A stopped or crashed SID worker may leave stale ownership behind.
    -- Explicit process-absence confirmation authorizes the fenced parent
    -- update below to retire and clear that ownership atomically.
    SELECT COUNT(*)
      INTO invalid_count
      FROM public.ingestion_leases AS leases
      JOIN bcfy_calls_managed_sids AS managed
        ON managed.sid = leases.lease_key
     WHERE leases.source_type = 'bcfy_calls'
       AND (
           leases.fencing_token > 9223372036854775805
           OR leases.membership_revision = 9223372036854775807
       );

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback found % overflow-unsafe SID Leases',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_cutover_rows
     WHERE worker_id IS NOT NULL;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback found % Calls children with current owners',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_managed_sids AS managed
      LEFT JOIN bcfy_calls_structural_membership AS membership
        ON membership.sid = managed.sid
     WHERE membership.feed_id IS NULL;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback found % managed SIDs without structural children',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_cutover_rows AS calls
      LEFT JOIN bcfy_calls_managed_sids AS managed
        ON managed.sid = calls.sid
     WHERE calls.feed_status = 'active'
       AND managed.sid IS NULL;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback found % active children outside SID authority',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_structural_membership
     WHERE fencing_token = 9223372036854775807;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback found % child fences at BIGINT maximum',
            invalid_count;
    END IF;
END
$preflight$;

-- Retire and fence parent authority first. Child fences are derived from the
-- exact values returned by this update.
CREATE TEMPORARY TABLE bcfy_calls_rolled_back_leases ON COMMIT DROP AS
WITH retired AS (
    UPDATE public.ingestion_leases AS leases
    SET status = 'deactivated'::public.feed_status,
        worker_id = NULL,
        last_heartbeat = NULL,
        fencing_token = leases.fencing_token + 1,
        membership_revision = leases.membership_revision + 1,
        updated_at = NOW()
    FROM bcfy_calls_managed_sids AS managed
    WHERE leases.source_type = 'bcfy_calls'
      AND leases.lease_key = managed.sid
      AND leases.status <> 'deactivated'::public.feed_status
    RETURNING
        leases.source_type,
        leases.lease_key,
        leases.fencing_token AS new_parent_fence,
        leases.membership_revision
)
SELECT *
FROM retired
ORDER BY lease_key;

CREATE TEMPORARY TABLE bcfy_calls_rolled_back_children ON COMMIT DROP AS
WITH restored AS (
    UPDATE public.feeds AS feeds
    SET status = CASE
            WHEN feeds.status = 'active'::public.feed_status
                THEN 'unclaimed'::public.feed_status
            ELSE feeds.status
        END,
        worker_id = NULL,
        last_heartbeat = NULL,
        fencing_token = GREATEST(
            feeds.fencing_token,
            retired.new_parent_fence + 1
        ),
        unclaimed_since = CASE
            WHEN feeds.status = 'active'::public.feed_status THEN NOW()
            ELSE feeds.unclaimed_since
        END
    FROM bcfy_calls_structural_membership AS membership
    JOIN bcfy_calls_rolled_back_leases AS retired
      ON retired.lease_key = membership.sid
    WHERE feeds.id = membership.feed_id
    RETURNING feeds.id, membership.sid, feeds.fencing_token
)
SELECT *
FROM restored
ORDER BY sid, id;

DO $postflight$
DECLARE
    expected_count BIGINT;
    invalid_count BIGINT;
    updated_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO expected_count FROM bcfy_calls_managed_sids;
    SELECT COUNT(*) INTO updated_count FROM bcfy_calls_rolled_back_leases;

    IF updated_count <> expected_count THEN
        RAISE EXCEPTION
            'rollback changed % SID Leases, expected %',
            updated_count,
            expected_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_lease_before AS before_state
      JOIN public.ingestion_leases AS leases
        ON leases.source_type = before_state.source_type
       AND leases.lease_key = before_state.lease_key
     WHERE leases.status <> 'deactivated'::public.feed_status
        OR leases.worker_id IS NOT NULL
        OR leases.last_heartbeat IS NOT NULL
        OR leases.fencing_token <> before_state.fencing_token + 1
        OR leases.membership_revision <>
           before_state.membership_revision + 1
        OR to_jsonb(leases) - ARRAY[
               'status',
               'worker_id',
               'fencing_token',
               'last_heartbeat',
               'membership_revision',
               'updated_at'
           ]::TEXT[] IS DISTINCT FROM before_state.preserved_state;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback SID Lease postflight failed for % rows',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_child_before AS before_state
      JOIN public.feeds AS feeds ON feeds.id = before_state.feed_id
      JOIN bcfy_calls_structural_membership AS membership
        ON membership.feed_id = feeds.id
      JOIN bcfy_calls_rolled_back_leases AS retired
        ON retired.lease_key = membership.sid
     WHERE feeds.worker_id IS NOT NULL
        OR feeds.last_heartbeat IS NOT NULL
        OR feeds.fencing_token <= retired.new_parent_fence
        OR feeds.fencing_token < before_state.fencing_token
        OR (
            before_state.feed_status = 'active'
            AND feeds.status <> 'unclaimed'::public.feed_status
        )
        OR (
            before_state.feed_status <> 'active'
            AND feeds.status::TEXT <> before_state.feed_status
        )
        OR to_jsonb(feeds) - ARRAY[
               'status',
               'worker_id',
               'fencing_token',
               'last_heartbeat',
               'unclaimed_since'
           ]::TEXT[] IS DISTINCT FROM before_state.preserved_state;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback child Feed postflight failed for % rows',
            invalid_count;
    END IF;
END
$postflight$;

SELECT
    retired.lease_key AS sid,
    retired.new_parent_fence,
    retired.membership_revision,
    COUNT(children.id) AS structural_feed_count,
    COUNT(children.id) FILTER (
        WHERE feeds.status = 'unclaimed'::public.feed_status
    ) AS legacy_claimable_feed_count
FROM bcfy_calls_rolled_back_leases AS retired
JOIN bcfy_calls_rolled_back_children AS children
  ON children.sid = retired.lease_key
JOIN public.feeds AS feeds ON feeds.id = children.id
GROUP BY
    retired.lease_key,
    retired.new_parent_fence,
    retired.membership_revision
ORDER BY retired.lease_key;

COMMIT;
