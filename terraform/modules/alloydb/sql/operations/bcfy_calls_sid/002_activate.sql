\set ON_ERROR_STOP on

\if :{?process_absence_confirmed}
\else
\echo 'process_absence_confirmed psql variable is required'
\quit 3
\endif

-- Run only after every legacy Feed worker has stopped. The transaction makes
-- the child-to-parent authority handoff atomic.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TEMPORARY TABLE bcfy_calls_activation_input (
    process_absence_confirmed TEXT NOT NULL
) ON COMMIT DROP;
INSERT INTO bcfy_calls_activation_input
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

CREATE TEMPORARY TABLE bcfy_calls_target_sids ON COMMIT DROP AS
SELECT DISTINCT sid
FROM bcfy_calls_cutover_rows
WHERE feed_status IN ('unclaimed', 'active', 'failing')
ORDER BY sid;

-- Every mapped child under an activated SID participates in the fence
-- baseline, including quarantined and deactivated children.
CREATE TEMPORARY TABLE bcfy_calls_structural_membership ON COMMIT DROP AS
SELECT calls.*
FROM bcfy_calls_cutover_rows AS calls
JOIN bcfy_calls_target_sids AS target ON target.sid = calls.sid
ORDER BY calls.sid, calls.group_id, calls.feed_id;

CREATE TEMPORARY TABLE bcfy_calls_child_before ON COMMIT DROP AS
SELECT
    feeds.id AS feed_id,
    feeds.status::TEXT AS feed_status,
    feeds.fencing_token,
    to_jsonb(feeds) - ARRAY[
        'status',
        'worker_id',
        'last_heartbeat'
    ]::TEXT[] AS preserved_state
FROM public.feeds AS feeds
JOIN bcfy_calls_structural_membership AS membership
  ON membership.feed_id = feeds.id
ORDER BY feeds.id;

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
JOIN bcfy_calls_target_sids AS target
  ON target.sid = leases.lease_key
WHERE leases.source_type = 'bcfy_calls'
ORDER BY leases.lease_key;

DO $preflight$
DECLARE
    confirmation TEXT;
    duplicate_count BIGINT;
    invalid_count BIGINT;
BEGIN
    SELECT process_absence_confirmed
      INTO STRICT confirmation
      FROM bcfy_calls_activation_input;

    IF confirmation <> 'CONFIRMED' THEN
        RAISE EXCEPTION
            'legacy process absence has not been confirmed';
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

    -- A stopped legacy fleet has no child owner. Stale heartbeats without an
    -- owner are harmless and are cleared by the mutation below.
    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_cutover_rows
     WHERE worker_id IS NOT NULL;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'activation found % Calls children with current owners',
            invalid_count;
    END IF;

    -- Before activation, every Calls parent must still be dormant.
    SELECT COUNT(*)
      INTO invalid_count
      FROM public.ingestion_leases AS leases
     WHERE leases.source_type = 'bcfy_calls'
       AND (
           leases.status <> 'deactivated'::public.feed_status
           OR leases.worker_id IS NOT NULL
           OR leases.last_heartbeat IS NOT NULL
       );

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'activation found % SID Leases outside dormant state',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_target_sids AS target
      LEFT JOIN public.ingestion_leases AS leases
        ON leases.source_type = 'bcfy_calls'
       AND leases.lease_key = target.sid
     WHERE leases.lease_key IS NULL
        OR leases.status <> 'deactivated'::public.feed_status
        OR leases.worker_id IS NOT NULL
        OR leases.last_heartbeat IS NOT NULL
        OR leases.fencing_token = 9223372036854775807
        OR leases.membership_revision = 9223372036854775807;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'activation found % missing or unsafe target SID Leases',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM (
          SELECT
              target.sid,
              COUNT(*) FILTER (
                  WHERE membership.feed_status IN (
                      'unclaimed',
                      'active',
                      'failing'
                  )
              ) AS eligible_child_count,
              MAX(membership.fencing_token) AS maximum_child_fence
          FROM bcfy_calls_target_sids AS target
          LEFT JOIN bcfy_calls_structural_membership AS membership
            ON membership.sid = target.sid
          GROUP BY target.sid
      ) AS sid_state
     WHERE sid_state.eligible_child_count = 0
        OR sid_state.maximum_child_fence IS NULL
        OR sid_state.maximum_child_fence = 9223372036854775807;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'activation found % SIDs without safe eligible children',
            invalid_count;
    END IF;
END
$preflight$;

UPDATE public.feeds AS feeds
SET status = CASE
        WHEN feeds.status IN (
            'unclaimed'::public.feed_status,
            'active'::public.feed_status
        )
            THEN 'active'::public.feed_status
        ELSE feeds.status
    END,
    worker_id = NULL,
    last_heartbeat = NULL
FROM bcfy_calls_structural_membership AS membership
WHERE feeds.id = membership.feed_id;

CREATE TEMPORARY TABLE bcfy_calls_activated_leases ON COMMIT DROP AS
WITH child_fences AS (
    SELECT sid, MAX(fencing_token) AS maximum_child_fence
    FROM bcfy_calls_structural_membership
    GROUP BY sid
),
activated AS (
    UPDATE public.ingestion_leases AS leases
    SET status = 'unclaimed'::public.feed_status,
        worker_id = NULL,
        last_heartbeat = NULL,
        fencing_token = GREATEST(
            leases.fencing_token,
            child_fences.maximum_child_fence + 1
        ),
        membership_revision = leases.membership_revision + 1,
        updated_at = NOW()
    FROM child_fences
    WHERE leases.source_type = 'bcfy_calls'
      AND leases.lease_key = child_fences.sid
      AND leases.status = 'deactivated'::public.feed_status
    RETURNING
        leases.source_type,
        leases.lease_key,
        leases.fencing_token,
        leases.membership_revision
)
SELECT *
FROM activated
ORDER BY lease_key;

DO $postflight$
DECLARE
    expected_count BIGINT;
    invalid_count BIGINT;
    updated_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO expected_count FROM bcfy_calls_target_sids;
    SELECT COUNT(*) INTO updated_count FROM bcfy_calls_activated_leases;

    IF updated_count <> expected_count THEN
        RAISE EXCEPTION
            'activation changed % SID Leases, expected %',
            updated_count,
            expected_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_lease_before AS before_state
      JOIN public.ingestion_leases AS leases
        ON leases.source_type = before_state.source_type
       AND leases.lease_key = before_state.lease_key
      JOIN (
          SELECT sid, MAX(fencing_token) AS maximum_child_fence
          FROM bcfy_calls_structural_membership
          GROUP BY sid
      ) AS child_fences ON child_fences.sid = leases.lease_key
     WHERE leases.status <> 'unclaimed'::public.feed_status
        OR leases.worker_id IS NOT NULL
        OR leases.last_heartbeat IS NOT NULL
        OR leases.fencing_token <= child_fences.maximum_child_fence
        OR leases.fencing_token < before_state.fencing_token
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
            'activation SID Lease postflight failed for % rows',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM bcfy_calls_child_before AS before_state
      JOIN public.feeds AS feeds ON feeds.id = before_state.feed_id
     WHERE feeds.worker_id IS NOT NULL
        OR feeds.last_heartbeat IS NOT NULL
        OR (
            before_state.feed_status IN ('unclaimed', 'active')
            AND feeds.status <> 'active'::public.feed_status
        )
        OR (
            before_state.feed_status NOT IN ('unclaimed', 'active')
            AND feeds.status::TEXT <> before_state.feed_status
        )
        OR to_jsonb(feeds) - ARRAY[
               'status',
               'worker_id',
               'last_heartbeat'
           ]::TEXT[] IS DISTINCT FROM before_state.preserved_state;

    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'activation child Feed postflight failed for % rows',
            invalid_count;
    END IF;
END
$postflight$;

SELECT
    activated.lease_key AS sid,
    activated.fencing_token,
    activated.membership_revision,
    COUNT(membership.feed_id) AS structural_feed_count,
    COUNT(membership.feed_id) FILTER (
        WHERE membership.feed_status IN ('unclaimed', 'active', 'failing')
    ) AS eligible_feed_count
FROM bcfy_calls_activated_leases AS activated
JOIN bcfy_calls_structural_membership AS membership
  ON membership.sid = activated.lease_key
GROUP BY
    activated.lease_key,
    activated.fencing_token,
    activated.membership_revision
ORDER BY activated.lease_key;

COMMIT;
