\set ON_ERROR_STOP on

\if :{?process_absence_confirmed}
\else
\echo 'process_absence_confirmed psql variable is required'
\quit 3
\endif

-- Forbidden until the runbook proves every SID process absent.  Lease rows
-- are deliberately read and locked but never changed by this inverse handoff.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TEMPORARY TABLE phase7_rollback_input (
    process_absence_confirmed TEXT NOT NULL
) ON COMMIT DROP;
INSERT INTO phase7_rollback_input VALUES (:'process_absence_confirmed');

-- Global controlled-mutation order: Lease -> Feed -> properties.
LOCK TABLE public.ingestion_leases IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.feeds IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.feed_properties IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMPORARY TABLE phase7_managed_sids ON COMMIT DROP AS
SELECT leases.lease_key AS sid
FROM public.ingestion_leases AS leases
WHERE leases.source_type = 'bcfy_calls'
  AND leases.status <> 'deactivated'::public.feed_status
ORDER BY leases.lease_key;

CREATE TEMPORARY TABLE phase7_structural_membership ON COMMIT DROP AS
SELECT
    properties.feed_id,
    properties.bcfy_calls_sid AS sid,
    properties.bcfy_calls_group_id AS group_id,
    feeds.status::TEXT AS status,
    feeds.worker_id,
    feeds.last_heartbeat
FROM public.feed_properties AS properties
JOIN public.feeds AS feeds ON feeds.id = properties.feed_id
JOIN phase7_managed_sids AS managed
  ON managed.sid = properties.bcfy_calls_sid
WHERE properties.source_type = 'bcfy_calls'
  AND feeds.source_type = 'bcfy_calls'
  AND properties.bcfy_calls_is_trunked IS TRUE
  AND properties.bcfy_calls_sid IS NOT NULL
  AND properties.bcfy_calls_group_id IS NOT NULL
ORDER BY properties.bcfy_calls_sid,
         properties.bcfy_calls_group_id,
         properties.feed_id;

CREATE TEMPORARY TABLE phase7_lease_before ON COMMIT DROP AS
SELECT
    leases.source_type,
    leases.lease_key,
    to_jsonb(leases) AS complete_state
FROM public.ingestion_leases AS leases
WHERE leases.source_type = 'bcfy_calls'
ORDER BY leases.source_type, leases.lease_key;

CREATE TEMPORARY TABLE phase7_child_before ON COMMIT DROP AS
SELECT
    feeds.id,
    feeds.status::TEXT AS status,
    to_jsonb(feeds) - ARRAY['status', 'unclaimed_since']::TEXT[]
        AS durable_state
FROM public.feeds AS feeds
JOIN phase7_structural_membership AS membership
  ON membership.feed_id = feeds.id
ORDER BY feeds.id;

DO $preflight$
DECLARE
    duplicate_count INTEGER;
    incomplete_count INTEGER;
    invalid_count INTEGER;
    managed_sid_count INTEGER;
    confirmation TEXT;
BEGIN
    SELECT process_absence_confirmed
      INTO STRICT confirmation
      FROM phase7_rollback_input;
    IF confirmation <> 'CONFIRMED' THEN
        RAISE EXCEPTION 'SID process absence has not been confirmed';
    END IF;

    SELECT COUNT(*) INTO managed_sid_count FROM phase7_managed_sids;
    IF managed_sid_count <> 19 THEN
        RAISE EXCEPTION
            'rollback found % managed SIDs, expected 19', managed_sid_count;
    END IF;

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
       );
    IF incomplete_count <> 0 THEN
        RAISE EXCEPTION
            'rollback projection is absent, partial, or inconsistent for % rows',
            incomplete_count;
    END IF;

    SELECT COUNT(*)
      INTO duplicate_count
      FROM (
          SELECT sid, group_id
          FROM phase7_structural_membership
          GROUP BY sid, group_id
          HAVING COUNT(*) <> 1
      ) AS duplicate_routes;
    IF duplicate_count <> 0 THEN
        RAISE EXCEPTION
            'rollback projection contains duplicate canonical routes';
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM phase7_structural_membership
     WHERE status = 'active'
       AND (worker_id IS NOT NULL OR last_heartbeat IS NOT NULL);
    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback found % active children with live authority state',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM phase7_managed_sids AS managed
      LEFT JOIN phase7_structural_membership AS membership
        ON membership.sid = managed.sid
     WHERE membership.feed_id IS NULL;
    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback found % managed SIDs without mapped children',
            invalid_count;
    END IF;
END
$preflight$;

CREATE TEMPORARY TABLE phase7_rolled_back_children ON COMMIT DROP AS
WITH rolled_back AS (
    UPDATE public.feeds AS feeds
    SET status = 'unclaimed'::public.feed_status,
        unclaimed_since = NOW()
    FROM phase7_structural_membership AS membership
    WHERE feeds.id = membership.feed_id
      AND feeds.status = 'active'::public.feed_status
      AND feeds.worker_id IS NULL
      AND feeds.last_heartbeat IS NULL
    RETURNING feeds.id
)
SELECT id FROM rolled_back ORDER BY id;

DO $postflight$
DECLARE
    invalid_count INTEGER;
BEGIN
    SELECT COUNT(*)
      INTO invalid_count
      FROM phase7_lease_before AS before_state
      JOIN public.ingestion_leases AS leases
        ON leases.source_type = before_state.source_type
       AND leases.lease_key = before_state.lease_key
     WHERE to_jsonb(leases) IS DISTINCT FROM before_state.complete_state;
    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback changed % permanent Lease rows', invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM phase7_child_before AS before_state
      JOIN public.feeds AS feeds ON feeds.id = before_state.id
     WHERE to_jsonb(feeds) - ARRAY[
               'status',
               'unclaimed_since'
           ]::TEXT[] IS DISTINCT FROM before_state.durable_state
        OR before_state.status = 'active' AND
           feeds.status <> 'unclaimed'::public.feed_status
        OR before_state.status <> 'active' AND
           feeds.status::TEXT <> before_state.status;
    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'rollback child postflight failed for % rows', invalid_count;
    END IF;
END
$postflight$;

SELECT COUNT(*) AS rolled_back_child_count
FROM phase7_rolled_back_children;

SELECT
    membership.sid,
    COUNT(*) FILTER (
        WHERE feeds.status = 'unclaimed'::public.feed_status
    ) AS legacy_claimable_count,
    COUNT(*) FILTER (
        WHERE feeds.status = 'failing'::public.feed_status
    ) AS preserved_failing_count,
    COUNT(*) FILTER (
        WHERE feeds.status = 'quarantined'::public.feed_status
    ) AS preserved_quarantined_count,
    COUNT(*) FILTER (
        WHERE feeds.status = 'deactivated'::public.feed_status
    ) AS preserved_deactivated_count
FROM phase7_structural_membership AS membership
JOIN public.feeds AS feeds ON feeds.id = membership.feed_id
GROUP BY membership.sid
ORDER BY membership.sid;

COMMIT;
