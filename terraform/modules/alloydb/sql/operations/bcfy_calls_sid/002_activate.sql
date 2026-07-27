\set ON_ERROR_STOP on

\if :{?reviewed_eligible_feed_count}
\else
\echo 'reviewed_eligible_feed_count psql variable is required'
\quit 3
\endif
\if :{?reviewed_eligible_sid_count}
\else
\echo 'reviewed_eligible_sid_count psql variable is required'
\quit 3
\endif
\if :{?reviewed_eligible_manifest_digest}
\else
\echo 'reviewed_eligible_manifest_digest psql variable is required'
\quit 3
\endif
\if :{?process_absence_confirmed}
\else
\echo 'process_absence_confirmed psql variable is required'
\quit 3
\endif

-- Forbidden until the runbook proves every legacy process absent.  This
-- transaction performs the one-time child/parent authority handoff.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TEMPORARY TABLE phase7_review_input (
    reviewed_eligible_feed_count INTEGER NOT NULL,
    reviewed_eligible_sid_count INTEGER NOT NULL,
    reviewed_eligible_manifest_digest TEXT NOT NULL,
    process_absence_confirmed TEXT NOT NULL
) ON COMMIT DROP;
INSERT INTO phase7_review_input VALUES (
    :'reviewed_eligible_feed_count'::INTEGER,
    :'reviewed_eligible_sid_count'::INTEGER,
    :'reviewed_eligible_manifest_digest',
    :'process_absence_confirmed'
);

-- Global controlled-mutation order: Lease -> Feed -> properties.  The table
-- locks also make the reviewed membership set stable through final postflight.
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

CREATE TEMPORARY TABLE phase7_expected_sids ON COMMIT DROP AS
SELECT DISTINCT sid
FROM phase7_expected_membership
ORDER BY sid;

-- Every structurally mapped child contributes to the parent fence, including
-- quarantined/deactivated children excluded from current work eligibility.
CREATE TEMPORARY TABLE phase7_structural_membership ON COMMIT DROP AS
SELECT
    properties.feed_id,
    properties.bcfy_calls_sid AS sid,
    properties.bcfy_calls_group_id AS group_id,
    feeds.status::TEXT AS status,
    feeds.worker_id,
    feeds.last_heartbeat,
    feeds.fencing_token
FROM public.feed_properties AS properties
JOIN public.feeds AS feeds ON feeds.id = properties.feed_id
JOIN phase7_expected_sids AS expected
  ON expected.sid = properties.bcfy_calls_sid
WHERE properties.source_type = 'bcfy_calls'
  AND feeds.source_type = 'bcfy_calls'
  AND properties.bcfy_calls_is_trunked IS TRUE
ORDER BY properties.bcfy_calls_sid,
         properties.bcfy_calls_group_id,
         properties.feed_id;

CREATE TEMPORARY TABLE phase7_child_before ON COMMIT DROP AS
SELECT
    feeds.id,
    feeds.status::TEXT AS status,
    to_jsonb(feeds) - ARRAY[
        'status',
        'worker_id',
        'last_heartbeat',
        'unclaimed_since'
    ]::TEXT[] AS durable_state
FROM public.feeds AS feeds
JOIN phase7_structural_membership AS membership
  ON membership.feed_id = feeds.id
ORDER BY feeds.id;

CREATE TEMPORARY TABLE phase7_lease_before ON COMMIT DROP AS
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
    ]::TEXT[] AS durable_state
FROM public.ingestion_leases AS leases
WHERE leases.source_type = 'bcfy_calls'
ORDER BY leases.source_type, leases.lease_key;

DO $preflight$
DECLARE
    duplicate_count INTEGER;
    expected_digest TEXT;
    expected_feed_count INTEGER;
    expected_sid_count INTEGER;
    incomplete_count INTEGER;
    invalid_count INTEGER;
    review RECORD;
BEGIN
    SELECT * INTO STRICT review FROM phase7_review_input;
    IF review.process_absence_confirmed <> 'CONFIRMED'
       OR review.reviewed_eligible_feed_count <= 0
       OR review.reviewed_eligible_sid_count <= 0
       OR review.reviewed_eligible_manifest_digest !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION
            'activation review input or process-absence confirmation is invalid';
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
            'maintained Calls projection contains duplicate routes';
    END IF;

    SELECT
        COUNT(DISTINCT sid),
        COUNT(*),
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
        )
      INTO expected_sid_count, expected_feed_count, expected_digest
      FROM phase7_expected_membership;
    IF expected_sid_count <> review.reviewed_eligible_sid_count
       OR expected_feed_count <> review.reviewed_eligible_feed_count
       OR expected_digest IS DISTINCT FROM
          review.reviewed_eligible_manifest_digest THEN
        RAISE EXCEPTION
            'reviewed membership changed: SIDs %, Feeds %, digest %',
            expected_sid_count,
            expected_feed_count,
            expected_digest;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM public.ingestion_leases AS leases
      FULL JOIN phase7_expected_sids AS expected
        ON expected.sid = leases.lease_key
       AND leases.source_type = 'bcfy_calls'
     WHERE (leases.source_type = 'bcfy_calls' OR expected.sid IS NOT NULL)
       AND (
           expected.sid IS NULL
           OR leases.source_type IS NULL
           OR leases.status <> 'deactivated'::public.feed_status
           OR leases.worker_id IS NOT NULL
           OR leases.last_heartbeat IS NOT NULL
           OR leases.fencing_token = 9223372036854775807
           OR leases.membership_revision = 9223372036854775807
       );
    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'activation Lease set/state is missing, extra, owned, or unsafe';
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM phase7_structural_membership
     WHERE worker_id IS NOT NULL
       AND (
           status <> 'active'
           OR last_heartbeat IS NULL
           OR last_heartbeat >= NOW() - INTERVAL '60 seconds'
       );
    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'activation found % children with non-stale ownership',
            invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM (
          SELECT sid
          FROM phase7_expected_membership
          GROUP BY sid
          HAVING COUNT(*) FILTER (
              WHERE status IN ('unclaimed', 'active', 'failing')
          ) = 0
             OR MAX(fencing_token) = 9223372036854775807
      ) AS invalid_sid;
    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'activation found % SIDs without safe eligible children/fences',
            invalid_count;
    END IF;
END
$preflight$;

UPDATE public.feeds AS feeds
SET status = 'active'::public.feed_status,
    worker_id = NULL,
    last_heartbeat = NULL,
    unclaimed_since = NULL
FROM phase7_structural_membership AS membership
WHERE feeds.id = membership.feed_id
  AND feeds.status IN (
      'unclaimed'::public.feed_status,
      'active'::public.feed_status
  );

CREATE TEMPORARY TABLE phase7_activated_leases ON COMMIT DROP AS
WITH child_fences AS (
    SELECT sid, MAX(fencing_token) AS maximum_child_fence
    FROM phase7_structural_membership
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
SELECT * FROM activated ORDER BY source_type, lease_key;

DO $postflight$
DECLARE
    invalid_count INTEGER;
    transition_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO transition_count FROM phase7_activated_leases;
    IF transition_count <> (
        SELECT reviewed_eligible_sid_count FROM phase7_review_input
    ) THEN
        RAISE EXCEPTION
            'activation changed % Leases, expected reviewed SID count',
            transition_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM phase7_lease_before AS before_state
      JOIN public.ingestion_leases AS leases
        ON leases.source_type = before_state.source_type
       AND leases.lease_key = before_state.lease_key
      JOIN (
          SELECT sid, MAX(fencing_token) AS maximum_child_fence
          FROM phase7_structural_membership
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
           ]::TEXT[] IS DISTINCT FROM before_state.durable_state;
    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'activation Lease postflight failed for % rows', invalid_count;
    END IF;

    SELECT COUNT(*)
      INTO invalid_count
      FROM phase7_child_before AS before_state
      JOIN public.feeds AS feeds ON feeds.id = before_state.id
     WHERE to_jsonb(feeds) - ARRAY[
               'status',
               'worker_id',
               'last_heartbeat',
               'unclaimed_since'
           ]::TEXT[] IS DISTINCT FROM before_state.durable_state
        OR feeds.worker_id IS NOT NULL
        OR before_state.status IN ('unclaimed', 'active') AND
           feeds.status <> 'active'::public.feed_status
        OR before_state.status NOT IN ('unclaimed', 'active') AND
           feeds.status::TEXT <> before_state.status;
    IF invalid_count <> 0 THEN
        RAISE EXCEPTION
            'activation child postflight failed for % rows', invalid_count;
    END IF;
END
$postflight$;

SELECT
    activated.lease_key AS sid,
    activated.fencing_token,
    activated.membership_revision,
    COUNT(membership.feed_id) FILTER (
        WHERE membership.status <> 'deactivated'
    ) AS mapped_feed_count
FROM phase7_activated_leases AS activated
JOIN phase7_structural_membership AS membership
  ON membership.sid = activated.lease_key
GROUP BY
    activated.lease_key,
    activated.fencing_token,
    activated.membership_revision
ORDER BY activated.lease_key;

COMMIT;
