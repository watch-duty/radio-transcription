\set ON_ERROR_STOP on

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TEMPORARY TABLE phase7_verified_membership ON COMMIT DROP AS
SELECT
    properties.feed_id,
    properties.bcfy_calls_sid AS sid,
    properties.bcfy_calls_group_id AS group_id,
    feeds.status::TEXT AS status,
    feeds.worker_id,
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

DO $projection_postflight$
DECLARE
    duplicate_count INTEGER;
    expected_feed_count INTEGER;
    expected_sid_count INTEGER;
    incomplete_count INTEGER;
BEGIN
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
            'verification found % incomplete Calls projections',
            incomplete_count;
    END IF;

    SELECT COUNT(*)
      INTO duplicate_count
      FROM (
          SELECT sid, group_id
          FROM phase7_verified_membership
          GROUP BY sid, group_id
          HAVING COUNT(*) <> 1
      ) AS duplicate_routes;
    IF duplicate_count <> 0 THEN
        RAISE EXCEPTION
            'verification found duplicate canonical Calls routes';
    END IF;

    SELECT COUNT(*), COUNT(DISTINCT sid)
      INTO expected_feed_count, expected_sid_count
      FROM phase7_verified_membership;
    IF expected_sid_count <> 19 OR expected_feed_count <> 154 THEN
        RAISE EXCEPTION
            'verification expected 19 SIDs/154 Feeds, found %/%',
            expected_sid_count,
            expected_feed_count;
    END IF;
END
$projection_postflight$;

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
FROM phase7_verified_membership;

SELECT
    membership.sid,
    COUNT(*) AS enabled_feed_count,
    COUNT(*) FILTER (
        WHERE membership.status IN ('active', 'failing')
    ) AS sid_eligible_feed_count,
    COUNT(*) FILTER (
        WHERE membership.worker_id IS NOT NULL
    ) AS owned_child_count,
    MAX(membership.fencing_token) AS maximum_child_fence,
    leases.status::TEXT AS lease_status,
    leases.worker_id AS lease_worker_id,
    leases.fencing_token AS lease_fence,
    leases.membership_revision,
    leases.fencing_token > MAX(membership.fencing_token)
        AS lease_fence_dominates
FROM phase7_verified_membership AS membership
LEFT JOIN public.ingestion_leases AS leases
  ON leases.source_type = 'bcfy_calls'
 AND leases.lease_key = membership.sid
GROUP BY
    membership.sid,
    leases.status,
    leases.worker_id,
    leases.fencing_token,
    leases.membership_revision
ORDER BY membership.sid;

SELECT
    COUNT(*) FILTER (
        WHERE leases.lease_key IS NULL
    ) AS missing_lease_count,
    COUNT(*) FILTER (
        WHERE expected.sid IS NULL
    ) AS extra_lease_count,
    COUNT(*) FILTER (
        WHERE leases.worker_id IS NOT NULL
    ) AS owned_lease_count
FROM (
    SELECT DISTINCT sid FROM phase7_verified_membership
) AS expected
FULL JOIN public.ingestion_leases AS leases
  ON leases.source_type = 'bcfy_calls'
 AND leases.lease_key = expected.sid
WHERE leases.source_type = 'bcfy_calls' OR expected.sid IS NOT NULL;

COMMIT;
