\set ON_ERROR_STOP on

-- Run this immediately before 000_backfill_membership.sql.  It derives the
-- exact psql review inputs from one read-only snapshot; do not copy counts
-- from a prior run.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $preflight$
DECLARE
    calls_feed_count INTEGER;
    incomplete_count INTEGER;
    duplicate_count INTEGER;
BEGIN
    SELECT COUNT(*)
      INTO calls_feed_count
      FROM public.feeds AS feeds
     WHERE feeds.source_type = 'bcfy_calls';
    IF calls_feed_count = 0 THEN
        RAISE EXCEPTION 'no Broadcastify Calls feeds exist to hand off';
    END IF;

    -- Before the backfill, membership is valid only when all three nullable
    -- fields are absent, or when a prior successful backfill matches the
    -- canonical legacy source ID exactly.
    SELECT COUNT(*)
      INTO incomplete_count
      FROM public.feeds AS feeds
      LEFT JOIN public.feed_properties AS properties
        ON properties.feed_id = feeds.id
     WHERE feeds.source_type = 'bcfy_calls'
       AND (
           properties.feed_id IS NULL
           OR properties.source_type <> feeds.source_type
           OR properties.source_feed_id !~ '^[0-9]+-[0-9]+$'
           OR (
               NOT (
                   properties.bcfy_calls_sid IS NULL
                   AND properties.bcfy_calls_group_id IS NULL
                   AND properties.bcfy_calls_is_trunked IS NULL
               )
               AND NOT (
                   properties.bcfy_calls_sid = split_part(
                       properties.source_feed_id,
                       '-',
                       1
                   )
                   AND properties.bcfy_calls_group_id = split_part(
                       properties.source_feed_id,
                       '-',
                       2
                   )
                   AND properties.bcfy_calls_is_trunked IS TRUE
               )
           )
       );
    IF incomplete_count <> 0 THEN
        RAISE EXCEPTION
            'Calls source membership is absent, partial, or malformed for % rows',
            incomplete_count;
    END IF;

    SELECT COUNT(*)
      INTO duplicate_count
      FROM (
          SELECT
              split_part(properties.source_feed_id, '-', 1) AS sid,
              split_part(properties.source_feed_id, '-', 2) AS group_id
          FROM public.feed_properties AS properties
          JOIN public.feeds AS feeds ON feeds.id = properties.feed_id
         WHERE properties.source_type = 'bcfy_calls'
           AND feeds.source_type = 'bcfy_calls'
          GROUP BY
              split_part(properties.source_feed_id, '-', 1),
              split_part(properties.source_feed_id, '-', 2)
          HAVING COUNT(*) <> 1
      ) AS duplicate_routes;
    IF duplicate_count <> 0 THEN
        RAISE EXCEPTION
            'Calls source membership contains % duplicate canonical routes',
            duplicate_count;
    END IF;
END
$preflight$;

WITH full_calls_membership AS (
    SELECT
        properties.feed_id,
        split_part(properties.source_feed_id, '-', 1) AS sid,
        split_part(properties.source_feed_id, '-', 2) AS group_id
    FROM public.feed_properties AS properties
    JOIN public.feeds AS feeds ON feeds.id = properties.feed_id
    WHERE properties.source_type = 'bcfy_calls'
      AND feeds.source_type = 'bcfy_calls'
),
eligible_calls_membership AS (
    SELECT full_calls_membership.*
    FROM full_calls_membership
    JOIN public.feeds AS feeds ON feeds.id = full_calls_membership.feed_id
    WHERE feeds.status <> 'deactivated'::public.feed_status
),
manifests AS (
    SELECT
        'full'::TEXT AS manifest_scope,
        COUNT(*) AS feed_count,
        COUNT(DISTINCT sid) AS sid_count,
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
    FROM full_calls_membership
    UNION ALL
    SELECT
        'eligible'::TEXT AS manifest_scope,
        COUNT(*) AS feed_count,
        COUNT(DISTINCT sid) AS sid_count,
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
    FROM eligible_calls_membership
)
SELECT manifest_scope, feed_count, sid_count, manifest_digest
FROM manifests
ORDER BY manifest_scope;

COMMIT;
