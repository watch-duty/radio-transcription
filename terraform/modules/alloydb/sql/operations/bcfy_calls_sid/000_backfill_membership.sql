\set ON_ERROR_STOP on

\if :{?reviewed_total_feed_count}
\else
\echo 'reviewed_total_feed_count psql variable is required'
\quit 3
\endif
\if :{?reviewed_total_sid_count}
\else
\echo 'reviewed_total_sid_count psql variable is required'
\quit 3
\endif
\if :{?reviewed_total_manifest_digest}
\else
\echo 'reviewed_total_manifest_digest psql variable is required'
\quit 3
\endif
\if :{?backfill_confirmed}
\else
\echo 'backfill_confirmed psql variable is required'
\quit 3
\endif

-- Populate the nullable legacy metadata only after an operator has reviewed
-- the exact complete Calls set.  The source IDs are the pre-existing
-- canonical ``SID-group`` identities; authority operations never parse them.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Keep the mutation order compatible with the subsequent authority handoff.
LOCK TABLE public.ingestion_leases IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.feeds IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.feed_properties IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMPORARY TABLE phase7_review_input (
    reviewed_total_feed_count INTEGER NOT NULL,
    reviewed_total_sid_count INTEGER NOT NULL,
    reviewed_total_manifest_digest TEXT NOT NULL,
    backfill_confirmed TEXT NOT NULL
) ON COMMIT DROP;
INSERT INTO phase7_review_input VALUES (
    :'reviewed_total_feed_count'::INTEGER,
    :'reviewed_total_sid_count'::INTEGER,
    :'reviewed_total_manifest_digest',
    :'backfill_confirmed'
);

CREATE TEMPORARY TABLE phase7_full_membership ON COMMIT DROP AS
SELECT
    properties.feed_id,
    properties.source_feed_id,
    split_part(properties.source_feed_id, '-', 1) AS sid,
    split_part(properties.source_feed_id, '-', 2) AS group_id
FROM public.feed_properties AS properties
JOIN public.feeds AS feeds ON feeds.id = properties.feed_id
WHERE properties.source_type = 'bcfy_calls'
  AND feeds.source_type = 'bcfy_calls'
ORDER BY properties.feed_id;

DO $preflight$
DECLARE
    review RECORD;
    incomplete_count INTEGER;
    duplicate_count INTEGER;
    expected_feed_count INTEGER;
    expected_sid_count INTEGER;
    expected_digest TEXT;
BEGIN
    SELECT * INTO STRICT review FROM phase7_review_input;
    IF review.backfill_confirmed <> 'CONFIRMED'
       OR review.reviewed_total_feed_count <= 0
       OR review.reviewed_total_sid_count <= 0
       OR review.reviewed_total_manifest_digest !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION
            'backfill review input or confirmation is invalid';
    END IF;

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
            'Calls membership cannot be backfilled safely for % rows',
            incomplete_count;
    END IF;

    SELECT COUNT(*)
      INTO duplicate_count
      FROM (
          SELECT sid, group_id
          FROM phase7_full_membership
          GROUP BY sid, group_id
          HAVING COUNT(*) <> 1
      ) AS duplicate_routes;
    IF duplicate_count <> 0 THEN
        RAISE EXCEPTION
            'Calls membership contains % duplicate canonical routes',
            duplicate_count;
    END IF;

    SELECT
        COUNT(*),
        COUNT(DISTINCT sid),
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
      INTO expected_feed_count, expected_sid_count, expected_digest
      FROM phase7_full_membership;
    IF expected_feed_count <> review.reviewed_total_feed_count
       OR expected_sid_count <> review.reviewed_total_sid_count
       OR expected_digest IS DISTINCT FROM review.reviewed_total_manifest_digest THEN
        RAISE EXCEPTION
            'reviewed full Calls membership changed: SIDs %, Feeds %, digest %',
            expected_sid_count,
            expected_feed_count,
            expected_digest;
    END IF;
END
$preflight$;

UPDATE public.feed_properties AS properties
SET bcfy_calls_sid = membership.sid,
    bcfy_calls_group_id = membership.group_id,
    bcfy_calls_is_trunked = TRUE
FROM phase7_full_membership AS membership
WHERE properties.feed_id = membership.feed_id
  AND properties.bcfy_calls_sid IS NULL
  AND properties.bcfy_calls_group_id IS NULL
  AND properties.bcfy_calls_is_trunked IS NULL;

DO $postflight$
DECLARE
    expected_feed_count INTEGER;
    structured_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO expected_feed_count FROM phase7_full_membership;

    SELECT COUNT(*)
      INTO structured_count
      FROM public.feed_properties AS properties
      JOIN phase7_full_membership AS membership
        ON membership.feed_id = properties.feed_id
     WHERE properties.bcfy_calls_sid = membership.sid
       AND properties.bcfy_calls_group_id = membership.group_id
       AND properties.bcfy_calls_is_trunked IS TRUE;
    IF structured_count <> expected_feed_count THEN
        RAISE EXCEPTION
            'backfill postflight found % structured rows, expected %',
            structured_count,
            expected_feed_count;
    END IF;
END
$postflight$;

SELECT
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
FROM phase7_full_membership;

COMMIT;
