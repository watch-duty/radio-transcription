-- HOT-protection guard. Runs against the schema produced by applying all
-- files under sql/ingestion/*.sql to fresh PostgreSQL 15/16 instances.
-- Returns one row per (index, column, reference kind) that violates the HOT
-- invariant — CI fails the build if any row is returned.
--
-- Invariant: no index on the feeds table may reference a column that the
-- hot write path mutates, because PostgreSQL's Heap-Only Tuple optimization
-- is disabled for an UPDATE whenever any indexed column is modified. The
-- guarded columns below are mutated at high frequency by claim, heartbeat,
-- progress, release, or failure paths.
--
-- retry_after is intentionally indexed only by idx_feeds_failing_retryable.
-- It is mutable, so keep it guarded and explicitly exempt that one direct
-- reference. Any future retry_after index must be reviewed deliberately.
--
-- This query uses pg_depend to find catalog-level index dependencies on those
-- columns. Direct key references are classified as direct; predicate and
-- expression-index dependencies are classified as derived. This avoids parsing
-- PostgreSQL's decompiled expression text while still blocking risky
-- references for review.
--
-- Schema safety: the joins walk pg_class OIDs rather than index names. A
-- name-based join (JOIN pg_class c ON c.relname = i.indexname) matches any
-- index with the given name across every schema, which could surface false
-- positives if a test or migration creates a same-named index elsewhere.
-- The OID-based form anchors everything to the feeds table in public.
WITH guarded_columns(attname) AS (
    VALUES
        ('last_heartbeat'),
        ('unclaimed_since'),
        ('worker_id'),
        ('fencing_token'),
        ('last_processed_filename'),
        ('last_bookmark_time'),
        ('failure_count'),
        ('retry_after'),
        ('status_reason'),
        ('status_reason_updated_at'),
        ('status_reason_detail'),
        ('audit_revision')
),
feed_indexes AS (
    SELECT
        t.oid AS table_oid,
        x.indexrelid,
        c.relname AS indexname,
        x.indkey
      FROM pg_class t
      JOIN pg_index x ON x.indrelid = t.oid
      JOIN pg_class c ON c.oid = x.indexrelid
     WHERE t.relname = 'feeds'
       AND t.relnamespace = 'public'::regnamespace
),
index_column_dependencies AS (
    SELECT
        i.indexname,
        g.attname,
        CASE
            WHEN a.attnum = ANY(i.indkey) THEN 'direct'
            ELSE 'derived'
        END AS reference_kind
      FROM feed_indexes i
      JOIN pg_depend d
        ON d.classid = 'pg_class'::regclass
       AND d.objid = i.indexrelid
       AND d.refclassid = 'pg_class'::regclass
       AND d.refobjid = i.table_oid
       AND d.refobjsubid > 0
      JOIN pg_attribute a
        ON a.attrelid = i.table_oid
       AND a.attnum = d.refobjsubid
      JOIN guarded_columns g ON g.attname = a.attname
),
allowed_references AS (
    SELECT
        'idx_feeds_failing_retryable'::text AS indexname,
        'retry_after'::text AS attname,
        'direct'::text AS reference_kind
),
violations AS (
    SELECT DISTINCT * FROM index_column_dependencies
)
SELECT v.indexname, v.attname, v.reference_kind
  FROM violations v
 WHERE NOT EXISTS (
       SELECT 1
         FROM allowed_references a
        WHERE a.indexname = v.indexname
          AND a.attname = v.attname
          AND a.reference_kind = v.reference_kind
  )
 ORDER BY v.indexname, v.attname, v.reference_kind;
