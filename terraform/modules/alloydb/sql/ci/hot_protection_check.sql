-- HOT-protection guard. Runs against the schema produced by applying all
-- files under sql/ingestion/*.sql to a fresh PostgreSQL 16 instance.
-- Returns one row per (index, column) pair that violates the HOT invariant
-- — CI fails the build if any row is returned.
--
-- Invariant: no index on the feeds table may reference a column that the
-- hot write path mutates, because PostgreSQL's Heap-Only Tuple optimization
-- is disabled for an UPDATE whenever any indexed column is modified. The
-- nine guarded columns below are all mutated at high frequency by claim,
-- heartbeat, progress, release, or failure paths.
--
-- The one allow-list exception is idx_feeds_failing_retryable on retry_after.
-- retry_after is HOT-protected in principle but mutated only on the (rare)
-- failure-to-retry transition. Partial-index bloat there is operationally
-- acceptable given the volume. The exception is column-scoped: the same index
-- may not include any other guarded column.
--
-- Known blindspot: this query matches indexed columns via pg_index.indkey and
-- partial-index predicates via pg_index.indpred. It still does not parse
-- expression-index entries from pg_index.indexprs, which store 0 in indkey.
-- An expression index such as
-- CREATE INDEX ... ON feeds ((COALESCE(worker_id, ''))) would therefore
-- slip past this check. Parsing indexprs to catch that case is deliberately
-- not done — expression indexes on these columns are unlikely in practice
-- and the added complexity is not worth it. Reviewers of future migrations
-- should scan CREATE INDEX diffs for expression-form references to the
-- guarded column list below.
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
        ('status_reason_detail')
)
SELECT c.relname AS indexname, g.attname
  FROM pg_class t
  JOIN pg_index x ON x.indrelid = t.oid
  JOIN pg_class c ON c.oid = x.indexrelid
  JOIN guarded_columns g ON (
       g.attname IN (
           SELECT a.attname
             FROM pg_attribute a
            WHERE a.attrelid = t.oid
              AND a.attnum = ANY(x.indkey)
       )
       OR (
           x.indpred IS NOT NULL
           AND pg_get_expr(x.indpred, x.indrelid) ~ ('\m' || g.attname || '\M')
       )
  )
 WHERE t.relname = 'feeds'
   AND t.relnamespace = 'public'::regnamespace
   AND NOT (
       c.relname = 'idx_feeds_failing_retryable'
       AND g.attname = 'retry_after'
   );
