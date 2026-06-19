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
-- The one allow-list exception is idx_feeds_failing_retryable: it indexes
-- retry_after, which is HOT-protected in principle but mutated only on the
-- (rare) failure-to-retry transition. Partial-index bloat there is
-- operationally acceptable given the volume; no other index on retry_after
-- is permitted.
--
-- Known blindspot: this query matches indexed columns via pg_index.indkey,
-- which stores 0 for expression-index entries (the real expression lives in
-- pg_index.indexprs as a pg_node_tree). An expression index such as
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
SELECT c.relname AS indexname, a.attname
  FROM pg_class t
  JOIN pg_index x ON x.indrelid = t.oid
  JOIN pg_class c ON c.oid = x.indexrelid
  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(x.indkey)
 WHERE t.relname = 'feeds'
   AND t.relnamespace = 'public'::regnamespace
   AND a.attname IN (
       'last_heartbeat',
       'unclaimed_since',
       'worker_id',
       'fencing_token',
       'last_processed_filename',
       'last_bookmark_time',
       'failure_count',
       'retry_after',
       'status_reason_detail'
   )
   AND c.relname <> 'idx_feeds_failing_retryable';
