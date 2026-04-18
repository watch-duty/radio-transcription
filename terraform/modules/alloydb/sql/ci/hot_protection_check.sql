-- HOT-protection guard. Runs against the schema produced by applying all
-- files under sql/ingestion/*.sql to a fresh PostgreSQL 16 instance.
-- Returns one row per (index, column) pair that violates the HOT invariant
-- — CI fails the build if any row is returned.
--
-- Invariant: no index on the feeds table may reference a column that the
-- hot write path mutates, because PostgreSQL's Heap-Only Tuple optimization
-- is disabled for an UPDATE whenever any indexed column is modified. The
-- eight guarded columns below are all mutated at high frequency by claim,
-- heartbeat, progress, release, or failure paths.
--
-- The one allow-list exception is idx_feeds_failing_retryable: it indexes
-- retry_after, which is HOT-protected in principle but mutated only on the
-- (rare) failure-to-retry transition. Partial-index bloat there is
-- operationally acceptable given the volume; no other index on retry_after
-- is permitted.
SELECT i.indexname, a.attname
  FROM pg_indexes i
  JOIN pg_class c ON c.relname = i.indexname
  JOIN pg_index x ON x.indexrelid = c.oid
  JOIN pg_attribute a ON a.attrelid = x.indrelid
 WHERE i.schemaname = 'public'
   AND i.tablename = 'feeds'
   AND a.attname IN (
       'last_heartbeat',
       'unclaimed_since',
       'worker_id',
       'fencing_token',
       'last_processed_filename',
       'last_bookmark_time',
       'failure_count',
       'retry_after'
   )
   AND a.attnum = ANY(x.indkey)
   AND i.indexname <> 'idx_feeds_failing_retryable';
