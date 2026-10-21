-- Relation identity snapshot. CI runs this after each replay pass of
-- sql/ingestion/*.sql and diffs the two outputs; any difference fails the
-- build.
--
-- Invariant: replaying the migration set against an already-migrated
-- database must not recreate any table storage or index. A migration that
-- applies without error can still do expensive work on every deploy:
--
--   - ALTER COLUMN ... TYPE takes an ACCESS EXCLUSIVE lock and drops and
--     recreates every index that references the column, even when the column
--     is already the target type. An index with an expression or predicate
--     (such as a partial GIN index) cannot reuse its storage and is rebuilt
--     serially while the lock is held. A cast that is not a no-op also
--     rewrites the heap, which changes the table's relfilenode.
--   - An unguarded DROP INDEX / CREATE INDEX pair rebuilds the index and
--     assigns it a new OID.
--
-- Each case shows up here as a changed row between passes. Static scans of
-- the SQL text for CREATE/DROP INDEX cannot see the first case, which is why
-- this check looks at the catalog instead.
SELECT
    c.relkind,
    c.relname,
    c.oid,
    c.relfilenode
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'public'
   AND c.relkind IN ('r', 'i')
 ORDER BY c.relkind, c.relname;
