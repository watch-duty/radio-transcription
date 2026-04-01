-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
--
-- IMPORTANT: The slugs in this table must be kept in sync with the
-- SourceType StrEnum in backend/pipeline/storage/feed_store.py and with the
-- seed data in 006_seed_source_types.sql.
CREATE TABLE IF NOT EXISTS source_types (
    slug        TEXT PRIMARY KEY,
    description TEXT,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
