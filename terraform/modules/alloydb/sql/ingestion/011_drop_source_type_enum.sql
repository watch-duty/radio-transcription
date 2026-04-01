-- Remove the redundant source_type_enum column.
--
-- Source type is now represented by the existing source_type TEXT column,
-- which references source_types(slug).  Application code uses the
-- SourceType StrEnum in backend/pipeline/storage/feed_store.py, whose
-- values correspond directly to those slugs.
--
-- Idempotent: DROP COLUMN IF EXISTS allows safe re-application during
-- Terraform runs.
ALTER TABLE feeds DROP COLUMN IF EXISTS source_type_enum;
