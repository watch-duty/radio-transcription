-- Removes the deprecated external_id column from feed_properties.
-- This field is superseded by the tags system (migration 021).
ALTER TABLE feed_properties DROP COLUMN IF EXISTS external_id;
