-- Ensure last_bookmark_time exists on pre-existing feeds tables as well.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS last_bookmark_time TIMESTAMP WITH TIME ZONE;