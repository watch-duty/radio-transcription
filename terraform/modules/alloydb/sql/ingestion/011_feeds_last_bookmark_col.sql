-- Ensure last_bookmark exists on pre-existing feeds tables as well.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS last_bookmark TIMESTAMP WITH TIME ZONE;