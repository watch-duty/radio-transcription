-- Column for the progress-write path (UPDATE_PROGRESS_SQL post-Phase-0).
-- Deliberately NOT indexed: mutated ~1,200 times/sec at peak. Keeps progress
-- writes fully HOT-eligible and removes them from last_heartbeat's write
-- path. Idempotent: ADD COLUMN IF NOT EXISTS.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS last_progress_at TIMESTAMP WITH TIME ZONE;
