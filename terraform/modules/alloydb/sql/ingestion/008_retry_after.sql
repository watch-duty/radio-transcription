-- Add retry_after column for exponential backoff on feed failures.
-- Feeds in 'failing' status with retry_after in the future are skipped
-- by the lease query until the backoff expires.
ALTER TABLE feeds ADD COLUMN IF NOT EXISTS retry_after TIMESTAMP WITH TIME ZONE;
