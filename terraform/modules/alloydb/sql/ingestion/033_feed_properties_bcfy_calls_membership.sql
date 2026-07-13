-- Maintained Broadcastify Calls membership metadata. Existing rows remain in
-- the valid legacy state because all three columns are nullable with no
-- default and this migration performs no data rewrite.
-- Bound ACCESS EXCLUSIVE lock acquisition so this migration fails instead of
-- queueing behind long-running work and blocking later feed_properties traffic.
BEGIN;
SET LOCAL lock_timeout = '5s';

ALTER TABLE public.feed_properties
    ADD COLUMN IF NOT EXISTS bcfy_calls_sid TEXT;

ALTER TABLE public.feed_properties
    ADD COLUMN IF NOT EXISTS bcfy_calls_group_id TEXT;

ALTER TABLE public.feed_properties
    ADD COLUMN IF NOT EXISTS bcfy_calls_is_trunked BOOLEAN;

-- PostgreSQL has no ADD CONSTRAINT IF NOT EXISTS, so ignore only the duplicate
-- name raised when this migration is replayed.
DO $migration$
BEGIN
    ALTER TABLE public.feed_properties
        ADD CONSTRAINT feed_properties_bcfy_calls_membership_check
        CHECK (
            CASE
                WHEN bcfy_calls_sid IS NULL
                 AND bcfy_calls_group_id IS NULL
                 AND bcfy_calls_is_trunked IS NULL
                    THEN TRUE
                WHEN source_type <> 'bcfy_calls'
                    THEN FALSE
                WHEN bcfy_calls_is_trunked IS TRUE
                    THEN bcfy_calls_sid IS NOT NULL
                     AND bcfy_calls_group_id IS NOT NULL
                     AND bcfy_calls_sid ~ '^[0-9]+$'
                     AND bcfy_calls_group_id ~ '^[0-9]+$'
                     AND source_feed_id =
                         bcfy_calls_sid || '-' || bcfy_calls_group_id
                WHEN bcfy_calls_is_trunked IS FALSE
                    THEN bcfy_calls_sid IS NULL
                     AND bcfy_calls_group_id IS NULL
                ELSE FALSE
            END
        ) NOT VALID;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$migration$;

COMMIT;
