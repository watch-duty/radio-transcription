-- Idempotent: PostgreSQL does not support CREATE TYPE IF NOT EXISTS,
-- so we check pg_type directly before creating the enum.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'feed_status') THEN
        -- Keep in sync with frontend/common/src/types/feeds.ts
        CREATE TYPE feed_status AS ENUM (
            'unclaimed',
            'active',
            'failing',
            'quarantined',
            'deactivated'
        );
    END IF;
END
$$;
