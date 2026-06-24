-- Drop the deprecated pre-audit diagnostic-detail column from existing DBs.
ALTER TABLE feeds
    DROP COLUMN IF EXISTS quarantine_reason;
