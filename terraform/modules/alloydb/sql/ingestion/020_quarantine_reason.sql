-- Add quarantine_reason column to feeds for operator triage.
-- Populated by REPORT_FAILURE_SQL when a feed transitions to 'quarantined',
-- storing the reason string passed by the catch-all handler in
-- collector_runtime._process_feed (typically str(exc) or type(exc).__name__).
-- NULL when the feed has never been quarantined.
-- No CHECK constraint: reason strings are free-form.
-- No index: read on individual rows during triage, not in hot-path queries.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;
