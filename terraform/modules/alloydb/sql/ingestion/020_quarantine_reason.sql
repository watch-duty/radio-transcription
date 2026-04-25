-- Add quarantine_reason column to feeds for operator triage attribution.
-- Populated by REPORT_FAILURE_SQL (Phase 2) when a feed transitions to
-- 'quarantined', storing the latest SourceError.reason string.
-- NULL when the feed has never been quarantined.
-- No CHECK constraint: reason strings are free-form per-source (per D-02).
-- No index: read on individual rows during triage, not in hot-path queries (per D-03).
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;
