-- Add canonical feed status reason fields for operator triage.
-- Existing rows remain NULL until application code records or clears a reason.
-- status_reason_updated_at records the last status-reason observation or clear.
-- Known values are enforced by application code.
-- No index is added because this phase has no status-reason query path.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS status_reason TEXT;

ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS status_reason_updated_at TIMESTAMP WITH TIME ZONE;
