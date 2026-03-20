-- Idempotent: ADD COLUMN IF NOT EXISTS allows safe re-application during Terraform runs.
ALTER TABLE feeds ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0;
