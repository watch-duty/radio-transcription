-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
CREATE TABLE IF NOT EXISTS source_types (
    slug        TEXT PRIMARY KEY,
    description TEXT,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
