-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
CREATE TABLE IF NOT EXISTS rule_groups (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_name               VARCHAR(255) NOT NULL,
    description             TEXT,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    scope                   JSONB NOT NULL,
    operator                VARCHAR(50) NOT NULL,
    child_rule_ids          UUID[] NOT NULL DEFAULT '{}',
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by              TEXT NOT NULL
);
