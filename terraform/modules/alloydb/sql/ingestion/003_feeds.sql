-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
CREATE TABLE IF NOT EXISTS feeds (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                    VARCHAR(255) NOT NULL UNIQUE,
    source_type             TEXT NOT NULL REFERENCES source_types(slug),
    status                  feed_status NOT NULL DEFAULT 'unclaimed'::feed_status,
    failure_count           INT NOT NULL DEFAULT 0,

    -- Dynamic leasing & state columns
    worker_id               UUID,
    last_heartbeat          TIMESTAMP WITH TIME ZONE,
    last_processed_filename TEXT,

    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
