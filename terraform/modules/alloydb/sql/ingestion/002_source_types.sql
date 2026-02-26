CREATE TABLE source_types (
    slug        TEXT PRIMARY KEY,
    description TEXT,
    requires_vad BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
