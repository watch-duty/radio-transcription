CREATE TABLE source_types (
    slug        TEXT PRIMARY KEY,
    description TEXT,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
