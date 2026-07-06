-- Create pg_trgm extension if not exists for trigram index.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create partial GIN trigram index on transcript annotations text for fast substring searching under high write throughput.
-- Set a larger gin_pending_list_limit (131072 = 128MB in kB) to allow autovacuum to merge pending entries in the background,
-- reducing insert latency and write amplification.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_annotations_transcript_text_trgm
ON annotations USING gin ((data->>'text') gin_trgm_ops)
WITH (gin_pending_list_limit = 131072)
WHERE type = 'TRANSCRIPT'::annotation_type;

-- Update existing index if it was created without the storage parameter.
ALTER INDEX IF EXISTS idx_annotations_transcript_text_trgm SET (gin_pending_list_limit = 131072);
