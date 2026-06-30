-- Create pg_trgm extension if not exists for trigram index.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create partial GIN trigram index on transcript annotations text for fast substring searching.
CREATE INDEX IF NOT EXISTS idx_annotations_transcript_text_trgm
ON annotations USING gin ((data->>'text') gin_trgm_ops)
WHERE type = 'TRANSCRIPT'::annotation_type;
