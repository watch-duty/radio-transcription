-- Create pg_trgm extension if not exists for trigram index.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create partial GiST trigram index on transcript annotations text for fast substring searching under high write throughput.
CREATE INDEX IF NOT EXISTS idx_annotations_transcript_text_gist
ON annotations USING gist ((data->>'text') gist_trgm_ops)
WHERE type = 'TRANSCRIPT'::annotation_type;
