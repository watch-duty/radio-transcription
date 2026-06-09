-- Per-rule annotations describing how each triggered rule matched, keyed by
-- rule_id. Text-match rules carry span offsets and the matched substring;
-- future non-text rule kinds will carry their own annotation payload,
-- discriminated by which `*_match` field is present on each entry.
ALTER TABLE transcripts
    ADD COLUMN IF NOT EXISTS evaluation_annotations JSONB NOT NULL DEFAULT '{}';
