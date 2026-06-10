-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
ALTER TABLE audio_segments ADD COLUMN IF NOT EXISTS external_audio_segment_id TEXT;
