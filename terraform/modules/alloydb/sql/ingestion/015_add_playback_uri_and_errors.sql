-- Add missing columns to transcripts table
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS playback_audio_uri TEXT;
ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS evaluation_errors TEXT[] NOT NULL DEFAULT '{}';
