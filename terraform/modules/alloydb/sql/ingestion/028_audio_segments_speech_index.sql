-- Create a partial index on audio_segments for SPEECH classification to optimize feed timestamp querying.
CREATE INDEX IF NOT EXISTS idx_audio_segments_last_speech
    ON audio_segments (feed_id, end_timestamp DESC, id DESC)
    WHERE classification = 'SPEECH';
