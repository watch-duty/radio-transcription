-- Consolidate idx_audio_segments_feed_pagination to include classification.
-- This optimizes both feed-based pagination and last-speech-segment querying,
-- while reducing index write amplification on the high-volume audio_segments table.

DROP INDEX IF EXISTS idx_audio_segments_feed_pagination;

CREATE INDEX IF NOT EXISTS idx_audio_segments_feed_pagination
    ON audio_segments (feed_id, end_timestamp DESC, id DESC, classification);
