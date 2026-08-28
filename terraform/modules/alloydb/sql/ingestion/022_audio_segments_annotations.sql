-- Idempotent: DO block to safely define the AUDIO_CLASSIFICATION ENUM.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'audio_classification') THEN
        CREATE TYPE AUDIO_CLASSIFICATION AS ENUM (
            'UNSPECIFIED',
            'SPEECH',
            'OTHER',
            'SPEECH_DETECTED',
            'UNCLASSIFIED'
        );
    END IF;
END
$$;

-- Create audio_segments table with metadata matching legacy transcripts structure.
CREATE TABLE IF NOT EXISTS audio_segments (
    id                      UUID PRIMARY KEY,
    feed_id                 UUID NOT NULL REFERENCES feeds(id),
    classification          AUDIO_CLASSIFICATION NOT NULL,
    start_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,
    end_timestamp           TIMESTAMP WITH TIME ZONE NOT NULL,
    missing_prior_context   BOOLEAN NOT NULL DEFAULT FALSE,
    missing_post_context    BOOLEAN NOT NULL DEFAULT FALSE,
    source_audio_uris       TEXT[] NOT NULL DEFAULT '{}',
    canonical_audio_uri     TEXT,
    start_audio_offset      INTERVAL,
    end_audio_offset        INTERVAL,
    playback_audio_uri      TEXT,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create EAV style annotations table.
CREATE TABLE IF NOT EXISTS annotations (
    audio_segment_id        UUID NOT NULL REFERENCES audio_segments(id) ON DELETE CASCADE,
    type                    VARCHAR(50) NOT NULL, -- e.g., 'TRANSCRIPTION', 'EVALUATION'
    data                    JSONB NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Enforce only one annotation of a specific type per segment
    PRIMARY KEY (audio_segment_id, type)
);

-- Composite index on audio_segments for high-performance feed-based keyset pagination.
CREATE INDEX IF NOT EXISTS idx_audio_segments_feed_pagination
    ON audio_segments (feed_id, end_timestamp DESC, id DESC);

-- Disabled: dropped in 037 (GOO-761).
--
-- CREATE INDEX IF NOT EXISTS idx_annotations_data
--     ON annotations USING GIN (data);
