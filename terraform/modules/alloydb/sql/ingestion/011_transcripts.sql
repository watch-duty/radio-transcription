-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
CREATE TABLE IF NOT EXISTS transcripts (
    transmission_id         UUID PRIMARY KEY,
    feed_id                 UUID NOT NULL REFERENCES feeds(id),
    transcript              TEXT NOT NULL,
    start_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,
    end_timestamp           TIMESTAMP WITH TIME ZONE NOT NULL,
    missing_prior_context   BOOLEAN NOT NULL DEFAULT FALSE,
    missing_post_context    BOOLEAN NOT NULL DEFAULT FALSE,
    source_audio_uris       TEXT[] NOT NULL DEFAULT '{}',
    canonical_audio_uri     TEXT,
    start_audio_offset      INTERVAL,
    end_audio_offset        INTERVAL,
    -- For now storing the triggered rules together.
    -- If this becomes too large, we can migrate this to a separate join table.
    evaluation_decisions    TEXT[] NOT NULL DEFAULT '{}',
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
