"""SQL query constants for TranscriptStore."""

TRANSCRIPT_COLUMNS_SQL = """\
    transmission_id,
    feed_id,
    transcript,
    start_timestamp,
    end_timestamp,
    missing_prior_context,
    missing_post_context,
    source_audio_uris,
    canonical_audio_uri,
    start_audio_offset,
    end_audio_offset,
    evaluation_decisions,
    created_at
"""

CREATE_TRANSCRIPT_SQL = (
    """\
INSERT INTO transcripts (
    transmission_id,
    feed_id,
    transcript,
    start_timestamp,
    end_timestamp,
    missing_prior_context,
    missing_post_context,
    source_audio_uris,
    canonical_audio_uri,
    start_audio_offset,
    end_audio_offset,
    evaluation_decisions
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
RETURNING
"""
    + TRANSCRIPT_COLUMNS_SQL
)

GET_TRANSCRIPT_SQL = (
    """\
SELECT
"""
    + TRANSCRIPT_COLUMNS_SQL
    + """\
FROM transcripts
WHERE transmission_id = $1
"""
)

GET_TRANSCRIPTS_BY_FEED_SQL = (
    """\
SELECT
"""
    + TRANSCRIPT_COLUMNS_SQL
    + """\
FROM transcripts
WHERE feed_id = $1
ORDER BY end_timestamp DESC
"""
)

LIST_TRANSCRIPTS_SQL = (
    """\
SELECT
"""
    + TRANSCRIPT_COLUMNS_SQL
    + """\
FROM transcripts
ORDER BY end_timestamp DESC
"""
)

DELETE_TRANSCRIPT_SQL = """\
DELETE FROM transcripts
WHERE transmission_id = $1
"""
