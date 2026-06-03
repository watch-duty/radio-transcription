"""SQL query constants for TranscriptStore."""

TRANSCRIPT_COLUMNS_SQL = """\
    segment_id,
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
    playback_audio_uri,
    evaluation_errors,
    evaluation_annotations,
    created_at
"""

CREATE_TRANSCRIPT_SQL = (
    """\
INSERT INTO transcripts (
    segment_id,
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
    playback_audio_uri,
    evaluation_errors,
    evaluation_annotations
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (segment_id) DO UPDATE
SET segment_id = transcripts.segment_id
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
WHERE segment_id = $1
"""
)

# Fetches transcripts for a specific feed, ordered from newest to oldest.
# Keyset pagination: $2 (timestamp) and $3 (segment_id) define the cursor.
# We fetch records that are older than the cursor (i.e., strictly less than).
# $4 (start_time) and $5 (end_time) define the time window.
GET_TRANSCRIPTS_BY_FEED_SQL = (
    """\
SELECT
"""
    + TRANSCRIPT_COLUMNS_SQL
    + """\
FROM transcripts
WHERE feed_id = $1
  AND ($2::timestamptz IS NULL OR end_timestamp < $2 OR (end_timestamp = $2 AND segment_id < $3))
  AND ($4::timestamptz IS NULL OR end_timestamp >= $4)
  AND ($5::timestamptz IS NULL OR end_timestamp <= $5)
  AND ($6::boolean IS NULL OR (cardinality(evaluation_decisions) > 0) = $6::boolean)
ORDER BY end_timestamp DESC, segment_id DESC
LIMIT $7
"""
)

GET_TRANSCRIPTS_BY_FEED_ASC_SQL = (
    """\
SELECT
"""
    + TRANSCRIPT_COLUMNS_SQL
    + """\
FROM transcripts
WHERE feed_id = $1
  AND ($2::timestamptz IS NULL OR end_timestamp > $2 OR (end_timestamp = $2 AND segment_id > $3))
  AND ($4::timestamptz IS NULL OR end_timestamp >= $4)
  AND ($5::timestamptz IS NULL OR end_timestamp <= $5)
  AND ($6::boolean IS NULL OR (cardinality(evaluation_decisions) > 0) = $6::boolean)
ORDER BY end_timestamp ASC, segment_id ASC
LIMIT $7
"""
)

# Fetches transcripts across all feeds, ordered from newest to oldest.
# Keyset pagination: $1 (timestamp) and $2 (segment_id) define the cursor.
# We fetch records that are older than the cursor (i.e., strictly less than).
# $3 (start_time) and $4 (end_time) define the time window.
LIST_TRANSCRIPTS_SQL = (
    """\
SELECT
"""
    + TRANSCRIPT_COLUMNS_SQL
    + """\
FROM transcripts
WHERE ($1::timestamptz IS NULL OR end_timestamp < $1 OR (end_timestamp = $1 AND segment_id < $2))
  AND ($3::timestamptz IS NULL OR end_timestamp >= $3)
  AND ($4::timestamptz IS NULL OR end_timestamp <= $4)
  AND ($5::boolean IS NULL OR (cardinality(evaluation_decisions) > 0) = $5::boolean)
ORDER BY end_timestamp DESC, segment_id DESC
LIMIT $6
"""
)

LIST_TRANSCRIPTS_ASC_SQL = (
    """\
SELECT
"""
    + TRANSCRIPT_COLUMNS_SQL
    + """\
FROM transcripts
WHERE ($1::timestamptz IS NULL OR end_timestamp > $1 OR (end_timestamp = $1 AND segment_id > $2))
  AND ($3::timestamptz IS NULL OR end_timestamp >= $3)
  AND ($4::timestamptz IS NULL OR end_timestamp <= $4)
  AND ($5::boolean IS NULL OR (cardinality(evaluation_decisions) > 0) = $5::boolean)
ORDER BY end_timestamp ASC, segment_id ASC
LIMIT $6
"""
)

DELETE_TRANSCRIPT_SQL = """\
DELETE FROM transcripts
WHERE segment_id = $1
"""
