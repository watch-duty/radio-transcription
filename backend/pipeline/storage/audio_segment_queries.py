"""SQL query constants for AudioSegmentStore."""

LIST_AUDIO_SEGMENTS_DESC_SQL = """
SELECT
    s.id,
    s.feed_id,
    s.classification,
    s.start_timestamp,
    s.end_timestamp,
    s.missing_prior_context,
    s.missing_post_context,
    s.source_audio_uris,
    s.canonical_audio_uri,
    s.start_audio_offset,
    s.end_audio_offset,
    s.playback_audio_uri,
    s.external_audio_segment_id,
    s.created_at,
    COALESCE(a.annotations, '[]'::json) AS annotations
FROM audio_segments s
LEFT JOIN (
    SELECT audio_segment_id,
           json_agg(
               json_build_object(
                   'audio_segment_id', audio_segment_id,
                   'type', type,
                   'data', data,
                   'created_at', created_at,
                   'updated_at', updated_at
               )
           ) AS annotations,
           bool_or(type = 'EVALUATION' AND jsonb_array_length(data->'decisions') > 0) AS is_alert
    FROM annotations
    GROUP BY audio_segment_id
) a ON s.id = a.audio_segment_id
WHERE ($1::uuid[] IS NULL OR s.feed_id = ANY($1))
  AND ($2::timestamptz IS NULL OR s.end_timestamp < $2 OR (s.end_timestamp = $2 AND s.id < $3))
  AND ($4::timestamptz IS NULL OR s.end_timestamp >= $4)
  AND ($5::timestamptz IS NULL OR s.end_timestamp <= $5)
  AND ($6::boolean IS NULL OR COALESCE(a.is_alert, False) = $6::boolean)
  AND ($7::uuid[] IS NULL OR s.id = ANY($7))
ORDER BY s.end_timestamp DESC, s.id DESC
LIMIT $8
"""

LIST_AUDIO_SEGMENTS_ASC_SQL = """
SELECT
    s.id,
    s.feed_id,
    s.classification,
    s.start_timestamp,
    s.end_timestamp,
    s.missing_prior_context,
    s.missing_post_context,
    s.source_audio_uris,
    s.canonical_audio_uri,
    s.start_audio_offset,
    s.end_audio_offset,
    s.playback_audio_uri,
    s.external_audio_segment_id,
    s.created_at,
    COALESCE(a.annotations, '[]'::json) AS annotations
FROM audio_segments s
LEFT JOIN (
    SELECT audio_segment_id,
           json_agg(
               json_build_object(
                   'audio_segment_id', audio_segment_id,
                   'type', type,
                   'data', data,
                   'created_at', created_at,
                   'updated_at', updated_at
               )
           ) AS annotations,
           bool_or(type = 'EVALUATION' AND jsonb_array_length(data->'decisions') > 0) AS is_alert
    FROM annotations
    GROUP BY audio_segment_id
) a ON s.id = a.audio_segment_id
WHERE ($1::uuid[] IS NULL OR s.feed_id = ANY($1))
  AND ($2::timestamptz IS NULL OR s.end_timestamp > $2 OR (s.end_timestamp = $2 AND s.id > $3))
  AND ($4::timestamptz IS NULL OR s.end_timestamp >= $4)
  AND ($5::timestamptz IS NULL OR s.end_timestamp <= $5)
  AND ($6::boolean IS NULL OR COALESCE(a.is_alert, False) = $6::boolean)
  AND ($7::uuid[] IS NULL OR s.id = ANY($7))
ORDER BY s.end_timestamp ASC, s.id ASC
LIMIT $8
"""

# Filters on end_timestamp; with feed_ids this rides
# idx_audio_segments_feed_pagination. Cursor ($2,$3) resumes the next page.
# is_alert uses a LATERAL aggregate scoped to each in-window segment via the
# annotations PK, not a full-table GROUP BY.
LIST_AUDIO_SEGMENT_SUMMARIES_IN_WINDOW_SQL = """
SELECT
    s.id,
    s.feed_id,
    s.start_timestamp,
    s.end_timestamp,
    s.classification,
    COALESCE(a.is_alert, False) AS is_alert
FROM audio_segments s
LEFT JOIN LATERAL (
    SELECT bool_or(type = 'EVALUATION' AND jsonb_array_length(data->'decisions') > 0) AS is_alert
    FROM annotations
    WHERE audio_segment_id = s.id
) a ON TRUE
WHERE ($1::uuid[] IS NULL OR s.feed_id = ANY($1))
  AND ($2::timestamptz IS NULL OR s.end_timestamp < $2 OR (s.end_timestamp = $2 AND s.id < $3))
  AND s.end_timestamp >= $4
  AND s.end_timestamp <= $5
  AND ($6::boolean IS NULL OR COALESCE(a.is_alert, False) = $6::boolean)
ORDER BY s.end_timestamp DESC, s.id DESC
LIMIT $7
"""

ADD_ANNOTATION_SQL = """
INSERT INTO annotations (audio_segment_id, type, data)
VALUES ($1, $2, $3)
ON CONFLICT (audio_segment_id, type) DO UPDATE
SET audio_segment_id = annotations.audio_segment_id
RETURNING audio_segment_id, type, data, created_at, updated_at
"""

CREATE_AUDIO_SEGMENT_SQL = """
INSERT INTO audio_segments (
    id,
    feed_id,
    classification,
    start_timestamp,
    end_timestamp,
    missing_prior_context,
    missing_post_context,
    source_audio_uris,
    canonical_audio_uri,
    start_audio_offset,
    end_audio_offset,
    playback_audio_uri,
    external_audio_segment_id
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (id) DO UPDATE
SET id = audio_segments.id
RETURNING
    id,
    feed_id,
    classification,
    start_timestamp,
    end_timestamp,
    missing_prior_context,
    missing_post_context,
    source_audio_uris,
    canonical_audio_uri,
    start_audio_offset,
    end_audio_offset,
    playback_audio_uri,
    external_audio_segment_id,
    created_at
"""
