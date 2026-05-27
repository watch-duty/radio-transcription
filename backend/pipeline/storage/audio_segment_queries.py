"""SQL query constants for AudioSegmentStore."""

LIST_AUDIO_SEGMENTS_SQL = """
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
    s.created_at,
    COALESCE(
        json_agg(
            json_build_object(
                'audio_segment_id', a.audio_segment_id,
                'type', a.type,
                'data', a.data,
                'created_at', a.created_at,
                'updated_at', a.updated_at
            )
        ) FILTER (WHERE a.type IS NOT NULL),
        '[]'::json
    ) AS annotations
FROM audio_segments s
LEFT JOIN annotations a ON s.id = a.audio_segment_id
WHERE $1::uuid[] IS NULL OR s.feed_id = ANY($1)
GROUP BY s.id
"""

ADD_ANNOTATION_SQL = """
INSERT INTO annotations (audio_segment_id, type, data)
VALUES ($1, $2, $3)
RETURNING audio_segment_id, type, data, created_at, updated_at
"""

CREATE_AUDIO_SEGMENT_SQL = """
INSERT INTO audio_segments (
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
    playback_audio_uri
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING id, feed_id, classification, start_timestamp, end_timestamp, missing_prior_context, missing_post_context, source_audio_uris, canonical_audio_uri, start_audio_offset, end_audio_offset, playback_audio_uri, created_at
"""
