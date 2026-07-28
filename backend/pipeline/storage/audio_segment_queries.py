"""SQL query constants for AudioSegmentStore."""

_BASE_LIST_AUDIO_SEGMENTS_SQL = """
WITH matched_segments AS (
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
        s.created_at
    FROM audio_segments s
    WHERE ($1::uuid[] IS NULL OR s.feed_id = ANY($1))
      AND (
          $2::timestamptz IS NULL
          OR s.end_timestamp {operator} $2
          OR (s.end_timestamp = $2 AND s.id {operator} $3)
      )
      AND ($4::timestamptz IS NULL OR s.end_timestamp >= $4)
      AND ($5::timestamptz IS NULL OR s.end_timestamp <= $5)
      AND ($6::boolean IS NULL OR (
          $6::boolean = EXISTS (
              SELECT 1 FROM annotations
              WHERE audio_segment_id = s.id
                AND type = 'EVALUATION'
                AND jsonb_array_length(data->'decisions') > 0
          )
      ))
      AND ($8::text IS NULL OR EXISTS (
          SELECT 1 FROM annotations
          WHERE audio_segment_id = s.id
            AND type = 'TRANSCRIPT'::annotation_type
            AND data->>'text' ILIKE '%' || $8 || '%'
      ))
    ORDER BY s.end_timestamp {direction}, s.id {direction}
    LIMIT $7
)
SELECT
    ms.*,
    COALESCE(
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'audio_segment_id', a.audio_segment_id,
                    'type', a.type,
                    'data', a.data,
                    'created_at', a.created_at,
                    'updated_at', a.updated_at
                )
            )
            FROM annotations a
            WHERE a.audio_segment_id = ms.id
        ),
        '[]'::jsonb
    ) AS annotations
FROM matched_segments ms
ORDER BY ms.end_timestamp {direction}, ms.id {direction}
"""

LIST_AUDIO_SEGMENTS_DESC_SQL = _BASE_LIST_AUDIO_SEGMENTS_SQL.format(
    operator="<",
    direction="DESC",
)

LIST_AUDIO_SEGMENTS_ASC_SQL = _BASE_LIST_AUDIO_SEGMENTS_SQL.format(
    operator=">",
    direction="ASC",
)

ADD_ANNOTATION_SQL = """
INSERT INTO annotations (audio_segment_id, type, data)
VALUES ($1, $2, $3)
ON CONFLICT (audio_segment_id, type) DO UPDATE
SET audio_segment_id = annotations.audio_segment_id
RETURNING audio_segment_id, type, data, created_at, updated_at
"""

FLAG_TRANSCRIPT_SQL = """
INSERT INTO annotations (audio_segment_id, type, data)
VALUES (
    $1, 
    'TRANSCRIPT_FLAG', 
    jsonb_build_object('flagged_by_user_ids', jsonb_build_array($2::text))
)
ON CONFLICT (audio_segment_id, type) DO UPDATE
SET 
  data = jsonb_set(
    annotations.data,
    '{flagged_by_user_ids}',
    (COALESCE(annotations.data->'flagged_by_user_ids', '[]'::jsonb) - $2::text) 
      || jsonb_build_array($2::text)
  ),
  updated_at = NOW()
RETURNING audio_segment_id, type, data, created_at, updated_at
"""

UNFLAG_TRANSCRIPT_SQL = """
INSERT INTO annotations (audio_segment_id, type, data)
VALUES ($1, 'TRANSCRIPT_FLAG', '{"flagged_by_user_ids": []}'::jsonb)
ON CONFLICT (audio_segment_id, type) DO UPDATE
SET 
  data = jsonb_set(
    annotations.data,
    '{flagged_by_user_ids}',
    COALESCE(annotations.data->'flagged_by_user_ids', '[]'::jsonb) - $2::text
  ),
  updated_at = NOW()
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
