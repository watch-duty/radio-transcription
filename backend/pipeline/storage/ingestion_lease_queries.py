"""Static SQL for fenced ingestion Lease control operations."""

CLAIM_UNCLAIMED_LEASES_SQL = """\
WITH candidates AS MATERIALIZED (
    SELECT source_type, lease_key
    FROM public.ingestion_leases
    WHERE source_type = $1
      AND status = 'unclaimed'::public.feed_status
    ORDER BY source_type, lease_key
    LIMIT $3
    FOR NO KEY UPDATE SKIP LOCKED
),
claimed AS (
    UPDATE public.ingestion_leases AS leases
    SET status = 'active'::public.feed_status,
        worker_id = $2,
        fencing_token = leases.fencing_token + 1,
        last_heartbeat = NOW(),
        retry_after = NULL,
        updated_at = NOW()
    FROM candidates
    WHERE leases.source_type = candidates.source_type
      AND leases.lease_key = candidates.lease_key
    RETURNING
        leases.source_type,
        leases.lease_key,
        leases.status::text AS status,
        leases.worker_id,
        leases.fencing_token,
        leases.failure_count,
        leases.status_reason
)
SELECT *
FROM claimed
ORDER BY source_type, lease_key
"""


CLAIM_RECOVERABLE_LEASES_SQL = """\
WITH candidates AS MATERIALIZED (
    SELECT
        source_type,
        lease_key,
        CASE
            WHEN status = 'failing'::public.feed_status THEN 0
            ELSE 1
        END AS recovery_priority
    FROM public.ingestion_leases
    WHERE source_type = $1
      AND (
          (
              status = 'failing'::public.feed_status
              AND worker_id IS NULL
              AND last_heartbeat IS NULL
              AND (retry_after IS NULL OR retry_after <= NOW())
          )
          OR (
              status = 'active'::public.feed_status
              AND last_heartbeat < NOW() - $4::interval
          )
      )
    ORDER BY recovery_priority, source_type, lease_key
    LIMIT $3
    FOR NO KEY UPDATE SKIP LOCKED
),
claimed AS (
    UPDATE public.ingestion_leases AS leases
    SET status = 'active'::public.feed_status,
        worker_id = $2,
        fencing_token = leases.fencing_token + 1,
        last_heartbeat = NOW(),
        retry_after = NULL,
        updated_at = NOW()
    FROM candidates
    WHERE leases.source_type = candidates.source_type
      AND leases.lease_key = candidates.lease_key
    RETURNING
        leases.source_type,
        leases.lease_key,
        leases.status::text AS status,
        leases.worker_id,
        leases.fencing_token,
        leases.failure_count,
        leases.status_reason
)
SELECT *
FROM claimed
ORDER BY source_type, lease_key
"""


RENEW_LEASE_HEARTBEATS_SQL = """\
WITH input AS MATERIALIZED (
    SELECT
        input_values.source_type,
        input_values.lease_key,
        input_values.owner_worker_id,
        input_values.requested_fencing_token,
        input_values.caller_ordinal
    FROM UNNEST(
        $1::text[],
        $2::text[],
        $3::uuid[],
        $4::bigint[],
        $5::bigint[]
    ) AS input_values(
        source_type,
        lease_key,
        owner_worker_id,
        requested_fencing_token,
        caller_ordinal
    )
),
current_state AS MATERIALIZED (
    SELECT
        input.caller_ordinal,
        input.owner_worker_id,
        input.requested_fencing_token,
        leases.source_type,
        leases.lease_key,
        leases.status,
        leases.worker_id,
        leases.fencing_token
    FROM input
    JOIN public.ingestion_leases AS leases
      ON leases.source_type = input.source_type
     AND leases.lease_key = input.lease_key
    ORDER BY leases.source_type, leases.lease_key
    FOR NO KEY UPDATE OF leases
),
renewed AS (
    UPDATE public.ingestion_leases AS leases
    SET last_heartbeat = NOW(),
        updated_at = NOW()
    FROM current_state
    WHERE leases.source_type = current_state.source_type
      AND leases.lease_key = current_state.lease_key
      AND current_state.status = 'active'::public.feed_status
      AND current_state.worker_id = current_state.owner_worker_id
      AND current_state.fencing_token =
          current_state.requested_fencing_token
    -- The returned identity is only the per-input applied marker.
    RETURNING
        leases.source_type,
        leases.lease_key
)
-- Rejections need only the locked grant fields required for classification.
SELECT
    input.caller_ordinal,
    input.source_type,
    input.lease_key,
    current_state.status::text AS status,
    current_state.worker_id,
    current_state.fencing_token,
    renewed.source_type IS NOT NULL AS applied
FROM input
LEFT JOIN current_state
  ON current_state.caller_ordinal = input.caller_ordinal
LEFT JOIN renewed
  ON renewed.source_type = input.source_type
 AND renewed.lease_key = input.lease_key
ORDER BY input.caller_ordinal
"""


RELEASE_LEASE_SQL = """\
WITH current_state AS MATERIALIZED (
    SELECT
        source_type,
        lease_key,
        status,
        worker_id,
        fencing_token,
        failure_count,
        status_reason
    FROM public.ingestion_leases
    WHERE source_type = $1
      AND lease_key = $2
    FOR NO KEY UPDATE
),
released AS (
    UPDATE public.ingestion_leases AS leases
    SET status = 'unclaimed'::public.feed_status,
        worker_id = NULL,
        last_heartbeat = NULL,
        updated_at = NOW()
    FROM current_state
    WHERE leases.source_type = current_state.source_type
      AND leases.lease_key = current_state.lease_key
      AND current_state.status = 'active'::public.feed_status
      AND current_state.worker_id = $3
      AND current_state.fencing_token = $4
    RETURNING
        leases.source_type,
        leases.lease_key,
        leases.status
)
-- Owner and fence classify rejection; status and failure evidence are public.
SELECT
    current_state.source_type,
    current_state.lease_key,
    CASE
        WHEN released.source_type IS NOT NULL THEN released.status::text
        ELSE current_state.status::text
    END AS status,
    current_state.worker_id,
    current_state.fencing_token,
    current_state.failure_count,
    current_state.status_reason,
    released.source_type IS NOT NULL AS applied
FROM current_state
LEFT JOIN released
  ON released.source_type = current_state.source_type
 AND released.lease_key = current_state.lease_key
"""


FINALIZE_BUDGETED_FAILURE_SQL = """\
WITH current_state AS MATERIALIZED (
    SELECT
        source_type,
        lease_key,
        status,
        worker_id,
        fencing_token,
        failure_count
    FROM public.ingestion_leases
    WHERE source_type = $1
      AND lease_key = $2
    FOR NO KEY UPDATE
),
updated AS (
    UPDATE public.ingestion_leases AS leases
    SET status = CASE
            WHEN leases.failure_count + 1 >= $5
                THEN 'quarantined'::public.feed_status
            ELSE 'failing'::public.feed_status
        END,
        worker_id = NULL,
        last_heartbeat = NULL,
        failure_count = leases.failure_count + 1,
        retry_after = CASE
            WHEN leases.failure_count + 1 >= $5 THEN NULL
            ELSE NOW()
                + INTERVAL '1 second' * LEAST(
                    $7::double precision,
                    $6::double precision * POWER(
                        2::double precision,
                        LEAST(leases.failure_count, 30)::double precision
                    )
                )
                + (RANDOM() * INTERVAL '10 seconds')
        END,
        status_reason = $8,
        status_reason_detail = $9,
        updated_at = NOW()
    FROM current_state
    WHERE leases.source_type = current_state.source_type
      AND leases.lease_key = current_state.lease_key
      AND current_state.status = 'active'::public.feed_status
      AND current_state.worker_id = $3
      AND current_state.fencing_token = $4
    RETURNING
        leases.source_type,
        leases.lease_key,
        leases.status
)
SELECT
    current_state.source_type,
    current_state.lease_key,
    current_state.status::text AS status,
    current_state.worker_id,
    current_state.fencing_token,
    updated.status::text AS final_status,
    updated.source_type IS NOT NULL AS applied
FROM current_state
LEFT JOIN updated
  ON updated.source_type = current_state.source_type
 AND updated.lease_key = current_state.lease_key
"""


FINALIZE_NON_BUDGETED_FAILURE_SQL = """\
WITH current_state AS MATERIALIZED (
    SELECT
        source_type,
        lease_key,
        status,
        worker_id,
        fencing_token
    FROM public.ingestion_leases
    WHERE source_type = $1
      AND lease_key = $2
    FOR NO KEY UPDATE
),
updated AS (
    UPDATE public.ingestion_leases AS leases
    SET status = 'failing'::public.feed_status,
        worker_id = NULL,
        last_heartbeat = NULL,
        failure_count = 0,
        retry_after = $5,
        status_reason = $6,
        status_reason_detail = $7,
        updated_at = NOW()
    FROM current_state
    WHERE leases.source_type = current_state.source_type
      AND leases.lease_key = current_state.lease_key
      AND current_state.status = 'active'::public.feed_status
      AND current_state.worker_id = $3
      AND current_state.fencing_token = $4
    RETURNING
        leases.source_type,
        leases.lease_key,
        leases.status
)
SELECT
    current_state.source_type,
    current_state.lease_key,
    current_state.status::text AS status,
    current_state.worker_id,
    current_state.fencing_token,
    updated.status::text AS final_status,
    updated.source_type IS NOT NULL AS applied
FROM current_state
LEFT JOIN updated
  ON updated.source_type = current_state.source_type
 AND updated.lease_key = current_state.lease_key
"""


LOCK_LEASE_SQL = """\
SELECT
    source_type,
    lease_key,
    status::text AS status,
    worker_id,
    fencing_token,
    last_heartbeat,
    failure_count,
    retry_after,
    status_reason,
    status_reason_detail,
    membership_revision,
    updated_at
FROM public.ingestion_leases
WHERE source_type = $1
  AND lease_key = $2
FOR NO KEY UPDATE
"""


LOAD_BCFY_CALLS_MEMBERSHIP_SQL = """\
SELECT
    fp.feed_id,
    fp.source_type AS property_source_type,
    feeds.source_type AS feed_source_type,
    fp.source_feed_id,
    fp.bcfy_calls_sid AS sid,
    fp.bcfy_calls_group_id AS group_id,
    feeds.status::text AS status,
    feeds.last_bookmark_time,
    feeds.failure_count,
    feeds.retry_after,
    feeds.status_reason,
    feeds.status_reason_detail
FROM public.feed_properties AS fp
LEFT JOIN public.feeds AS feeds
  ON feeds.id = fp.feed_id
WHERE fp.source_type = 'bcfy_calls'
  AND fp.bcfy_calls_is_trunked IS TRUE
  AND fp.bcfy_calls_sid = $1
ORDER BY fp.bcfy_calls_group_id, fp.feed_id
"""
