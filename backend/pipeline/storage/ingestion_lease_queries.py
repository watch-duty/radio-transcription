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
        unclaimed_since = NULL,
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
        leases.last_heartbeat,
        leases.failure_count,
        leases.retry_after,
        leases.status_reason,
        leases.status_reason_detail,
        leases.status_reason_updated_at,
        leases.audit_revision,
        leases.membership_revision,
        leases.updated_at
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
        unclaimed_since = NULL,
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
        leases.last_heartbeat,
        leases.failure_count,
        leases.retry_after,
        leases.status_reason,
        leases.status_reason_detail,
        leases.status_reason_updated_at,
        leases.audit_revision,
        leases.membership_revision,
        leases.updated_at
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
        input_values.caller_ordinal,
        input_values.lock_ordinal
    FROM UNNEST(
        $1::text[],
        $2::text[],
        $3::uuid[],
        $4::bigint[],
        $5::bigint[]
    ) WITH ORDINALITY AS input_values(
        source_type,
        lease_key,
        owner_worker_id,
        requested_fencing_token,
        caller_ordinal,
        lock_ordinal
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
        leases.fencing_token,
        leases.last_heartbeat,
        leases.failure_count,
        leases.retry_after,
        leases.status_reason,
        leases.status_reason_detail,
        leases.status_reason_updated_at,
        leases.audit_revision,
        leases.membership_revision,
        leases.updated_at
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
    RETURNING
        leases.source_type,
        leases.lease_key,
        leases.status,
        leases.worker_id,
        leases.fencing_token,
        leases.last_heartbeat,
        leases.failure_count,
        leases.retry_after,
        leases.status_reason,
        leases.status_reason_detail,
        leases.status_reason_updated_at,
        leases.audit_revision,
        leases.membership_revision,
        leases.updated_at
)
SELECT
    input.caller_ordinal,
    input.source_type,
    input.lease_key,
    CASE
        WHEN renewed.source_type IS NOT NULL THEN renewed.status::text
        ELSE current_state.status::text
    END AS status,
    CASE
        WHEN renewed.source_type IS NOT NULL THEN renewed.worker_id
        ELSE current_state.worker_id
    END AS worker_id,
    CASE
        WHEN renewed.source_type IS NOT NULL THEN renewed.fencing_token
        ELSE current_state.fencing_token
    END AS fencing_token,
    CASE
        WHEN renewed.source_type IS NOT NULL THEN renewed.last_heartbeat
        ELSE current_state.last_heartbeat
    END AS last_heartbeat,
    CASE
        WHEN renewed.source_type IS NOT NULL THEN renewed.failure_count
        ELSE current_state.failure_count
    END AS failure_count,
    CASE
        WHEN renewed.source_type IS NOT NULL THEN renewed.retry_after
        ELSE current_state.retry_after
    END AS retry_after,
    CASE
        WHEN renewed.source_type IS NOT NULL THEN renewed.status_reason
        ELSE current_state.status_reason
    END AS status_reason,
    CASE
        WHEN renewed.source_type IS NOT NULL
            THEN renewed.status_reason_detail
        ELSE current_state.status_reason_detail
    END AS status_reason_detail,
    CASE
        WHEN renewed.source_type IS NOT NULL
            THEN renewed.status_reason_updated_at
        ELSE current_state.status_reason_updated_at
    END AS status_reason_updated_at,
    CASE
        WHEN renewed.source_type IS NOT NULL THEN renewed.audit_revision
        ELSE current_state.audit_revision
    END AS audit_revision,
    CASE
        WHEN renewed.source_type IS NOT NULL
            THEN renewed.membership_revision
        ELSE current_state.membership_revision
    END AS membership_revision,
    CASE
        WHEN renewed.source_type IS NOT NULL THEN renewed.updated_at
        ELSE current_state.updated_at
    END AS updated_at,
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
        last_heartbeat,
        failure_count,
        retry_after,
        status_reason,
        status_reason_detail,
        status_reason_updated_at,
        audit_revision,
        membership_revision,
        updated_at
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
        unclaimed_since = NOW(),
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
        leases.status,
        leases.worker_id,
        leases.fencing_token,
        leases.last_heartbeat,
        leases.failure_count,
        leases.retry_after,
        leases.status_reason,
        leases.status_reason_detail,
        leases.status_reason_updated_at,
        leases.audit_revision,
        leases.membership_revision,
        leases.updated_at
)
SELECT
    current_state.source_type,
    current_state.lease_key,
    CASE
        WHEN released.source_type IS NOT NULL THEN released.status::text
        ELSE current_state.status::text
    END AS status,
    CASE
        WHEN released.source_type IS NOT NULL THEN released.worker_id
        ELSE current_state.worker_id
    END AS worker_id,
    CASE
        WHEN released.source_type IS NOT NULL THEN released.fencing_token
        ELSE current_state.fencing_token
    END AS fencing_token,
    CASE
        WHEN released.source_type IS NOT NULL THEN released.last_heartbeat
        ELSE current_state.last_heartbeat
    END AS last_heartbeat,
    CASE
        WHEN released.source_type IS NOT NULL THEN released.failure_count
        ELSE current_state.failure_count
    END AS failure_count,
    CASE
        WHEN released.source_type IS NOT NULL THEN released.retry_after
        ELSE current_state.retry_after
    END AS retry_after,
    CASE
        WHEN released.source_type IS NOT NULL THEN released.status_reason
        ELSE current_state.status_reason
    END AS status_reason,
    CASE
        WHEN released.source_type IS NOT NULL
            THEN released.status_reason_detail
        ELSE current_state.status_reason_detail
    END AS status_reason_detail,
    CASE
        WHEN released.source_type IS NOT NULL
            THEN released.status_reason_updated_at
        ELSE current_state.status_reason_updated_at
    END AS status_reason_updated_at,
    CASE
        WHEN released.source_type IS NOT NULL THEN released.audit_revision
        ELSE current_state.audit_revision
    END AS audit_revision,
    CASE
        WHEN released.source_type IS NOT NULL
            THEN released.membership_revision
        ELSE current_state.membership_revision
    END AS membership_revision,
    CASE
        WHEN released.source_type IS NOT NULL THEN released.updated_at
        ELSE current_state.updated_at
    END AS updated_at,
    released.source_type IS NOT NULL AS applied
FROM current_state
LEFT JOIN released
  ON released.source_type = current_state.source_type
 AND released.lease_key = current_state.lease_key
"""
