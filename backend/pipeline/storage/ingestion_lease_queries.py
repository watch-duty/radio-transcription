"""Static SQL for fenced ingestion Lease control operations."""
# ruff: noqa: S608

from backend.pipeline.storage import feed_audit_sql

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
    status_reason_updated_at,
    audit_revision,
    membership_revision,
    updated_at
FROM public.ingestion_leases
WHERE source_type = $1
  AND lease_key = $2
FOR NO KEY UPDATE
"""


_FAILURE_RESULT_SELECT_SQL = """\
SELECT
    current_state.source_type,
    current_state.lease_key,
    current_state.status::text AS status,
    current_state.worker_id,
    current_state.fencing_token,
    current_state.last_heartbeat,
    current_state.failure_count,
    current_state.retry_after,
    current_state.status_reason,
    current_state.status_reason_detail,
    current_state.status_reason_updated_at,
    current_state.audit_revision,
    current_state.membership_revision,
    current_state.updated_at,
    CASE
        WHEN updated.source_type IS NOT NULL THEN updated.status::text
        ELSE current_state.status::text
    END AS after_status,
    CASE
        WHEN updated.source_type IS NOT NULL THEN updated.last_heartbeat
        ELSE current_state.last_heartbeat
    END AS after_last_heartbeat,
    CASE
        WHEN updated.source_type IS NOT NULL THEN updated.failure_count
        ELSE current_state.failure_count
    END AS after_failure_count,
    CASE
        WHEN updated.source_type IS NOT NULL THEN updated.retry_after
        ELSE current_state.retry_after
    END AS after_retry_after,
    CASE
        WHEN updated.source_type IS NOT NULL THEN updated.status_reason
        ELSE current_state.status_reason
    END AS after_status_reason,
    CASE
        WHEN updated.source_type IS NOT NULL
            THEN updated.status_reason_detail
        ELSE current_state.status_reason_detail
    END AS after_status_reason_detail,
    CASE
        WHEN updated.source_type IS NOT NULL
            THEN updated.status_reason_updated_at
        ELSE current_state.status_reason_updated_at
    END AS after_status_reason_updated_at,
    CASE
        WHEN updated.source_type IS NOT NULL THEN updated.audit_revision
        ELSE current_state.audit_revision
    END AS after_audit_revision,
    CASE
        WHEN updated.source_type IS NOT NULL
            THEN updated.membership_revision
        ELSE current_state.membership_revision
    END AS after_membership_revision,
    CASE
        WHEN updated.source_type IS NOT NULL THEN updated.updated_at
        ELSE current_state.updated_at
    END AS after_updated_at,
    updated.source_type IS NOT NULL AS applied
FROM current_state
LEFT JOIN updated
  ON updated.source_type = current_state.source_type
 AND updated.lease_key = current_state.lease_key
"""


FINALIZE_BUDGETED_FAILURE_SQL = (
    """\
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
                + LEAST(
                    $6 * INTERVAL '1 second',
                    $7 * INTERVAL '1 second'
                        * POWER(2, leases.failure_count)
                )
                + (RANDOM() * INTERVAL '10 seconds')
        END,
        unclaimed_since = NOW(),
        status_reason = $8,
        status_reason_detail = $9,
        status_reason_updated_at = NOW(),
        audit_revision = leases.audit_revision + 1,
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
"""
    + _FAILURE_RESULT_SELECT_SQL
)


FINALIZE_NON_BUDGETED_FAILURE_SQL = (
    """\
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
updated AS (
    UPDATE public.ingestion_leases AS leases
    SET status = 'failing'::public.feed_status,
        worker_id = NULL,
        last_heartbeat = NULL,
        failure_count = 0,
        retry_after = $5,
        unclaimed_since = NOW(),
        status_reason = $6,
        status_reason_detail = $7,
        status_reason_updated_at = NOW(),
        audit_revision = leases.audit_revision + 1,
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
"""
    + _FAILURE_RESULT_SELECT_SQL
)


LOAD_BCFY_CALLS_MEMBERSHIP_SQL = """\
SELECT
    fp.feed_id,
    fp.source_type AS property_source_type,
    feeds.source_type AS feed_source_type,
    fp.source_feed_id,
    fp.bcfy_calls_sid AS sid,
    fp.bcfy_calls_group_id AS group_id,
    feeds.status::text AS status,
    feeds.last_processed_filename,
    feeds.last_bookmark_time,
    feeds.failure_count,
    feeds.retry_after,
    feeds.status_reason,
    feeds.status_reason_detail,
    feeds.audit_revision
FROM public.feed_properties AS fp
LEFT JOIN public.feeds AS feeds
  ON feeds.id = fp.feed_id
WHERE fp.source_type = 'bcfy_calls'
  AND fp.bcfy_calls_is_trunked IS TRUE
  AND fp.bcfy_calls_sid = $1
ORDER BY fp.bcfy_calls_group_id, fp.feed_id
"""


LOCK_CHILD_FEEDS_SQL = """\
SELECT
    id,
    name,
    source_type,
    status::text AS status,
    last_processed_filename,
    last_bookmark_time,
    failure_count,
    retry_after,
    status_reason,
    status_reason_detail,
    status_reason_updated_at,
    audit_revision,
    created_at
FROM public.feeds
WHERE id = ANY($1::uuid[])
ORDER BY id
FOR NO KEY UPDATE
"""


APPLY_ADMITTED_PROGRESS_SQL = """\
WITH input AS MATERIALIZED (
    SELECT
        input_values.feed_id,
        input_values.last_processed_filename,
        input_values.cursor,
        input_values.write_cursor,
        input_values.write_path,
        input_values.clear_lifecycle,
        input_values.caller_ordinal,
        input_values.row_ordinal
    FROM UNNEST(
        $1::uuid[],
        $2::text[],
        $3::timestamptz[],
        $4::boolean[],
        $5::boolean[],
        $6::boolean[],
        $7::bigint[]
    ) WITH ORDINALITY AS input_values(
        feed_id,
        last_processed_filename,
        cursor,
        write_cursor,
        write_path,
        clear_lifecycle,
        caller_ordinal,
        row_ordinal
    )
),
updated AS (
    UPDATE public.feeds AS feeds
    SET last_processed_filename = CASE
            WHEN input.write_path THEN input.last_processed_filename
            ELSE feeds.last_processed_filename
        END,
        last_bookmark_time = CASE
            WHEN input.write_cursor
                THEN GREATEST(feeds.last_bookmark_time, input.cursor)
            ELSE feeds.last_bookmark_time
        END,
        status = CASE
            WHEN input.clear_lifecycle
             AND feeds.status IN (
                 'active'::public.feed_status,
                 'failing'::public.feed_status
             )
                THEN 'active'::public.feed_status
            ELSE feeds.status
        END,
        failure_count = CASE
            WHEN input.clear_lifecycle THEN 0
            ELSE feeds.failure_count
        END,
        retry_after = CASE
            WHEN input.clear_lifecycle THEN NULL
            ELSE feeds.retry_after
        END,
        status_reason = CASE
            WHEN input.clear_lifecycle THEN NULL
            ELSE feeds.status_reason
        END,
        status_reason_detail = CASE
            WHEN input.clear_lifecycle THEN NULL
            ELSE feeds.status_reason_detail
        END,
        status_reason_updated_at = CASE
            WHEN input.clear_lifecycle
             AND (
                 feeds.status_reason IS NOT NULL
                 OR feeds.status_reason_detail IS NOT NULL
             )
                THEN NOW()
            ELSE feeds.status_reason_updated_at
        END,
        audit_revision = feeds.audit_revision
            + CASE WHEN input.clear_lifecycle THEN 1 ELSE 0 END
    FROM input
    WHERE feeds.id = input.feed_id
      AND feeds.status IN (
          'active'::public.feed_status,
          'failing'::public.feed_status,
          'deactivated'::public.feed_status
      )
      AND (
          (
              input.write_cursor
              AND (
                  feeds.last_bookmark_time IS NULL
                  OR input.cursor > feeds.last_bookmark_time
              )
          )
          OR input.write_path
          OR input.clear_lifecycle
      )
    RETURNING
        input.caller_ordinal,
        feeds.id,
        feeds.name,
        feeds.source_type,
        feeds.status::text AS status,
        feeds.last_processed_filename,
        feeds.last_bookmark_time,
        feeds.failure_count,
        feeds.retry_after,
        feeds.status_reason,
        feeds.status_reason_detail,
        feeds.status_reason_updated_at,
        feeds.audit_revision,
        feeds.created_at
)
SELECT *
FROM updated
ORDER BY caller_ordinal
"""


APPLY_SOURCE_OBSERVATIONS_SQL = """\
WITH input AS MATERIALIZED (
    SELECT
        input_values.feed_id,
        input_values.cursor,
        input_values.write_cursor,
        input_values.clear_lifecycle,
        input_values.caller_ordinal,
        input_values.row_ordinal
    FROM UNNEST(
        $1::uuid[],
        $2::timestamptz[],
        $3::boolean[],
        $4::boolean[],
        $5::bigint[]
    ) WITH ORDINALITY AS input_values(
        feed_id,
        cursor,
        write_cursor,
        clear_lifecycle,
        caller_ordinal,
        row_ordinal
    )
),
updated AS (
    UPDATE public.feeds AS feeds
    SET last_bookmark_time = CASE
            WHEN input.write_cursor
                THEN GREATEST(feeds.last_bookmark_time, input.cursor)
            ELSE feeds.last_bookmark_time
        END,
        status = CASE
            WHEN input.clear_lifecycle THEN 'active'::public.feed_status
            ELSE feeds.status
        END,
        failure_count = CASE
            WHEN input.clear_lifecycle THEN 0
            ELSE feeds.failure_count
        END,
        retry_after = CASE
            WHEN input.clear_lifecycle THEN NULL
            ELSE feeds.retry_after
        END,
        status_reason = CASE
            WHEN input.clear_lifecycle THEN NULL
            ELSE feeds.status_reason
        END,
        status_reason_detail = CASE
            WHEN input.clear_lifecycle THEN NULL
            ELSE feeds.status_reason_detail
        END,
        status_reason_updated_at = CASE
            WHEN input.clear_lifecycle
             AND (
                 feeds.status_reason IS NOT NULL
                 OR feeds.status_reason_detail IS NOT NULL
             )
                THEN NOW()
            ELSE feeds.status_reason_updated_at
        END,
        audit_revision = feeds.audit_revision
            + CASE WHEN input.clear_lifecycle THEN 1 ELSE 0 END
    FROM input
    WHERE feeds.id = input.feed_id
      AND feeds.status IN (
          'active'::public.feed_status,
          'failing'::public.feed_status
      )
      AND (
          (
              input.write_cursor
              AND (
                  feeds.last_bookmark_time IS NULL
                  OR input.cursor > feeds.last_bookmark_time
              )
          )
          OR input.clear_lifecycle
      )
    RETURNING
        input.caller_ordinal,
        feeds.id,
        feeds.name,
        feeds.source_type,
        feeds.status::text AS status,
        feeds.last_processed_filename,
        feeds.last_bookmark_time,
        feeds.failure_count,
        feeds.retry_after,
        feeds.status_reason,
        feeds.status_reason_detail,
        feeds.status_reason_updated_at,
        feeds.audit_revision,
        feeds.created_at
)
SELECT *
FROM updated
ORDER BY caller_ordinal
"""


APPLY_FEED_FAILURES_SQL = """\
WITH input AS MATERIALIZED (
    SELECT
        input_values.feed_id,
        input_values.cursor,
        input_values.write_cursor,
        input_values.is_budgeted,
        input_values.failure_threshold,
        input_values.backoff_max_sec,
        input_values.backoff_base_sec,
        input_values.retry_after,
        input_values.status_reason,
        input_values.status_reason_detail,
        input_values.caller_ordinal,
        input_values.row_ordinal
    FROM UNNEST(
        $1::uuid[],
        $2::timestamptz[],
        $3::boolean[],
        $4::boolean[],
        $5::integer[],
        $6::integer[],
        $7::integer[],
        $8::timestamptz[],
        $9::text[],
        $10::text[],
        $11::bigint[]
    ) WITH ORDINALITY AS input_values(
        feed_id,
        cursor,
        write_cursor,
        is_budgeted,
        failure_threshold,
        backoff_max_sec,
        backoff_base_sec,
        retry_after,
        status_reason,
        status_reason_detail,
        caller_ordinal,
        row_ordinal
    )
),
updated AS (
    UPDATE public.feeds AS feeds
    SET last_bookmark_time = CASE
            WHEN input.write_cursor
                THEN GREATEST(feeds.last_bookmark_time, input.cursor)
            ELSE feeds.last_bookmark_time
        END,
        status = CASE
            WHEN input.is_budgeted
             AND feeds.failure_count + 1 >= input.failure_threshold
                THEN 'quarantined'::public.feed_status
            ELSE 'failing'::public.feed_status
        END,
        failure_count = CASE
            WHEN input.is_budgeted THEN feeds.failure_count + 1
            ELSE 0
        END,
        retry_after = CASE
            WHEN NOT input.is_budgeted THEN input.retry_after
            WHEN feeds.failure_count + 1 >= input.failure_threshold THEN NULL
            ELSE NOW()
                + LEAST(
                    input.backoff_max_sec * INTERVAL '1 second',
                    input.backoff_base_sec * INTERVAL '1 second'
                        * POWER(2, feeds.failure_count)
                )
                + (RANDOM() * INTERVAL '10 seconds')
        END,
        status_reason = input.status_reason,
        status_reason_detail = input.status_reason_detail,
        status_reason_updated_at = CASE
            WHEN feeds.status_reason IS DISTINCT FROM input.status_reason
                THEN NOW()
            ELSE feeds.status_reason_updated_at
        END,
        audit_revision = feeds.audit_revision + 1
    FROM input
    WHERE feeds.id = input.feed_id
      AND feeds.status IN (
          'active'::public.feed_status,
          'failing'::public.feed_status
      )
      AND (
          input.cursor IS NULL
          OR (
              input.write_cursor
              AND (
                  feeds.last_bookmark_time IS NULL
                  OR input.cursor > feeds.last_bookmark_time
              )
          )
      )
    RETURNING
        input.caller_ordinal,
        feeds.id,
        feeds.name,
        feeds.source_type,
        feeds.status::text AS status,
        feeds.last_processed_filename,
        feeds.last_bookmark_time,
        feeds.failure_count,
        feeds.retry_after,
        feeds.status_reason,
        feeds.status_reason_detail,
        feeds.status_reason_updated_at,
        feeds.audit_revision,
        feeds.created_at
)
SELECT *
FROM updated
ORDER BY caller_ordinal
"""


FINALIZE_LEASE_RECOVERY_SQL = """\
UPDATE public.ingestion_leases AS leases
SET failure_count = 0,
    retry_after = NULL,
    status_reason = NULL,
    status_reason_detail = NULL,
    status_reason_updated_at = NULL,
    audit_revision = leases.audit_revision + 1,
    updated_at = NOW()
WHERE source_type = $1
  AND lease_key = $2
  AND status = 'active'::public.feed_status
  AND worker_id = $3
  AND fencing_token = $4
  AND (
      failure_count <> 0
      OR retry_after IS NOT NULL
      OR status_reason IS NOT NULL
      OR status_reason_detail IS NOT NULL
      OR status_reason_updated_at IS NOT NULL
  )
RETURNING
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
    status_reason_updated_at,
    audit_revision,
    membership_revision,
    updated_at
"""


LOAD_CHILD_AUDIT_PROPERTIES_SQL = """\
SELECT
    fp.feed_id,
    fp.source_feed_id,
    COALESCE(fp.tags, '[]'::jsonb) AS tags
FROM public.feed_properties AS fp
WHERE fp.feed_id = ANY($1::uuid[])
ORDER BY fp.feed_id
"""


INSERT_CHILD_AUDIT_EVENTS_SQL = f"""\
WITH input AS MATERIALIZED (
    SELECT
        input_values.feed_id,
        input_values.action,
        input_values.actor_id,
        input_values.feed_revision,
        input_values.before_values,
        input_values.after_values,
        input_values.caller_ordinal,
        input_values.row_ordinal
    FROM UNNEST(
        $1::uuid[],
        $2::text[],
        $3::text[],
        $4::bigint[],
        $5::jsonb[],
        $6::jsonb[],
        $7::bigint[]
    ) WITH ORDINALITY AS input_values(
        feed_id,
        action,
        actor_id,
        feed_revision,
        before_values,
        after_values,
        caller_ordinal,
        row_ordinal
    )
),
inserted AS (
    INSERT INTO public.feed_audit_events (
        feed_id,
        action,
        actor_id,
        feed_revision,
        before_values,
        after_values
    )
    SELECT
        input.feed_id,
        input.action,
        input.actor_id,
        input.feed_revision,
        input.before_values,
        input.after_values
    FROM input
    ORDER BY input.caller_ordinal
    RETURNING
        id,
        feed_id,
        action,
        actor_id,
        occurred_at,
        feed_revision,
        before_values,
        after_values
)
SELECT
    input.caller_ordinal,
    {feed_audit_sql.feed_audit_event_payload_sql("inserted")}
        AS feed_audit_event
FROM input
JOIN inserted
  ON inserted.feed_id = input.feed_id
 AND inserted.feed_revision = input.feed_revision
ORDER BY input.caller_ordinal
"""
