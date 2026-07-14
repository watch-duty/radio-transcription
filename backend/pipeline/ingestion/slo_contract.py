"""Shared SLI/SLO contract constants for the ingestion pipeline.

Single source of truth for every string literal that the ops-team Terraform
alert-filter side matches against. Any rename here MUST be coordinated with
the ops-team Terraform repo (separate from this repo) — a silent rename will
break alert filters without any in-repo test failure.

DO NOT inline these literals at emit sites. Import them.

Constants:
    EVENT_TYPE_CHUNK_INGESTED:      structured-log event_type for LOG-01
    EVENT_TYPE_CALL_DOWNLOAD_FAILED: structured-log event_type for LOG-02
    EVENT_TYPE_FEED_QUARANTINED:    structured-log event_type for existing
                                    quarantine_telemetry (pinned to match shipped value)
    EVENT_TYPE_CALL_AUTH_FAILURE:   structured-log event_type emitted when the
                                    bcfy_calls collector gets a 401/403 from
                                    Broadcastify and has to refresh its JWT.
                                    No Terraform metric references this yet; the
                                    constant exists so future alerts/metrics can
                                    key on a stable literal.
    EVENT_TYPE_BCFY_JWT_FETCH_FAILED: structured-log event_type emitted when the
                                    bcfy_calls collector cannot fetch the shared
                                    Broadcastify JWT from Secret Manager.
    EVENT_TYPE_BCFY_CALLS_MISSING_CALL: one irreversible per-call gap event.
    EVENT_TYPE_BCFY_CALLS_SID_POLL: one settled logical SID-poll event.
    EVENT_TYPE_BCFY_CALLS_REPLAY_WINDOW_TRUNCATED: one known replay-window
                                    truncation interval at CRITICAL severity.
    BCFY_CALLS_MISSING_CALL_SCHEMA_VERSION: stable event schema version.
    METRIC_TYPE_QUARANTINE_EVENTS:  metric type URL for existing quarantine metric
                                    (pinned to match shipped value — migrated from
                                    quarantine_telemetry.py's private _METRIC_TYPE)
    INGESTION_LOGGER_PATH:          logger-path prefix the Terraform alert filters on
"""

from __future__ import annotations

__all__ = [
    "BCFY_CALLS_MISSING_CALL_ATTEMPT_COUNT_MAX",
    "BCFY_CALLS_MISSING_CALL_AUDIO_URL_MAX_LENGTH",
    "BCFY_CALLS_MISSING_CALL_IDENTITY_MAX_LENGTH",
    "BCFY_CALLS_MISSING_CALL_REASON_MAX_LENGTH",
    "BCFY_CALLS_MISSING_CALL_SCHEMA_VERSION",
    "BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_REQUIRED_FIELDS",
    "BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_SCHEMA_VERSION",
    "BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_SECONDS_MAX",
    "BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_STRING_MAX_LENGTH",
    "BCFY_CALLS_SID_POLL_COUNT_MAX",
    "BCFY_CALLS_SID_POLL_OPTIONAL_FIELDS",
    "BCFY_CALLS_SID_POLL_REQUIRED_FIELDS",
    "BCFY_CALLS_SID_POLL_SCHEMA_VERSION",
    "BCFY_CALLS_SID_POLL_SECONDS_MAX",
    "BCFY_CALLS_SID_POLL_STRING_MAX_LENGTH",
    "EVENT_TYPE_BCFY_CALLS_MISSING_CALL",
    "EVENT_TYPE_BCFY_CALLS_REPLAY_WINDOW_TRUNCATED",
    "EVENT_TYPE_BCFY_CALLS_SID_POLL",
    "EVENT_TYPE_BCFY_JWT_FETCH_FAILED",
    "EVENT_TYPE_CALL_AUTH_FAILURE",
    "EVENT_TYPE_CALL_DOWNLOAD_FAILED",
    "EVENT_TYPE_CHUNK_INGESTED",
    "EVENT_TYPE_FEED_QUARANTINED",
    "INGESTION_LOGGER_PATH",
    "METRIC_TYPE_QUARANTINE_EVENTS",
]

# ---------------------------------------------------------------------------
# Structured-log event_type values. Must match Terraform log-based metric
# filter strings exactly. Ops-team alert filters are of the form
# `jsonPayload.event_type = "chunk_ingested"`.
# ---------------------------------------------------------------------------
EVENT_TYPE_CHUNK_INGESTED: str = "chunk_ingested"
EVENT_TYPE_CALL_DOWNLOAD_FAILED: str = "call_download_failed"
EVENT_TYPE_FEED_QUARANTINED: str = "feed_quarantined"
EVENT_TYPE_CALL_AUTH_FAILURE: str = "call_auth_failure"
EVENT_TYPE_BCFY_JWT_FETCH_FAILED: str = "bcfy_jwt_fetch_failed"
EVENT_TYPE_BCFY_CALLS_MISSING_CALL: str = "bcfy_calls_missing_call"
EVENT_TYPE_BCFY_CALLS_SID_POLL: str = "bcfy_calls_sid_poll"
EVENT_TYPE_BCFY_CALLS_REPLAY_WINDOW_TRUNCATED: str = (
    "bcfy_calls_replay_window_truncated"
)

# The missing-call event is intentionally bounded because one live page owns
# all of its pending event data in memory. The exact signed URL is retained;
# oversized provider input fails closed rather than being truncated.
BCFY_CALLS_MISSING_CALL_SCHEMA_VERSION: int = 1
BCFY_CALLS_MISSING_CALL_AUDIO_URL_MAX_LENGTH: int = 8192
BCFY_CALLS_MISSING_CALL_IDENTITY_MAX_LENGTH: int = 512
BCFY_CALLS_MISSING_CALL_REASON_MAX_LENGTH: int = 2048
BCFY_CALLS_MISSING_CALL_ATTEMPT_COUNT_MAX: int = 1000

# The v1 SID-poll contract is one bounded scalar object. Optional additions
# may extend schema v1, but required fields cannot be removed or redefined.
BCFY_CALLS_SID_POLL_SCHEMA_VERSION: int = 1
BCFY_CALLS_SID_POLL_STRING_MAX_LENGTH: int = 512
BCFY_CALLS_SID_POLL_COUNT_MAX: int = 2**63 - 1
BCFY_CALLS_SID_POLL_SECONDS_MAX: float = 10**12
BCFY_CALLS_SID_POLL_REQUIRED_FIELDS: tuple[str, ...] = (
    "admitted_cohort_count",
    "admitted_record_count",
    "deduplicated_call_count",
    "duration_seconds",
    "early_flush_attempt_count",
    "event_type",
    "failed_feed_count",
    "fence_rejection_count",
    "fencing_token",
    "final_flush_attempt_count",
    "http_attempt_count",
    "item_to_feed_promotion_count",
    "lease_cursor_lag_seconds",
    "lifecycle_reason",
    "lifecycle_scope",
    "matched_call_count",
    "maximum_feed_cursor_lag_seconds",
    "maximum_flush_latency_seconds",
    "maximum_held_count",
    "maximum_queue_depth",
    "maximum_queue_wait_seconds",
    "maximum_worker_utilization_numerator",
    "membership_rejection_count",
    "membership_revision",
    "null_feed_cursor_count",
    "oldest_queue_age_seconds",
    "outcome",
    "owner_worker_id",
    "participating_feed_count",
    "pressure_encountered",
    "pressure_wait_count",
    "pressure_wait_seconds",
    "provider_observed",
    "replay_blocked_record_count",
    "request_pos",
    "response_byte_count",
    "response_last_pos",
    "response_last_pos_state",
    "response_row_count",
    "schema_version",
    "sid",
    "source_type",
    "successful_feed_count",
    "terminal_record_count",
    "total_flush_latency_seconds",
    "total_queue_wait_seconds",
    "worker_utilization_denominator",
)
BCFY_CALLS_SID_POLL_OPTIONAL_FIELDS: tuple[str, ...] = ()

# Replay-window evidence is intentionally separate from both the ordinary
# settled-poll event and the per-call missing-audio event. It contains only
# complete grant identity and the exact time interval known to be unavailable.
BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_SCHEMA_VERSION: int = 1
BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_STRING_MAX_LENGTH: int = 512
BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_SECONDS_MAX: float = 10**12
BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_REQUIRED_FIELDS: tuple[str, ...] = (
    "cause",
    "event_type",
    "fencing_token",
    "floor_start",
    "lost_duration_seconds",
    "lost_span_end",
    "lost_span_start",
    "owner_worker_id",
    "requested_start",
    "schema_version",
    "sid",
    "source_type",
)

# ---------------------------------------------------------------------------
# Cloud Monitoring metric type for the existing quarantine_events metric.
# URL path components are pinned to the values Terraform's alertPolicy
# filters are written against. Renaming will silently break alerts.
# ---------------------------------------------------------------------------
METRIC_TYPE_QUARANTINE_EVENTS: str = (
    "custom.googleapis.com/feeds/quarantine_events"
)

# ---------------------------------------------------------------------------
# Logger path prefix. All ingestion-side structured logs must be emitted on
# a logger whose name starts with this string; Terraform alert filters key
# on `logName =~ ".*backend.pipeline.ingestion.*"`.
# ---------------------------------------------------------------------------
INGESTION_LOGGER_PATH: str = "backend.pipeline.ingestion"
