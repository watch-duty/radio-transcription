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
    "EVENT_TYPE_BCFY_CALLS_MISSING_CALL",
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

# The missing-call event is intentionally bounded because one live page owns
# all of its pending event data in memory. The exact signed URL is retained;
# oversized provider input fails closed rather than being truncated.
BCFY_CALLS_MISSING_CALL_SCHEMA_VERSION: int = 1
BCFY_CALLS_MISSING_CALL_AUDIO_URL_MAX_LENGTH: int = 8192
BCFY_CALLS_MISSING_CALL_IDENTITY_MAX_LENGTH: int = 512
BCFY_CALLS_MISSING_CALL_REASON_MAX_LENGTH: int = 2048
BCFY_CALLS_MISSING_CALL_ATTEMPT_COUNT_MAX: int = 1000

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
