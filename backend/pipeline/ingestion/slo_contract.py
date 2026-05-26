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
    EVENT_TYPE_BATCH_UNPRODUCTIVE:  structured-log event_type emitted for
                                    evidence-only unproductive collector batches.
    METRIC_TYPE_QUARANTINE_EVENTS:  metric type URL for existing quarantine metric
                                    (pinned to match shipped value — migrated from
                                    quarantine_telemetry.py's private _METRIC_TYPE)
    INGESTION_LOGGER_PATH:          logger-path prefix the Terraform alert filters on
"""

from __future__ import annotations

__all__ = [
    "EVENT_TYPE_BATCH_UNPRODUCTIVE",
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
EVENT_TYPE_BATCH_UNPRODUCTIVE: str = "batch_unproductive"

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
