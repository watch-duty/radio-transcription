"""Shared SLI/SLO contract constants for the ingestion pipeline.

Single source of truth for every string literal that the ops-team Terraform
alert-filter side matches against. Any rename here MUST be coordinated with
the ops-team Terraform repo (separate from this repo) — a silent rename will
break alert filters without any in-repo test failure.

DO NOT inline these literals at emit sites. Import them.

Constants:
    EVENT_TYPE_CHUNK_INGESTED:      structured-log event_type for Phase 2 LOG-01
    EVENT_TYPE_CALL_DOWNLOAD_FAILED: structured-log event_type for Phase 2 LOG-02
    EVENT_TYPE_FEED_QUARANTINED:    structured-log event_type for existing
                                    quarantine_telemetry (pinned to match shipped value)
    METRIC_TYPE_ACTIVE_FEED_COUNT:  metric type URL for Phase 3 METRIC-01
    METRIC_TYPE_QUARANTINE_EVENTS:  metric type URL for existing quarantine metric
                                    (pinned to match shipped value — migrated from
                                    quarantine_telemetry.py's private _METRIC_TYPE)
    MONITORED_RESOURCE_TYPE:        `gce_instance` for Phase 3 METRIC-01
    METRIC_LABEL_ALLOWLIST:         frozenset runtime cardinality gate for Phase 3
    INGESTION_LOGGER_PATH:          logger-path prefix the Terraform alert filters on
"""

from __future__ import annotations

__all__ = [
    "EVENT_TYPE_CALL_DOWNLOAD_FAILED",
    "EVENT_TYPE_CHUNK_INGESTED",
    "EVENT_TYPE_FEED_QUARANTINED",
    "INGESTION_LOGGER_PATH",
    "METRIC_LABEL_ALLOWLIST",
    "METRIC_TYPE_ACTIVE_FEED_COUNT",
    "METRIC_TYPE_QUARANTINE_EVENTS",
    "MONITORED_RESOURCE_TYPE",
]

# ---------------------------------------------------------------------------
# Structured-log event_type values. Must match Terraform log-based metric
# filter strings exactly. Ops-team alert filters are of the form
# `jsonPayload.event_type = "chunk_ingested"`.
# ---------------------------------------------------------------------------
EVENT_TYPE_CHUNK_INGESTED: str = "chunk_ingested"
EVENT_TYPE_CALL_DOWNLOAD_FAILED: str = "call_download_failed"
EVENT_TYPE_FEED_QUARANTINED: str = "feed_quarantined"

# ---------------------------------------------------------------------------
# Cloud Monitoring metric types. URL path components are pinned to the values
# Terraform's alertPolicy filters are written against. Renaming either will
# silently break alerts.
# ---------------------------------------------------------------------------
METRIC_TYPE_ACTIVE_FEED_COUNT: str = (
    "custom.googleapis.com/ingestion/active_feed_count"
)
METRIC_TYPE_QUARANTINE_EVENTS: str = (
    "custom.googleapis.com/feeds/quarantine_events"
)

# ---------------------------------------------------------------------------
# Cloud Monitoring resource config for the active_feed_count metric.
# `gce_instance` is the only resource type supported by the MIG deployment;
# the allowlist is the runtime cardinality gate — any label key outside this
# set must be rejected by the reporter in Phase 3 (Pitfall 1 prevention).
# ---------------------------------------------------------------------------
MONITORED_RESOURCE_TYPE: str = "gce_instance"
METRIC_LABEL_ALLOWLIST: frozenset[str] = frozenset({"instance_id", "zone"})

# ---------------------------------------------------------------------------
# Logger path prefix. All ingestion-side structured logs must be emitted on
# a logger whose name starts with this string; Terraform alert filters key
# on `logName =~ ".*backend.pipeline.ingestion.*"`.
# ---------------------------------------------------------------------------
INGESTION_LOGGER_PATH: str = "backend.pipeline.ingestion"
