"""Feed-queue publisher Cloud Run function.

Triggered every 60 s by Cloud Scheduler. Queries AlloyDB for the count of
feeds in `unclaimed` status, publishes it as a DOUBLE GAUGE point to
custom.googleapis.com/feeds/unclaimed_count, returns 200.

The MIG autoscaler consumes this metric as its primary scaling signal
(per-group additive metric with single_instance_assignment math —
each VM responsible for absorbing N unclaimed feeds; autoscaler scales
to keep total queue <= N * VMs). Queue length is a LEADING indicator -
grows the moment claim rate falls behind arrival rate, well before any
individual feed has been waiting long enough to breach the SLO.

NOTE on naming: the directory `oldest_feed_publisher/` and Cloud Run
service name `oldest-feed-publisher-${env}` predate this metric pivot
(originally published `oldest_unclaimed_age_seconds` latency metric).
Renaming would force destroy+recreate of the Cloud Run service; not
worth the operational churn. A follow-up PR will likely re-add the
latency metric (publishing both from this same service for SLO alerting),
restoring the directory name's accuracy.

On failure (DB error, monitoring write error, missing env), logs ERROR
and returns 500 — does NOT publish a sentinel value. GCP autoscalers
reject negative custom metric values; metric staleness is the failure
signal, handled by alerts (deferred to a follow-up PR).

Per Phase 2 CONTEXT D-02: per-invocation asyncpg.connect/close — no
module-level pool. ~50 ms overhead per call is invisible at 1 call/min;
PgBouncer in transaction mode handles the connection churn cheaply.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

import asyncpg
import functions_framework

from backend.pipeline.common.clients.monitoring_client import MonitoringClient
from backend.pipeline.common.logging import setup_logging

if TYPE_CHECKING:
    import flask

# ---------------------------------------------------------------------------
# Configuration (read at module import; validated per-invocation)
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
ALLOYDB_HOST = os.environ.get("ALLOYDB_HOST", "")
# `or` fallback (vs `os.environ.get(name, default)`) defends against
# explicitly-empty env injection — if ALLOYDB_PORT is set to "" the dict.get
# default doesn't kick in, and `int("")` would raise downstream at connect.
ALLOYDB_PORT = os.environ.get("ALLOYDB_PORT") or "6432"
ALLOYDB_USER = os.environ.get("ALLOYDB_USER") or "worker"
ALLOYDB_DB = os.environ.get("ALLOYDB_DB", "")
ALLOYDB_PASSWORD = os.environ.get("ALLOYDB_PASSWORD", "")

METRIC_TYPE = "custom.googleapis.com/feeds/unclaimed_count"

# COUNT(*) is naturally non-negative and never NULL — no COALESCE needed.
# Cast to DOUBLE PRECISION because MonitoringClient.write_time_series_double()
# expects a Python float (mirrors the existing quarantine_events pattern;
# DOUBLE GAUGE is type-consistent with the autoscaler's metric block).
QUERY = (
    "SELECT COUNT(*)::DOUBLE PRECISION FROM feeds WHERE status = 'unclaimed'"
)

# Timeouts: per Phase 2 CONTEXT "Claude's Discretion" — recommended baseline.
CONNECT_TIMEOUT_SEC = 10.0
QUERY_TIMEOUT_SEC = 5.0

# ---------------------------------------------------------------------------
setup_logging()
logger = logging.getLogger(__name__)

# Shared MonitoringClient across warm invocations. MonitoringClient lazily
# initializes its underlying MetricServiceAsyncClient on first use, so
# constructing the wrapper at module level is cheap.
_monitoring_client: MonitoringClient | None = None


def _get_monitoring_client() -> MonitoringClient:
    global _monitoring_client  # noqa: PLW0603
    if _monitoring_client is None:
        _monitoring_client = MonitoringClient(project_id=PROJECT_ID)
    return _monitoring_client


def _require_environment() -> None:
    """Validate required env vars at invocation; raise RuntimeError if any missing."""
    required = {
        "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
        "ALLOYDB_HOST": ALLOYDB_HOST,
        "ALLOYDB_DB": ALLOYDB_DB,
        "ALLOYDB_PASSWORD": ALLOYDB_PASSWORD,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        msg = f"Missing required environment variables: {', '.join(missing)}"
        raise RuntimeError(msg)


async def _query_unclaimed_count() -> float:
    """Connect to AlloyDB via PgBouncer, run COUNT query, return queue depth.

    Per D-02: per-invocation connect/close, no module-level pool. Per
    connection.py: statement_cache_size=0 for PgBouncer transaction-mode
    compatibility (per-connection SET GUCs would be RESET between
    transactions in transaction-mode pooling — see PITFALLS.md Pitfall 10).
    """
    conn = await asyncpg.connect(
        host=ALLOYDB_HOST,
        port=int(ALLOYDB_PORT),
        user=ALLOYDB_USER,
        password=ALLOYDB_PASSWORD,
        database=ALLOYDB_DB,
        statement_cache_size=0,
        timeout=CONNECT_TIMEOUT_SEC,
    )
    try:
        value = await conn.fetchval(QUERY, timeout=QUERY_TIMEOUT_SEC)
    finally:
        await conn.close()
    # COUNT(*)::DOUBLE PRECISION returns float; defensive cast handles
    # asyncpg's PG-type-to-Python conversion edge cases.
    return float(value) if value is not None else 0.0


async def _publish(value: float) -> None:
    """Write a single DOUBLE GAUGE datapoint via the existing MonitoringClient."""
    client = _get_monitoring_client()
    await client.write_time_series_double(
        metric_type=METRIC_TYPE,
        labels={},
        value=value,
        resource_type="global",
        resource_labels={"project_id": PROJECT_ID},
    )


async def _run() -> None:
    value = await _query_unclaimed_count()
    await _publish(value)
    logger.info("published unclaimed_count=%.0f", value)


@functions_framework.http
def oldest_feed_publisher(request: flask.Request) -> tuple[str, int]:
    """HTTP entrypoint invoked by Cloud Scheduler every 60 s."""
    del request  # unused for scheduler-triggered requests
    try:
        _require_environment()
        asyncio.run(_run())
    except Exception:
        logger.exception("publisher failed; skipping write")
        return ("err", 500)
    return ("ok", 200)
