"""Oldest-feed publisher Cloud Run function.

Triggered every 60 s by Cloud Scheduler. Queries AlloyDB for the oldest
unclaimed feed's age in seconds, publishes it as a DOUBLE GAUGE point to
custom.googleapis.com/feeds/oldest_unclaimed_age_seconds, returns 200.

On failure (DB error, monitoring write error, missing env), logs ERROR
and returns 500 — does NOT publish a sentinel value (GCP autoscalers
reject negative custom metric values; metric staleness is the failure
signal, handled by alerts in a follow-up PR per Phase 2 CONTEXT D-06).

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

METRIC_TYPE = "custom.googleapis.com/feeds/oldest_unclaimed_age_seconds"

# PUB-07: COALESCE returns 0.0 when no unclaimed rows exist (autoscaler
# then sees "fully caught up" — correct behavior; not a sentinel).
QUERY = (
    "SELECT COALESCE("
    "EXTRACT(epoch FROM NOW() - MIN(unclaimed_since)), 0.0"
    ") FROM feeds WHERE status = 'unclaimed'"
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


async def _query_oldest_age() -> float:
    """Connect to AlloyDB via PgBouncer, run PUB-07 query, return age seconds.

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
    # asyncpg returns Decimal/None depending on PG type; COALESCE makes
    # None impossible, but cast defensively for the DOUBLE write contract.
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
    value = await _query_oldest_age()
    await _publish(value)
    logger.info("published oldest_unclaimed_age_seconds=%.3f", value)


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
