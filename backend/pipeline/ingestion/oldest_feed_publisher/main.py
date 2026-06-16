"""Feed-queue publisher Cloud Run function.

Triggered every 60 s by Cloud Scheduler. Queries AlloyDB for the count of
feeds in `unclaimed` status, publishes it as an INT64 GAUGE point to
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

Connection model: a 1-connection asyncpg pool reused across warm
invocations (Cloud Run reuses the container instance until it scales
in or restarts). The pool is bound to the module-level event loop
created at import time — `asyncio.run` would create a fresh loop per
request and orphan the pool (asyncpg pools are loop-bound and become
unusable when their loop closes). max_inactive_connection_lifetime
defends against the Cloud-Run-freezes-the-container-between-requests
case where a stale connection would otherwise be retrieved at the
next tick.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

import functions_framework

from backend.pipeline.common.clients.monitoring_client import MonitoringClient
from backend.pipeline.common.log_helper import (
    setup_asyncio_logging,
    setup_logging,
)
from backend.pipeline.storage.connection import create_pool_from_settings
from backend.pipeline.storage.settings import AlloyDBSettings

if TYPE_CHECKING:
    import asyncpg
    import flask

# ---------------------------------------------------------------------------
# Configuration (read at module import; validated at first pool creation)
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "")

METRIC_TYPE = "custom.googleapis.com/feeds/unclaimed_count"

# COUNT(*) returns BIGINT (Python int via asyncpg); naturally non-negative
# and never NULL — no COALESCE needed. INT64 matches the descriptor's
# value_type AND mirrors the existing quarantine_events pattern (the only
# other custom.googleapis.com/feeds/* metric in this codebase).
QUERY = "SELECT COUNT(*) FROM feeds WHERE status = 'unclaimed'"

# Per-query timeout. Overrides AlloyDBSettings.command_timeout_sec (default
# 30s) — the unclaimed-count query is a single sequential scan over a small
# in-memory index; 5s is generous and keeps the function well under
# Cloud Scheduler's 30s attempt_deadline.
QUERY_TIMEOUT_SEC = 5.0

# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# Persistent event loop, created once at module import. The asyncpg pool
# created inside this loop stays bound to it for the lifetime of the
# Cloud Run instance. Using `asyncio.run` instead would create a fresh
# loop per request and orphan the pool — pool.fetchval on the second
# invocation would raise "Task got Future attached to a different loop"
# or hang on the loop-bound semaphore.
#
# Concurrency assumption: this `_loop.run_until_complete(...)` pattern is
# NOT thread-safe — calling it from two threads simultaneously raises
# "This event loop is already running". Safe here because the deployment-
# repo Terraform sets `max_instance_request_concurrency = 1` on the Cloud
# Run service (terraform/modules/services/ingestion/oldest_feed_publisher/
# main.tf), so GCP routes at most one request at a time to a given
# instance. If anyone relaxes that to N>1, this code must add a
# threading.Lock around the run_until_complete call.
_loop = asyncio.new_event_loop()
setup_asyncio_logging(_loop)

# Reused across warm invocations.
_monitoring_client: MonitoringClient | None = None
_pool: asyncpg.Pool | None = None


def _get_monitoring_client() -> MonitoringClient:
    global _monitoring_client  # noqa: PLW0603
    if _monitoring_client is None:
        _monitoring_client = MonitoringClient(project_id=PROJECT_ID)
    return _monitoring_client


async def _get_pool() -> asyncpg.Pool:
    """Lazy 1-connection pool reused across warm Cloud Run invocations."""
    global _pool  # noqa: PLW0603
    if _pool is None:
        # AlloyDBSettings reads ALLOYDB_* env vars with sensible defaults; the
        # required vars are validated at asyncpg connection time (host
        # reachability check). PROJECT_ID is validated at module import (above).
        # Override pool sizing for a 1-tick-per-minute service (defaults are
        # 8/8, sized for the worker fleet; we need exactly 1 connection here).
        # max_inactive_connection_lifetime defends against Cloud Run freezing
        # the container between scheduler ticks — a connection idle for >30s
        # gets recycled rather than handing the next tick a half-dead socket.
        settings = AlloyDBSettings().replace(
            pool_min_size=1,
            pool_max_size=1,
            command_timeout_sec=QUERY_TIMEOUT_SEC,
        )
        _pool = await create_pool_from_settings(settings)
    return _pool


async def _publish(value: int) -> None:
    """Write a single INT64 GAUGE datapoint via the existing MonitoringClient."""
    client = _get_monitoring_client()
    await client.write_time_series(
        metric_type=METRIC_TYPE,
        labels={},
        value=value,
        resource_type="global",
        resource_labels={"project_id": PROJECT_ID},
    )


async def _run() -> None:
    pool = await _get_pool()
    raw = await pool.fetchval(QUERY, timeout=QUERY_TIMEOUT_SEC)
    # COUNT(*) returns BIGINT → Python int via asyncpg; defensive cast
    # handles the (impossible-by-COUNT-semantics) None case.
    value = int(raw) if raw is not None else 0
    await _publish(value)
    logger.info("published unclaimed_count=%d", value)


@functions_framework.http
def oldest_feed_publisher(request: flask.Request) -> tuple[str, int]:
    """HTTP entrypoint invoked by Cloud Scheduler every 60 s."""
    setup_logging()
    del request  # unused for scheduler-triggered requests
    if not PROJECT_ID:
        logger.error("missing required env var: GOOGLE_CLOUD_PROJECT")
        return ("err", 500)
    try:
        _loop.run_until_complete(_run())
    except Exception:
        logger.exception("publisher failed; skipping write")
        return ("err", 500)
    return ("ok", 200)
