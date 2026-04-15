"""Asyncio task that logs event-loop responsiveness every N seconds.

Experiment 1b only: measures how well the event loop schedules under
load.  A healthy loop returns from ``asyncio.sleep(0)`` in under 1ms
and experiences negligible drift between scheduled and actual
``asyncio.sleep(interval)`` durations.  Drift > 100ms at p95 signals
event-loop saturation — a ramp-step stop criterion.

Output is JSONL to stderr, one line per measurement.  Parse with jq:

    journalctl -u ingestion-service -o cat \\
        | jq -r 'select(.type == "event_loop_health")'

See model/data/wildfire_catalog/EXPERIMENT_1B_PLAN.md §0.2 Change 5.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import time

logger = logging.getLogger(__name__)


async def monitor_event_loop(interval_s: float = 10.0) -> None:
    """Background task — logs loop health every ``interval_s`` seconds.

    Runs until cancelled.  Intended to be started once alongside the
    other long-running ingestion tasks and cancelled during shutdown.
    """
    logger.info(
        "event_loop_monitor started (interval=%.1fs)",
        interval_s,
    )
    while True:
        # Synchronous snapshot: if the loop is healthy, sleep(0)
        # returns on the next tick (well under 1ms).  A large value
        # here means the loop is behind on its task queue.
        t0 = time.monotonic()
        await asyncio.sleep(0)
        loop_latency_ms = (time.monotonic() - t0) * 1000

        # Sleep-drift snapshot: schedule a wake at t+interval_s; measure
        # how late the wake actually fires.  Drift accumulates when the
        # loop is saturated and can't promptly resume its sleepers.
        t1 = time.monotonic()
        await asyncio.sleep(interval_s)
        actual = time.monotonic() - t1

        print(
            json.dumps({
                "type": "event_loop_health",
                "loop_latency_ms": round(loop_latency_ms, 2),
                "drift_ms": round((actual - interval_s) * 1000, 2),
                "interval_s": interval_s,
            }),
            file=sys.stderr,
            flush=True,
        )
