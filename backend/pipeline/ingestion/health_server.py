from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    import uuid

    from backend.pipeline.ingestion.settings import NormalizerSettings

logger = logging.getLogger(__name__)


@dataclass
class HealthState:
    """
    Shared state between the worker runtime and the /healthz handler.

    Lives on the event-loop thread; all reads/writes happen there, so no lock
    is required. ``feed_tasks`` is held by reference to the runtime's own
    ``_feed_tasks`` dict for diagnostic reporting only (``len()`` in the OK
    response body) — it is NOT a gate, because an idle worker with no feeds
    available is a valid state under low load.
    """

    startup_time: float = field(default_factory=time.monotonic)
    last_heartbeat_completed: float | None = None
    last_lease_attempt_completed: float | None = None
    feed_tasks: dict[uuid.UUID, object] = field(default_factory=dict)


# Typed aiohttp app keys (the recommended pattern since aiohttp 3.9).
_STATE_KEY: web.AppKey[HealthState] = web.AppKey("state", HealthState)
_SETTINGS_KEY: web.AppKey[NormalizerSettings] = web.AppKey("settings")


async def _healthz(request: web.Request) -> web.Response:
    state = request.app[_STATE_KEY]
    settings = request.app[_SETTINGS_KEY]
    now = time.monotonic()
    uptime = now - state.startup_time

    # Gate 1 (event-loop responsiveness) is implicit: if aiohttp dispatched
    # this handler, the loop is at least minimally alive.

    # Gate 2: heartbeat completed at least once, and freshly.
    # No startup grace: returning 200 before the worker has proven it can
    # heartbeat would let `wait_for_instances_status = HEALTHY` pass Terraform
    # the instant the port binds, masking a broken config (bad DB creds,
    # network partition). The MIG's `initial_delay_sec = 360` handles the
    # startup window on the autohealer side: 503s during the first 6 min of
    # VM life don't trigger recreation. Normal-case first heartbeat completes
    # ~15s after Python process start (heartbeat thread interval), so HEALTHY
    # transition happens well inside the 360s window.
    hb = state.last_heartbeat_completed
    if hb is None:
        return web.json_response(
            {
                "status": "unhealthy",
                "reason": "no_heartbeat_yet",
                "uptime_s": uptime,
            },
            status=503,
        )
    hb_age = now - hb
    if hb_age > settings.health_check_heartbeat_max_age_sec:
        return web.json_response(
            {
                "status": "unhealthy",
                "reason": "heartbeat_stale",
                "heartbeat_age_s": hb_age,
                "uptime_s": uptime,
            },
            status=503,
        )

    # Gate 3: the leasing loop is alive. Stamped at the top of every
    # _leasing_loop iteration regardless of whether feeds were actually
    # acquired — so "0 feeds leased because the pipeline is under-loaded"
    # is healthy, while "0 feeds leased because the loop is wedged" returns
    # 503. Catches the failure mode heartbeat gate can't: heartbeat cycle
    # and lease loop are independent, so a stuck lease loop won't age the
    # heartbeat stamp.
    la = state.last_lease_attempt_completed
    if la is None:
        return web.json_response(
            {
                "status": "unhealthy",
                "reason": "no_lease_attempt_yet",
                "uptime_s": uptime,
            },
            status=503,
        )
    la_age = now - la
    if la_age > settings.health_check_lease_attempt_max_age_sec:
        return web.json_response(
            {
                "status": "unhealthy",
                "reason": "lease_loop_stalled",
                "lease_attempt_age_s": la_age,
                "uptime_s": uptime,
            },
            status=503,
        )

    return web.json_response(
        {
            "status": "ok",
            "uptime_s": uptime,
            "heartbeat_age_s": hb_age,
            "lease_attempt_age_s": la_age,
            "active_feeds": len(state.feed_tasks),
        },
    )


def build_app(settings: NormalizerSettings, state: HealthState) -> web.Application:
    """Build an aiohttp Application that serves GET /healthz."""
    app = web.Application()
    app[_STATE_KEY] = state
    app[_SETTINGS_KEY] = settings
    app.router.add_get("/healthz", _healthz)
    return app


async def start(
    settings: NormalizerSettings,
    state: HealthState,
) -> web.AppRunner:
    """
    Start the /healthz HTTP server on the current event loop.

    Returns the AppRunner so the caller can ``await runner.cleanup()`` during
    shutdown to release the port.
    """
    app = build_app(settings, state)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=settings.health_check_port)  # noqa: S104
    await site.start()
    logger.info(
        "Health check server listening on 0.0.0.0:%d/healthz",
        settings.health_check_port,
    )
    return runner
