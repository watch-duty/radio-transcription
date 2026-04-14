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
    ``_feed_tasks`` dict, so ``len()`` in the handler reflects live leasing.
    """

    startup_time: float = field(default_factory=time.monotonic)
    last_heartbeat_completed: float | None = None
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

    # Gate 2: heartbeat freshness.
    hb = state.last_heartbeat_completed
    if hb is None:
        if uptime < settings.health_check_heartbeat_grace_sec:
            return web.json_response(
                {"status": "ok", "reason": "starting", "uptime_s": uptime},
            )
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

    # Gate 3: the worker has leased at least one feed, after the startup grace.
    feed_count = len(state.feed_tasks)
    if uptime > settings.health_check_feed_grace_sec and feed_count == 0:
        return web.json_response(
            {
                "status": "unhealthy",
                "reason": "no_active_feeds",
                "uptime_s": uptime,
                "active_feeds": 0,
            },
            status=503,
        )

    return web.json_response(
        {
            "status": "ok",
            "uptime_s": uptime,
            "heartbeat_age_s": hb_age,
            "active_feeds": feed_count,
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
