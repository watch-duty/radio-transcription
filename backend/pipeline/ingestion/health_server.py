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
    ``_feed_tasks`` dict so ``len()`` reflects live leasing without copying.

    ``last_nonzero_feeds_at`` is stamped by the leasing loop whenever the
    feed_tasks dict is observed non-empty. Used by gate 2 to decide whether
    zero-feed state has been *sustained* rather than transient.
    """

    startup_time: float = field(default_factory=time.monotonic)
    last_heartbeat_completed: float | None = None
    last_nonzero_feeds_at: float | None = None
    feed_tasks: dict[uuid.UUID, object] = field(default_factory=dict)


# Typed aiohttp app keys (the recommended pattern since aiohttp 3.9).
_STATE_KEY: web.AppKey[HealthState] = web.AppKey("state", HealthState)
_SETTINGS_KEY: web.AppKey[NormalizerSettings] = web.AppKey("settings")


async def _healthz(request: web.Request) -> web.Response:
    state = request.app[_STATE_KEY]
    settings = request.app[_SETTINGS_KEY]
    now = time.monotonic()
    uptime = now - state.startup_time
    hb = state.last_heartbeat_completed

    # Gate 0: Startup grace. Return 200 for the first
    # health_check_startup_grace_sec regardless of state. The MIG autohealer's
    # initial_delay_sec covers the same window externally — VMs are not killed
    # during grace even if they return 503 — so this gate exists to match the
    # spec's contract: an operator checking /healthz during startup sees the
    # worker booting, not a misleading 503.
    if uptime < settings.health_check_startup_grace_sec:
        return web.json_response(
            {
                "status": "healthy",
                "active_feeds": len(state.feed_tasks),
                "last_heartbeat_age_sec": (now - hb) if hb is not None else None,
            },
        )

    # Gate 1: Heartbeat freshness. Threshold is 2x the heartbeat interval per
    # spec: normal-case heartbeat cycles complete in <1s, and the stall
    # detector (heartbeat_stall_timeout_sec) triggers os._exit(1) if a cycle
    # hangs past its hard ceiling. Failing here means the event loop is
    # degraded (running but starving the heartbeat coroutine) in a way the
    # stall detector didn't catch.
    heartbeat_max_age_sec = 2.0 * settings.heartbeat_interval_sec
    if hb is None:
        return web.json_response(
            {"status": "unhealthy", "reason": "no_heartbeat"},
            status=503,
        )
    hb_age = now - hb
    if hb_age > heartbeat_max_age_sec:
        return web.json_response(
            {"status": "unhealthy", "reason": "heartbeat_stale"},
            status=503,
        )

    # Gate 2: Zero leased feeds for longer than the configured window,
    # measured post-grace. Reference point is max(grace_end, last_nonzero):
    # - grace_end ensures we don't fire the moment grace expires if feeds
    #   never arrived (must wait zero_feeds_max_sec AFTER grace);
    # - last_nonzero ensures that if feeds were briefly acquired then lost,
    #   the zero-duration clock resets from the loss.
    feed_count = len(state.feed_tasks)
    if feed_count == 0:
        grace_end = state.startup_time + settings.health_check_startup_grace_sec
        last_nonzero = state.last_nonzero_feeds_at
        reference = (
            max(grace_end, last_nonzero) if last_nonzero is not None else grace_end
        )
        if (now - reference) > settings.health_check_zero_feeds_max_sec:
            return web.json_response(
                {"status": "unhealthy", "reason": "zero_active_feeds"},
                status=503,
            )

    return web.json_response(
        {
            "status": "healthy",
            "active_feeds": feed_count,
            "last_heartbeat_age_sec": hb_age,
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
