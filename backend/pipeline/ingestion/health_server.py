from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from aiohttp import web

from backend.pipeline.ingestion import (
    grant_control,
    grant_supervisor,
    worker_profiles,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from backend.pipeline.ingestion.settings import CollectorSettings

logger = logging.getLogger(__name__)


@dataclass
class HealthState:
    """
    Shared state between the worker runtime and the /healthz handler.

    Lives on the event-loop thread; all reads/writes happen there, so no lock
    is required. The snapshot provider reads only the supervisor's immutable
    process-local projection and performs no request-time control or DB I/O.

    ``last_heartbeat_tick`` is stamped at the *beginning* of every
    ``_heartbeat_cycle`` invocation — before any DB I/O — so the stamp
    represents "the event loop dispatched the cycle" rather than "DB was
    reachable." Gate 1 of /healthz reads this; if the stamp were anchored to
    DB success, a transient AlloyDB outage would age every worker's stamp
    simultaneously and trigger fleet-wide autohealer kills (thundering herd
    on recovery).
    """

    snapshot_provider: Callable[[], grant_supervisor.SupervisorSnapshot]
    startup_time: float = field(default_factory=time.monotonic)
    last_heartbeat_tick: float | None = None


def _snapshot_payload(
    snapshot: grant_supervisor.SupervisorSnapshot,
) -> tuple[int, dict[str, dict[str, object]]]:
    """Render one bounded immutable supervisor snapshot for HTTP health."""
    grant_counts: dict[str, dict[str, object]] = {}
    active_feeds = 0
    for domain_id, count in snapshot.counts_by_domain.items():
        catalog_entry = worker_profiles.DOMAIN_CATALOG[domain_id]
        grant_counts[domain_id.value] = {
            "authority_kind": catalog_entry.authority_kind.value,
            "active": count.active,
            "retrying": count.retrying,
        }
        if domain_id is grant_control.DomainId.FEED:
            active_feeds = count.active
    return active_feeds, grant_counts


def _response_payload(
    state: HealthState,
    *,
    status: str,
    heartbeat_age_sec: float | None,
) -> dict[str, object]:
    snapshot = state.snapshot_provider()
    active_feeds, grant_counts = _snapshot_payload(snapshot)
    return {
        "status": status,
        "active_feeds": active_feeds,
        "last_heartbeat_age_sec": heartbeat_age_sec,
        "profile": snapshot.profile,
        "profile_digest": snapshot.profile_digest,
        "grant_counts": grant_counts,
    }


# Typed aiohttp app keys (the recommended pattern since aiohttp 3.9).
_STATE_KEY: web.AppKey[HealthState] = web.AppKey("state", HealthState)
_SETTINGS_KEY: web.AppKey[CollectorSettings] = web.AppKey("settings")


async def _healthz(request: web.Request) -> web.Response:
    state = request.app[_STATE_KEY]
    settings = request.app[_SETTINGS_KEY]
    now = time.monotonic()
    uptime = now - state.startup_time
    hb = state.last_heartbeat_tick

    # Gate 0: Startup grace. Return 200 for the first
    # health_check_startup_grace_sec regardless of state. The MIG autohealer's
    # initial_delay_sec covers boot, but GCP does NOT reset initial_delay_sec
    # when the container restarts on an existing VM — so this Python-side
    # grace also protects against "container crashed, systemd is restarting
    # it" cycles where connection-refused + 503 during boot would otherwise
    # unnecessarily tear down the VM.
    if uptime < settings.health_check_startup_grace_sec:
        return web.json_response(
            _response_payload(
                state,
                status="healthy",
                heartbeat_age_sec=(now - hb) if hb is not None else None,
            )
        )

    # Gate 1: Heartbeat dispatch freshness. Threshold = 2x
    # heartbeat_interval_sec.
    # Stamped at the start of each cycle (not on DB success), so this gate
    # fails only when the event loop stops dispatching the cycle at all —
    # not when the DB is transiently unreachable. Event-loop wedges are
    # normally self-recovered by _heartbeat_loop's 45s stall detector
    # (os._exit → systemd restart, well before autohealing would act). This
    # gate is the external backstop for the case where systemd can't restart
    # (Docker daemon wedged), where autohealing needs to replace the VM.
    heartbeat_max_age_sec = 2.0 * settings.heartbeat_interval_sec
    if hb is None:
        payload = _response_payload(
            state,
            status="unhealthy",
            heartbeat_age_sec=None,
        )
        payload["reason"] = "no_heartbeat"
        return web.json_response(
            payload,
            status=503,
        )
    hb_age = now - hb
    if hb_age > heartbeat_max_age_sec:
        payload = _response_payload(
            state,
            status="unhealthy",
            heartbeat_age_sec=hb_age,
        )
        payload["reason"] = "heartbeat_stale"
        return web.json_response(
            payload,
            status=503,
        )

    # Intentionally no "zero active feeds" gate. A worker with zero feeds is
    # not necessarily broken — in staging, during a scraper pause, or on an
    # over-provisioned fleet, idle workers are a valid state. Failing here
    # would crash-loop idle VMs fleet-wide whenever the upstream feed queue
    # dries up. If the lease loop itself crashes, the asyncio task will
    # propagate the exception and exit the process cleanly (systemd
    # restart); /healthz is not the right place to detect that.

    return web.json_response(
        _response_payload(
            state,
            status="healthy",
            heartbeat_age_sec=hb_age,
        )
    )


def build_app(
    settings: CollectorSettings, state: HealthState
) -> web.Application:
    """Build an aiohttp Application that serves GET /healthz."""
    app = web.Application()
    app[_STATE_KEY] = state
    app[_SETTINGS_KEY] = settings
    app.router.add_get("/healthz", _healthz)
    return app


async def start(
    settings: CollectorSettings,
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
