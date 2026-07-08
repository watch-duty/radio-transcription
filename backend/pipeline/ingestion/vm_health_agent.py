from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import logging
import math
import os
import signal
import time
import urllib.parse
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING

import aiohttp
from aiohttp import web

from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.common.tracing_utils import setup_tracing

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

logger = logging.getLogger(__name__)

_DEFAULT_WORKER_ENDPOINTS = (
    "http://127.0.0.1:8081/healthz",
    "http://127.0.0.1:8082/healthz",
)
_DEFAULT_PROBE_TIMEOUT_SEC = 2.0
_DEFAULT_PROBE_INTERVAL_SEC = 5.0
_DEFAULT_LISTEN_PORT = 8080
_DEFAULT_HYSTERESIS_SEC = 600.0

_MIN_PROBE_TIMEOUT_SEC = 0.1
_MAX_PROBE_TIMEOUT_SEC = 10.0
_MIN_PROBE_INTERVAL_SEC = 1.0
_MAX_PROBE_INTERVAL_SEC = 60.0
_MIN_HYSTERESIS_SEC = 60.0
_MAX_HYSTERESIS_SEC = 3600.0

# Fail closed when the VM Health agent itself stops completing probes for
# three expected cycles. This is monitor self-health, not worker health, so it
# intentionally bypasses the worker-down hysteresis.
_PROBE_STALE_MISSED_CYCLES = 3.0


def _worker_endpoints_from_env() -> tuple[str, ...]:
    raw = os.environ.get(
        "VM_HEALTH_WORKER_ENDPOINTS",
        ",".join(_DEFAULT_WORKER_ENDPOINTS),
    )
    return tuple(
        stripped
        for endpoint in raw.split(",")
        if (stripped := endpoint.strip())
    )


def _float_from_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        msg = f"{name} ({raw!r}) must be a float."
        raise ValueError(msg) from exc


def _listen_port_from_env() -> int:
    raw = os.environ.get("VM_HEALTH_LISTEN_PORT")
    if raw is None:
        return _DEFAULT_LISTEN_PORT
    try:
        return int(raw)
    except ValueError as exc:
        msg = f"VM_HEALTH_LISTEN_PORT ({raw!r}) must be an integer."
        raise ValueError(msg) from exc


def _validate_float_range(
    name: str,
    value: float,
    *,
    min_value: float,
    max_value: float,
) -> None:
    if not math.isfinite(value) or value < min_value or value > max_value:
        msg = (
            f"{name} ({value}s) must be finite and between "
            f"{min_value}s and {max_value}s."
        )
        raise ValueError(msg)


@dataclass(frozen=True, kw_only=True)
class VMHealthSettings:
    """Configuration for the VM-level health agent."""

    worker_endpoints: tuple[str, ...] = field(
        default_factory=_worker_endpoints_from_env,
    )
    probe_timeout_sec: float = field(
        default_factory=lambda: _float_from_env(
            "VM_HEALTH_PROBE_TIMEOUT_SEC",
            _DEFAULT_PROBE_TIMEOUT_SEC,
        ),
    )
    probe_interval_sec: float = field(
        default_factory=lambda: _float_from_env(
            "VM_HEALTH_PROBE_INTERVAL_SEC",
            _DEFAULT_PROBE_INTERVAL_SEC,
        ),
    )
    listen_host: str = field(
        default_factory=lambda: os.environ.get(
            "VM_HEALTH_LISTEN_HOST",
            "0.0.0.0",  # noqa: S104 - VM health must bind the host port.
        ),
    )
    listen_port: int = field(default_factory=_listen_port_from_env)
    hysteresis_sec: float = field(
        default_factory=lambda: _float_from_env(
            "VM_HEALTH_HYSTERESIS_SEC",
            _DEFAULT_HYSTERESIS_SEC,
        ),
    )

    def __post_init__(self) -> None:
        float_ranges = (
            (
                "probe_timeout_sec",
                self.probe_timeout_sec,
                _MIN_PROBE_TIMEOUT_SEC,
                _MAX_PROBE_TIMEOUT_SEC,
            ),
            (
                "probe_interval_sec",
                self.probe_interval_sec,
                _MIN_PROBE_INTERVAL_SEC,
                _MAX_PROBE_INTERVAL_SEC,
            ),
            (
                "hysteresis_sec",
                self.hysteresis_sec,
                _MIN_HYSTERESIS_SEC,
                _MAX_HYSTERESIS_SEC,
            ),
        )
        for name, value, min_value, max_value in float_ranges:
            _validate_float_range(
                name,
                value,
                min_value=min_value,
                max_value=max_value,
            )

        if self.probe_timeout_sec >= self.probe_interval_sec:
            msg = (
                f"probe_timeout_sec ({self.probe_timeout_sec}s) must be less "
                f"than probe_interval_sec ({self.probe_interval_sec}s)."
            )
            raise ValueError(msg)

        if self.listen_port <= 0 or self.listen_port > 65535:
            msg = (
                f"listen_port ({self.listen_port}) must be between 1 and 65535."
            )
            raise ValueError(msg)

        if not self.worker_endpoints:
            msg = "worker_endpoints must contain at least one local worker URL."
            raise ValueError(msg)

        for endpoint in self.worker_endpoints:
            _validate_worker_endpoint(endpoint)

        worker_ports = {
            urllib.parse.urlparse(endpoint).port
            for endpoint in self.worker_endpoints
        }
        if len(worker_ports) != len(self.worker_endpoints):
            msg = "worker_endpoints must reference distinct local ports."
            raise ValueError(msg)
        if self.listen_port in worker_ports:
            msg = (
                "worker_endpoints must not reference the VM Health listen_port."
            )
            raise ValueError(msg)

    @property
    def probe_stale_after_sec(self) -> float:
        """Return the monitor self-health deadline for missed probe cycles."""
        return self.probe_interval_sec * _PROBE_STALE_MISSED_CYCLES


@dataclass(frozen=True, kw_only=True)
class WorkerProbeResult:
    url: str
    healthy: bool
    status_code: int | None
    error: str | None = None


@dataclass(frozen=True, kw_only=True)
class VMHealthDecision:
    vm_healthy: bool
    http_status: int
    workers: tuple[WorkerProbeResult, ...]
    all_workers_unhealthy_since: float | None
    all_workers_unhealthy_for_sec: float
    hysteresis_sec: float
    probe_stale: bool
    last_probe_completed_ago_sec: float | None
    probe_stale_after_sec: float | None


@dataclass
class VMHealthState:
    started_at: float = field(default_factory=time.monotonic)
    all_workers_unhealthy_since: float | None = None
    last_probe_completed_at: float | None = None
    worker_results: tuple[WorkerProbeResult, ...] = field(
        default_factory=tuple,
    )

    def update(
        self,
        results: Sequence[WorkerProbeResult],
        *,
        now: float,
        hysteresis_sec: float,
    ) -> VMHealthDecision:
        self.worker_results = tuple(results)
        self.last_probe_completed_at = now
        all_workers_unhealthy = bool(self.worker_results) and all(
            not result.healthy for result in self.worker_results
        )

        if all_workers_unhealthy:
            if self.all_workers_unhealthy_since is None:
                self.all_workers_unhealthy_since = now
        else:
            self.all_workers_unhealthy_since = None

        return self.current_decision(
            now=now,
            hysteresis_sec=hysteresis_sec,
        )

    def current_decision(
        self,
        *,
        now: float,
        hysteresis_sec: float,
        probe_stale_after_sec: float | None = None,
    ) -> VMHealthDecision:
        # Default to healthy before the first probe completes so the MIG does
        # not recycle an otherwise booting instance.
        last_probe_completed_ago_sec: float | None = None
        probe_stale = False
        if self.last_probe_completed_at is not None:
            last_probe_completed_ago_sec = max(
                0.0,
                now - self.last_probe_completed_at,
            )
            probe_stale = (
                probe_stale_after_sec is not None
                and last_probe_completed_ago_sec > probe_stale_after_sec
            )
        elif probe_stale_after_sec is not None:
            probe_stale = (
                max(0.0, now - self.started_at) > probe_stale_after_sec
            )

        all_workers_unhealthy = bool(self.worker_results) and all(
            not result.healthy for result in self.worker_results
        )
        if (
            all_workers_unhealthy
            and self.all_workers_unhealthy_since is not None
        ):
            elapsed = max(0.0, now - self.all_workers_unhealthy_since)
        else:
            elapsed = 0.0

        vm_healthy = not probe_stale and (
            not all_workers_unhealthy or elapsed < hysteresis_sec
        )
        return VMHealthDecision(
            vm_healthy=vm_healthy,
            http_status=200 if vm_healthy else 503,
            workers=self.worker_results,
            all_workers_unhealthy_since=self.all_workers_unhealthy_since,
            all_workers_unhealthy_for_sec=elapsed,
            hysteresis_sec=hysteresis_sec,
            probe_stale=probe_stale,
            last_probe_completed_ago_sec=last_probe_completed_ago_sec,
            probe_stale_after_sec=probe_stale_after_sec,
        )


_SETTINGS_KEY: web.AppKey[VMHealthSettings] = web.AppKey(
    "settings",
    VMHealthSettings,
)
_STATE_KEY: web.AppKey[VMHealthState] = web.AppKey("state", VMHealthState)


def _validate_worker_endpoint(endpoint: str) -> None:
    parsed = urllib.parse.urlparse(endpoint)
    if parsed.scheme != "http":
        msg = f"VM Health worker endpoint must use http: {endpoint}"
        raise ValueError(msg)

    host = parsed.hostname
    if host is None:
        msg = f"VM Health worker endpoint must include a host: {endpoint}"
        raise ValueError(msg)

    try:
        port = parsed.port
    except ValueError as exc:
        msg = f"VM Health worker endpoint has invalid port: {endpoint}"
        raise ValueError(msg) from exc
    if port is None:
        msg = (
            "VM Health worker endpoint must include an explicit port: "
            f"{endpoint}"
        )
        raise ValueError(msg)
    if port <= 0 or port > 65535:
        msg = f"VM Health worker endpoint has invalid port: {endpoint}"
        raise ValueError(msg)

    if host.lower() == "localhost":
        return

    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        msg = f"VM Health worker endpoint must use a loopback host: {endpoint}"
        raise ValueError(msg) from exc

    if not address.is_loopback:
        msg = f"VM Health worker endpoint must use a loopback host: {endpoint}"
        raise ValueError(msg)


async def probe_worker(
    session: aiohttp.ClientSession,
    url: str,
    *,
    timeout_sec: float,
) -> WorkerProbeResult:
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    try:
        async with session.get(
            url,
            timeout=timeout,
            allow_redirects=False,
        ) as response:
            return WorkerProbeResult(
                url=url,
                healthy=200 <= response.status < 300,
                status_code=response.status,
            )
    except TimeoutError:
        return WorkerProbeResult(
            url=url,
            healthy=False,
            status_code=None,
            error="TimeoutError",
        )
    except aiohttp.ClientError as exc:
        return WorkerProbeResult(
            url=url,
            healthy=False,
            status_code=None,
            error=exc.__class__.__name__,
        )
    except Exception as exc:
        return WorkerProbeResult(
            url=url,
            healthy=False,
            status_code=None,
            error=exc.__class__.__name__,
        )


async def probe_workers(
    session: aiohttp.ClientSession,
    settings: VMHealthSettings,
) -> tuple[WorkerProbeResult, ...]:
    return tuple(
        await asyncio.gather(
            *(
                probe_worker(
                    session,
                    endpoint,
                    timeout_sec=settings.probe_timeout_sec,
                )
                for endpoint in settings.worker_endpoints
            ),
        )
    )


async def _probe_loop(
    session: aiohttp.ClientSession,
    settings: VMHealthSettings,
    state: VMHealthState,
) -> None:
    while True:
        try:
            results = await probe_workers(session, settings)
            state.update(
                results,
                now=time.monotonic(),
                hysteresis_sec=settings.hysteresis_sec,
            )
        except Exception:
            logger.warning("VM Health probe cycle failed", exc_info=True)
        await asyncio.sleep(settings.probe_interval_sec)


async def _healthz(request: web.Request) -> web.Response:
    settings = request.app[_SETTINGS_KEY]
    state = request.app[_STATE_KEY]
    decision = state.current_decision(
        now=time.monotonic(),
        hysteresis_sec=settings.hysteresis_sec,
        probe_stale_after_sec=settings.probe_stale_after_sec,
    )
    return web.json_response(
        {
            "status": "healthy" if decision.vm_healthy else "unhealthy",
            "workers": [asdict(worker) for worker in decision.workers],
            "all_workers_unhealthy_for_sec": (
                decision.all_workers_unhealthy_for_sec
            ),
            "hysteresis_sec": decision.hysteresis_sec,
            "probe_stale": decision.probe_stale,
            "last_probe_completed_ago_sec": (
                decision.last_probe_completed_ago_sec
            ),
            "probe_stale_after_sec": decision.probe_stale_after_sec,
        },
        status=decision.http_status,
    )


def build_app(
    settings: VMHealthSettings,
    state: VMHealthState,
) -> web.Application:
    """Build an aiohttp Application that serves GET /healthz."""
    app = web.Application()
    app[_SETTINGS_KEY] = settings
    app[_STATE_KEY] = state
    app.router.add_get("/healthz", _healthz)
    return app


async def start(
    settings: VMHealthSettings,
    state: VMHealthState,
) -> web.AppRunner:
    """Start the VM Health HTTP server and background worker probes."""
    app = build_app(settings, state)
    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=settings.probe_timeout_sec),
    )
    probe_task = asyncio.create_task(_probe_loop(session, settings, state))

    async def cleanup(_: web.Application) -> None:
        probe_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await probe_task
        await session.close()

    app.on_cleanup.append(cleanup)
    runner = web.AppRunner(app, access_log=None)
    try:
        await runner.setup()
        site = web.TCPSite(
            runner,
            host=settings.listen_host,
            port=settings.listen_port,
        )
        await site.start()
    except Exception:
        await runner.cleanup()
        raise

    logger.info(
        "VM Health server listening on %s:%d/healthz",
        settings.listen_host,
        settings.listen_port,
    )
    return runner


async def _serve_forever(settings: VMHealthSettings) -> None:
    shutdown = asyncio.Event()
    remove_signal_handlers = _install_shutdown_signal_handlers(shutdown)
    try:
        state = VMHealthState()
        runner = await start(settings, state)
        try:
            await shutdown.wait()
        finally:
            await runner.cleanup()
    finally:
        remove_signal_handlers()


def _install_shutdown_signal_handlers(
    shutdown: asyncio.Event,
) -> Callable[[], None]:
    loop = asyncio.get_running_loop()
    registered: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except (NotImplementedError, RuntimeError):
            logger.debug(
                "VM Health shutdown signal handlers unavailable",
                exc_info=True,
            )
            break
        registered.append(sig)

    def remove_signal_handlers() -> None:
        for sig in registered:
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(sig)

    return remove_signal_handlers


def main() -> None:
    setup_logging()
    setup_tracing(
        service_name="ingestion-vm-health-agent",
        is_ingestion=True,
    )
    asyncio.run(_serve_forever(VMHealthSettings()))


if __name__ == "__main__":
    main()
