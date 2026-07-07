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


def _worker_endpoints_from_env() -> tuple[str, ...]:
    raw = os.environ.get(
        "VM_HEALTH_WORKER_ENDPOINTS",
        ",".join(_DEFAULT_WORKER_ENDPOINTS),
    )
    return tuple(endpoint.strip() for endpoint in raw.split(",") if endpoint)


def _positive_float_from_env(name: str, default: str) -> float:
    return float(os.environ.get(name, default))


def _listen_port_from_env() -> int:
    return int(os.environ.get("VM_HEALTH_LISTEN_PORT", "8080"))


@dataclass(frozen=True, kw_only=True)
class VMHealthSettings:
    """Configuration for the VM-level health agent."""

    worker_endpoints: tuple[str, ...] = field(
        default_factory=_worker_endpoints_from_env,
    )
    probe_timeout_sec: float = field(
        default_factory=lambda: _positive_float_from_env(
            "VM_HEALTH_PROBE_TIMEOUT_SEC",
            "2.0",
        ),
    )
    probe_interval_sec: float = field(
        default_factory=lambda: _positive_float_from_env(
            "VM_HEALTH_PROBE_INTERVAL_SEC",
            "5.0",
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
        default_factory=lambda: _positive_float_from_env(
            "VM_HEALTH_HYSTERESIS_SEC",
            "600.0",
        ),
    )

    def __post_init__(self) -> None:
        numeric_values = (
            ("probe_timeout_sec", self.probe_timeout_sec),
            ("probe_interval_sec", self.probe_interval_sec),
            ("hysteresis_sec", self.hysteresis_sec),
        )
        for name, value in numeric_values:
            if not math.isfinite(value) or value <= 0:
                msg = f"{name} ({value}) must be finite and greater than zero."
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


@dataclass
class VMHealthState:
    all_workers_unhealthy_since: float | None = None
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
    ) -> VMHealthDecision:
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

        vm_healthy = not all_workers_unhealthy or elapsed < hysteresis_sec
        return VMHealthDecision(
            vm_healthy=vm_healthy,
            http_status=200 if vm_healthy else 503,
            workers=self.worker_results,
            all_workers_unhealthy_since=self.all_workers_unhealthy_since,
            all_workers_unhealthy_for_sec=elapsed,
            hysteresis_sec=hysteresis_sec,
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
    )
    return web.json_response(
        {
            "status": "healthy" if decision.vm_healthy else "unhealthy",
            "workers": [asdict(worker) for worker in decision.workers],
            "all_workers_unhealthy_for_sec": (
                decision.all_workers_unhealthy_for_sec
            ),
            "hysteresis_sec": decision.hysteresis_sec,
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
