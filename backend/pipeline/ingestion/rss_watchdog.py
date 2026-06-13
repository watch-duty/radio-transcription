"""RSS watchdog thread for ingestion claim back-pressure."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from typing import Protocol

from backend.pipeline.ingestion import cgroup_memory

logger = logging.getLogger(__name__)

MemoryReader = Callable[[], int | None]
MonotonicClock = Callable[[], float]


class RssWatchdogSettings(Protocol):
    """Settings required by RssWatchdog."""

    rss_watchdog_poll_interval_sec: float
    rss_watchdog_pause_threshold: float
    rss_watchdog_exit_threshold: float
    rss_watchdog_pause_consecutive_samples: int
    rss_watchdog_exit_consecutive_samples: int
    rss_watchdog_warmup_sec: float


class RssWatchdog:
    """
    Cgroup-aware RSS watchdog for the ingestion runtime.

    The watchdog runs in an OS daemon thread so it can keep polling even if
    the asyncio event loop is starved. It pauses new feed claims when memory
    crosses the sustained pause threshold and requests graceful shutdown when
    memory crosses the sustained exit threshold.
    """

    def __init__(
        self,
        settings: RssWatchdogSettings,
        thread_stop: threading.Event,
        *,
        limit_reader: MemoryReader = (
            cgroup_memory.resolve_container_memory_limit_bytes
        ),
        usage_reader: MemoryReader = (
            cgroup_memory.resolve_container_memory_usage_bytes
        ),
        monotonic: MonotonicClock = time.monotonic,
    ) -> None:
        self._settings = settings
        self._thread_stop = thread_stop
        self._limit_reader = limit_reader
        self._usage_reader = usage_reader
        self._monotonic = monotonic
        self._paused = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False

    def start(
        self,
        loop: asyncio.AbstractEventLoop,
        shutdown_event: asyncio.Event,
    ) -> None:
        """Start the watchdog thread, or disable it if no cgroup limit exists."""
        if self._started:
            msg = "RSS watchdog cannot be started more than once"
            raise RuntimeError(msg)
        self._started = True

        limit_bytes = self._limit_reader()
        if limit_bytes is None:
            logger.warning(
                "RSS watchdog disabled — no cgroup memory limit detected",
            )
            return

        self._thread = threading.Thread(
            target=self._run,
            args=(limit_bytes, loop, shutdown_event),
            daemon=True,
            name="rss-watchdog",
        )
        self._thread.start()

    def is_paused(self) -> bool:
        """Return True when memory back-pressure is pausing new claims."""
        return self._paused.is_set()

    async def join(self, timeout_sec: float) -> None:
        """Join the watchdog thread without blocking the asyncio event loop."""
        if self._thread is not None and self._thread.is_alive():
            await asyncio.to_thread(self._thread.join, timeout=timeout_sec)

    def _run(  # noqa: PLR0912
        self,
        limit_bytes: int,
        loop: asyncio.AbstractEventLoop,
        shutdown_event: asyncio.Event,
    ) -> None:
        """
        Poll cgroup memory and gate claims via pause/trip thresholds.

        Logging is transition-only: startup, pause-set, pause-clear, read
        failure, and trip.
        """
        settings = self._settings
        poll_interval = settings.rss_watchdog_poll_interval_sec
        pause_threshold = settings.rss_watchdog_pause_threshold
        exit_threshold = settings.rss_watchdog_exit_threshold
        resume_threshold = pause_threshold - 0.10
        pause_consec_target = settings.rss_watchdog_pause_consecutive_samples
        exit_consec_target = settings.rss_watchdog_exit_consecutive_samples
        warmup_sec = settings.rss_watchdog_warmup_sec
        watchdog_start = self._monotonic()

        pause_consec = 0
        exit_consec = 0
        usage_read_error_logged = False

        logger.info(
            "RSS watchdog limit = %d MB (cgroup detected)",
            limit_bytes // (1024 * 1024),
        )

        while not self._thread_stop.is_set():
            if self._thread_stop.wait(timeout=poll_interval):
                break

            if (self._monotonic() - watchdog_start) < warmup_sec:
                continue

            usage_bytes = self._usage_reader()
            if usage_bytes is None:
                if not self._paused.is_set():
                    self._paused.set()
                if not usage_read_error_logged:
                    logger.error(
                        "RSS watchdog: failed to read cgroup memory.current "
                        "— defensively pausing claims",
                    )
                    usage_read_error_logged = True
                continue

            usage_read_error_logged = False
            ratio = usage_bytes / limit_bytes

            if ratio >= exit_threshold:
                exit_consec += 1
            else:
                exit_consec = 0

            if exit_consec >= exit_consec_target:
                logger.error(
                    "RSS watchdog: %.1f%% >= exit threshold for %d samples "
                    "— initiating graceful shutdown",
                    ratio * 100.0,
                    exit_consec,
                )
                loop.call_soon_threadsafe(shutdown_event.set)
                self._thread_stop.set()
                continue

            if ratio >= pause_threshold:
                pause_consec += 1
            else:
                pause_consec = 0

            if (
                pause_consec >= pause_consec_target
                and not self._paused.is_set()
            ):
                self._paused.set()
                logger.warning(
                    "RSS watchdog: %.1f%% >= pause threshold for %d samples "
                    "— pausing claims",
                    ratio * 100.0,
                    pause_consec,
                )

            if self._paused.is_set() and ratio < resume_threshold:
                self._paused.clear()
                logger.info(
                    "RSS watchdog: %.1f%% — resuming claims",
                    ratio * 100.0,
                )
