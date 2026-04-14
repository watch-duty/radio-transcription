from __future__ import annotations

import time
import unittest
from typing import TYPE_CHECKING
from unittest import mock

from aiohttp.test_utils import AioHTTPTestCase

from backend.pipeline.ingestion.health_server import HealthState, build_app

if TYPE_CHECKING:
    from aiohttp import web


def _fake_settings(
    *,
    heartbeat_max_age: float = 45.0,
    lease_attempt_max_age: float = 30.0,
) -> mock.Mock:
    """Duck-typed stand-in for NormalizerSettings — only reads the fields /healthz uses."""
    settings = mock.Mock()
    settings.health_check_heartbeat_max_age_sec = heartbeat_max_age
    settings.health_check_lease_attempt_max_age_sec = lease_attempt_max_age
    settings.health_check_port = 8080
    return settings


class HealthzHandlerTests(AioHTTPTestCase):
    """
    Decision-matrix coverage for /healthz.

    The handler relies on ``time.monotonic`` for ``startup_time``,
    ``last_heartbeat_completed``, and ``last_lease_attempt_completed``. Each
    test fixes those absolutely (relative to ``now = time.monotonic()``) so
    uptime/age are deterministic without patching the clock.
    """

    async def get_application(self) -> web.Application:
        self.settings = _fake_settings()
        self.state = HealthState()
        return build_app(self.settings, self.state)

    async def _get_healthz(self) -> tuple[int, dict]:
        resp = await self.client.request("GET", "/healthz")
        body = await resp.json()
        return resp.status, body

    async def test_no_heartbeat_yet_returns_unhealthy(self) -> None:
        """last_heartbeat_completed is None -> 503 (no grace period)."""
        now = time.monotonic()
        self.state.startup_time = now - 2.0  # fresh VM, any uptime
        self.state.last_heartbeat_completed = None
        self.state.last_lease_attempt_completed = now

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "no_heartbeat_yet")

    async def test_stale_heartbeat_returns_unhealthy(self) -> None:
        """heartbeat_age > max_age -> 503 heartbeat_stale."""
        now = time.monotonic()
        self.state.startup_time = now - 600.0
        self.state.last_heartbeat_completed = now - 60.0  # 60s age, max=45s
        self.state.last_lease_attempt_completed = now

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "heartbeat_stale")
        self.assertGreater(body["heartbeat_age_s"], 45.0)

    async def test_no_lease_attempt_yet_returns_unhealthy(self) -> None:
        """Heartbeat stamped but leasing loop never iterated -> 503."""
        now = time.monotonic()
        self.state.startup_time = now - 10.0
        self.state.last_heartbeat_completed = now - 2.0
        self.state.last_lease_attempt_completed = None

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "no_lease_attempt_yet")

    async def test_stale_lease_attempt_returns_unhealthy(self) -> None:
        """lease_attempt_age > max_age -> 503 lease_loop_stalled."""
        now = time.monotonic()
        self.state.startup_time = now - 600.0
        self.state.last_heartbeat_completed = now - 2.0
        self.state.last_lease_attempt_completed = now - 45.0  # 45s age, max=30s

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "lease_loop_stalled")
        self.assertGreater(body["lease_attempt_age_s"], 30.0)

    async def test_healthy_worker_with_feeds_returns_ok(self) -> None:
        """Fresh heartbeat + fresh lease attempt + feeds leased -> 200."""
        now = time.monotonic()
        self.state.startup_time = now - 600.0
        self.state.last_heartbeat_completed = now - 2.0
        self.state.last_lease_attempt_completed = now - 1.0
        self.state.feed_tasks.update({
            "feed-1": object(),
            "feed-2": object(),
            "feed-3": object(),
        })

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["active_feeds"], 3)

    async def test_healthy_worker_with_zero_feeds_returns_ok(self) -> None:
        """
        0 feeds is NOT a failure on its own — underload is a valid state.

        Regression guard for the original design where uptime > feed_grace AND
        0 feeds returned 503, which crash-looped idle workers.
        """
        now = time.monotonic()
        self.state.startup_time = now - 3600.0  # 1h uptime — no time-based gate
        self.state.last_heartbeat_completed = now - 2.0
        self.state.last_lease_attempt_completed = now - 1.0
        # feed_tasks intentionally empty.

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["active_feeds"], 0)


if __name__ == "__main__":
    unittest.main()
