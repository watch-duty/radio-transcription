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
    grace: float = 60.0,
    max_age: float = 45.0,
    feed_grace: float = 300.0,
) -> mock.Mock:
    """Duck-typed stand-in for NormalizerSettings — only reads the fields /healthz uses."""
    settings = mock.Mock()
    settings.health_check_heartbeat_grace_sec = grace
    settings.health_check_heartbeat_max_age_sec = max_age
    settings.health_check_feed_grace_sec = feed_grace
    settings.health_check_port = 8080
    return settings


class HealthzHandlerTests(AioHTTPTestCase):
    """
    Decision-matrix coverage for /healthz.

    The handler relies on ``time.monotonic`` for both ``startup_time`` and
    ``last_heartbeat_completed``. We set both explicitly so each test fixes
    ``uptime`` and ``heartbeat_age`` independently, without patching the clock.
    """

    async def get_application(self) -> web.Application:
        self.settings = _fake_settings()
        self.state = HealthState()
        return build_app(self.settings, self.state)

    async def _get_healthz(self) -> tuple[int, dict]:
        resp = await self.client.request("GET", "/healthz")
        body = await resp.json()
        return resp.status, body

    async def test_starting_grace_no_heartbeat_returns_ok(self) -> None:
        """Uptime < grace AND last_heartbeat_completed is None -> 200 starting."""
        now = time.monotonic()
        self.state.startup_time = now - 10.0  # 10s uptime, grace is 60s
        self.state.last_heartbeat_completed = None

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["reason"], "starting")

    async def test_after_grace_no_heartbeat_returns_unhealthy(self) -> None:
        """Uptime >= grace AND last_heartbeat_completed is None -> 503."""
        now = time.monotonic()
        self.state.startup_time = now - 120.0  # 2min uptime, grace is 60s
        self.state.last_heartbeat_completed = None

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "no_heartbeat_yet")

    async def test_fresh_heartbeat_within_feed_grace_returns_ok(self) -> None:
        """Fresh stamp, uptime < feed_grace, zero feeds -> 200 (still in grace)."""
        now = time.monotonic()
        self.state.startup_time = now - 60.0  # 1min uptime, feed_grace is 300s
        self.state.last_heartbeat_completed = now - 5.0  # 5s age, max_age is 45s
        # feed_tasks stays empty -> 0 active feeds, but within grace.

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["active_feeds"], 0)

    async def test_stale_heartbeat_returns_unhealthy(self) -> None:
        """heartbeat_age > max_age -> 503 heartbeat_stale."""
        now = time.monotonic()
        self.state.startup_time = now - 600.0  # 10min uptime, well past all graces
        self.state.last_heartbeat_completed = now - 60.0  # 60s age, max_age is 45s

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "heartbeat_stale")
        self.assertGreater(body["heartbeat_age_s"], 45.0)

    async def test_no_active_feeds_after_feed_grace_returns_unhealthy(self) -> None:
        """Uptime > feed_grace AND 0 active feeds -> 503 no_active_feeds."""
        now = time.monotonic()
        self.state.startup_time = now - 600.0  # 10min uptime
        self.state.last_heartbeat_completed = now - 5.0  # fresh stamp
        # feed_tasks stays empty -> 0 active feeds.

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "no_active_feeds")
        self.assertEqual(body["active_feeds"], 0)

    async def test_healthy_worker_returns_ok_with_feed_count(self) -> None:
        """Fresh stamp, past feed_grace, feeds leased -> 200 with active_feeds."""
        now = time.monotonic()
        self.state.startup_time = now - 600.0
        self.state.last_heartbeat_completed = now - 2.0
        # Simulate 3 leased feeds; values don't matter, the handler only
        # calls len() on the dict.
        self.state.feed_tasks.update({
            "feed-1": object(),
            "feed-2": object(),
            "feed-3": object(),
        })

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["active_feeds"], 3)


if __name__ == "__main__":
    unittest.main()
