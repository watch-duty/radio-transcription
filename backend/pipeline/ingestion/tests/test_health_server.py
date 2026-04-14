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
    startup_grace: float = 120.0,
    zero_feeds_max: float = 60.0,
    heartbeat_interval: float = 15.0,
) -> mock.Mock:
    """Duck-typed stand-in for NormalizerSettings — only reads the fields /healthz uses."""
    settings = mock.Mock()
    settings.health_check_startup_grace_sec = startup_grace
    settings.health_check_zero_feeds_max_sec = zero_feeds_max
    settings.heartbeat_interval_sec = heartbeat_interval
    settings.health_check_port = 8080
    return settings


class HealthzHandlerTests(AioHTTPTestCase):
    """
    Decision-matrix coverage for /healthz.

    Each test fixes ``startup_time``, ``last_heartbeat_completed``, and
    ``last_nonzero_feeds_at`` relative to ``now = time.monotonic()``, so the
    handler's uptime / age arithmetic is deterministic without patching the
    clock.
    """

    async def get_application(self) -> web.Application:
        self.settings = _fake_settings()
        self.state = HealthState()
        return build_app(self.settings, self.state)

    async def _get_healthz(self) -> tuple[int, dict]:
        resp = await self.client.request("GET", "/healthz")
        body = await resp.json()
        return resp.status, body

    async def test_startup_grace_returns_healthy_without_heartbeat(self) -> None:
        """Within grace: 200 healthy even if heartbeat never happened."""
        now = time.monotonic()
        self.state.startup_time = now - 30.0  # 30s uptime, grace=120s
        self.state.last_heartbeat_completed = None

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "healthy")
        self.assertEqual(body["active_feeds"], 0)
        self.assertIsNone(body["last_heartbeat_age_sec"])

    async def test_startup_grace_returns_healthy_even_if_stale_heartbeat(self) -> None:
        """Within grace: 200 regardless of state (spec: 'regardless of state')."""
        now = time.monotonic()
        self.state.startup_time = now - 30.0
        # Even a "stale" heartbeat older than 2×interval doesn't fail during grace.
        self.state.last_heartbeat_completed = now - 100.0

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "healthy")

    async def test_post_grace_missing_heartbeat_returns_unhealthy(self) -> None:
        """Post-grace, hb is None → 503 no_heartbeat."""
        now = time.monotonic()
        self.state.startup_time = now - 200.0  # past grace
        self.state.last_heartbeat_completed = None
        self.state.last_nonzero_feeds_at = now - 10.0  # not the gate under test

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "unhealthy")
        self.assertEqual(body["reason"], "no_heartbeat")

    async def test_post_grace_stale_heartbeat_returns_unhealthy(self) -> None:
        """Post-grace, heartbeat age > 2x interval → 503 heartbeat_stale."""
        now = time.monotonic()
        self.state.startup_time = now - 300.0
        # age = 40s, threshold = 2*15 = 30s → stale
        self.state.last_heartbeat_completed = now - 40.0
        self.state.last_nonzero_feeds_at = now - 1.0

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "heartbeat_stale")

    async def test_post_grace_sustained_zero_feeds_returns_unhealthy(self) -> None:
        """Past (grace + zero_feeds_max) with zero feeds throughout → 503."""
        now = time.monotonic()
        # startup 200s ago, grace 120s, zero-window 60s ⇒ 200 > 120+60=180s zero
        self.state.startup_time = now - 200.0
        self.state.last_heartbeat_completed = now - 1.0
        self.state.last_nonzero_feeds_at = None  # never had feeds

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "zero_active_feeds")

    async def test_post_grace_recently_lost_feeds_still_healthy(self) -> None:
        """
        Past grace, currently zero feeds, but last_nonzero_feeds_at is fresh
        (<60s ago) → 200. Proves the zero-feed clock measures from last-nonzero,
        not from grace-end — so brief drops between leases don't flap.
        """
        now = time.monotonic()
        self.state.startup_time = now - 400.0
        self.state.last_heartbeat_completed = now - 1.0
        # Had feeds 30s ago, dropped to zero; 30 < 60 window → still healthy
        self.state.last_nonzero_feeds_at = now - 30.0

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "healthy")
        self.assertEqual(body["active_feeds"], 0)

    async def test_healthy_worker_returns_spec_response_shape(self) -> None:
        """Response body has exactly the keys the spec requires, no extras."""
        now = time.monotonic()
        self.state.startup_time = now - 400.0
        self.state.last_heartbeat_completed = now - 2.0
        self.state.last_nonzero_feeds_at = now - 1.0
        self.state.feed_tasks.update({
            "feed-1": object(),
            "feed-2": object(),
            "feed-3": object(),
        })

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(
            set(body.keys()),
            {"status", "active_feeds", "last_heartbeat_age_sec"},
        )
        self.assertEqual(body["status"], "healthy")
        self.assertEqual(body["active_feeds"], 3)
        self.assertIsInstance(body["last_heartbeat_age_sec"], (int, float))


if __name__ == "__main__":
    unittest.main()
