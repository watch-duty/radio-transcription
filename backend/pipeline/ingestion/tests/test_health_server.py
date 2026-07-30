from __future__ import annotations

import collections.abc
import time
import typing
import unittest
from typing import TYPE_CHECKING
from unittest import mock

from aiohttp.test_utils import AioHTTPTestCase

from backend.pipeline.ingestion import settings as ingestion_settings
from backend.pipeline.ingestion.health_server import HealthState, build_app

if TYPE_CHECKING:
    from aiohttp import web


def _fake_settings(
    *,
    startup_grace: float = 120.0,
    heartbeat_interval: float = 15.0,
) -> mock.Mock:
    """Duck-typed stand-in for CollectorSettings — only reads the fields /healthz uses."""
    settings = mock.Mock()
    settings.health_check_startup_grace_sec = startup_grace
    settings.heartbeat_interval_sec = heartbeat_interval
    settings.health_check_port = 8080
    return settings


class HealthzHandlerTests(AioHTTPTestCase):
    """
    Decision-matrix coverage for /healthz.

    Each test fixes ``startup_time`` and ``last_heartbeat_tick`` relative to
    ``now = time.monotonic()`` so the handler's uptime / age arithmetic is
    deterministic without patching the clock.
    """

    async def get_application(self) -> web.Application:
        self.settings = _fake_settings()
        self.state = HealthState(
            active_feed_count=lambda: 0,
            active_sid_count=lambda: 0,
            integrity_failed=lambda: False,
        )
        return build_app(self.settings, self.state)

    def test_public_annotations_resolve_at_runtime(self) -> None:
        state_hints = typing.get_type_hints(HealthState)
        app_hints = typing.get_type_hints(build_app)

        self.assertEqual(
            state_hints["active_feed_count"],
            collections.abc.Callable[[], int],
        )
        self.assertIs(
            app_hints["settings"],
            ingestion_settings.CollectorSettings,
        )

    async def _get_healthz(self) -> tuple[int, dict]:
        resp = await self.client.request("GET", "/healthz")
        body = await resp.json()
        return resp.status, body

    async def test_startup_grace_returns_healthy_without_heartbeat(
        self,
    ) -> None:
        """Within grace: 200 healthy even if heartbeat never happened."""
        now = time.monotonic()
        self.state.startup_time = now - 30.0  # 30s uptime, grace=120s
        self.state.last_heartbeat_tick = None

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "healthy")
        self.assertEqual(body["active_feeds"], 0)
        self.assertEqual(body["active_sids"], 0)
        self.assertEqual(body["bcfy_calls_authority_mode"], "sid_lease")
        self.assertIsNone(body["last_heartbeat_age_sec"])

    async def test_startup_grace_returns_healthy_even_with_stale_heartbeat(
        self,
    ) -> None:
        """Within grace: 200 regardless of state (spec: 'regardless of state')."""
        now = time.monotonic()
        self.state.startup_time = now - 30.0
        # Even a "stale" tick older than 2x interval doesn't fail during grace.
        self.state.last_heartbeat_tick = now - 100.0

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "healthy")

    async def test_integrity_failure_overrides_startup_grace_and_heartbeat(
        self,
    ) -> None:
        """Fatal supervisor evidence immediately fails process health."""
        now = time.monotonic()
        self.state.startup_time = now - 30.0
        self.state.last_heartbeat_tick = now - 1.0
        integrity_failed = mock.Mock(return_value=True)
        self.state.integrity_failed = integrity_failed

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "unhealthy")
        self.assertEqual(body["reason"], "integrity_failure")
        integrity_failed.assert_called_once_with()

    async def test_post_grace_missing_heartbeat_returns_unhealthy(self) -> None:
        """Post-grace, tick is None → 503 no_heartbeat."""
        now = time.monotonic()
        self.state.startup_time = now - 200.0  # past grace
        self.state.last_heartbeat_tick = None

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(
            body,
            {
                "status": "unhealthy",
                "reason": "no_heartbeat",
                "active_feeds": 0,
                "active_sids": 0,
                "bcfy_calls_authority_mode": "sid_lease",
                "last_heartbeat_age_sec": None,
            },
        )

    async def test_post_grace_stale_heartbeat_returns_unhealthy(self) -> None:
        """Post-grace, tick age > 2x interval → 503 heartbeat_stale."""
        now = time.monotonic()
        self.state.startup_time = now - 300.0
        # age = 40s, threshold = 2*15 = 30s → stale
        self.state.last_heartbeat_tick = now - 40.0

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "unhealthy")
        self.assertEqual(body["reason"], "heartbeat_stale")
        self.assertEqual(body["active_feeds"], 0)
        self.assertEqual(body["active_sids"], 0)
        self.assertEqual(body["bcfy_calls_authority_mode"], "sid_lease")
        self.assertIsInstance(body["last_heartbeat_age_sec"], (int, float))

    async def test_post_grace_zero_feeds_is_still_healthy(self) -> None:
        """
        Idle worker (0 feeds) with fresh heartbeat → 200.

        Regression guard: the original design failed 503 after 60s of zero
        feeds post-grace, which crash-looped idle workers (staging, scraper
        pause, over-provisioning). /healthz tests LOCAL process health — an
        empty upstream queue is not a local failure.
        """
        now = time.monotonic()
        self.state.startup_time = now - 3600.0  # 1h uptime — well past grace
        self.state.last_heartbeat_tick = now - 2.0

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "healthy")
        self.assertEqual(body["active_feeds"], 0)

    async def test_healthy_worker_returns_spec_response_shape(self) -> None:
        """Response body has exactly the keys the spec requires, no extras."""
        now = time.monotonic()
        self.state.startup_time = now - 400.0
        self.state.last_heartbeat_tick = now - 2.0
        active_feed_count = mock.Mock(return_value=3)
        active_sid_count = mock.Mock(return_value=2)
        self.state.active_feed_count = active_feed_count
        self.state.active_sid_count = active_sid_count
        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(
            set(body.keys()),
            {
                "status",
                "active_feeds",
                "active_sids",
                "bcfy_calls_authority_mode",
                "last_heartbeat_age_sec",
            },
        )
        self.assertEqual(body["status"], "healthy")
        self.assertEqual(body["active_feeds"], 3)
        self.assertEqual(body["active_sids"], 2)
        self.assertEqual(body["bcfy_calls_authority_mode"], "sid_lease")
        active_feed_count.assert_called_once_with()
        active_sid_count.assert_called_once_with()
        self.assertIsInstance(body["last_heartbeat_age_sec"], (int, float))


if __name__ == "__main__":
    unittest.main()
