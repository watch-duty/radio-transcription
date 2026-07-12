from __future__ import annotations

import time
import types
import unittest
from typing import TYPE_CHECKING
from unittest import mock

from aiohttp.test_utils import AioHTTPTestCase

from backend.pipeline.ingestion import (
    grant_control,
    grant_supervisor,
    worker_profiles,
)
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


def _snapshot(
    *,
    feed_active: int = 0,
    feed_retrying: int = 0,
    feed_failing: int = 0,
    sid_active: int | None = None,
) -> grant_supervisor.SupervisorSnapshot:
    """Build one immutable local health projection."""
    counts = {
        grant_control.DomainId.FEED: grant_supervisor.GrantCount(
            active=feed_active,
            retrying=feed_retrying,
            durable_failing=feed_failing,
        )
    }
    if sid_active is not None:
        counts[grant_control.DomainId.SID] = grant_supervisor.GrantCount(
            active=sid_active,
            retrying=0,
            durable_failing=0,
        )
    return grant_supervisor.SupervisorSnapshot(
        profile=worker_profiles.LEGACY_PROFILE.name,
        profile_digest=worker_profiles.profile_digest(
            worker_profiles.LEGACY_PROFILE
        ),
        counts_by_domain=types.MappingProxyType(counts),
    )


class HealthzHandlerTests(AioHTTPTestCase):
    """
    Decision-matrix coverage for /healthz.

    Each test fixes ``startup_time`` and ``last_heartbeat_tick`` relative to
    ``now = time.monotonic()`` so the handler's uptime / age arithmetic is
    deterministic without patching the clock.
    """

    async def get_application(self) -> web.Application:
        self.settings = _fake_settings()
        self.state = HealthState(snapshot_provider=_snapshot)
        return build_app(self.settings, self.state)

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

    async def test_post_grace_missing_heartbeat_returns_unhealthy(self) -> None:
        """Post-grace, tick is None → 503 no_heartbeat."""
        now = time.monotonic()
        self.state.startup_time = now - 200.0  # past grace
        self.state.last_heartbeat_tick = None

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "unhealthy")
        self.assertEqual(body["reason"], "no_heartbeat")

    async def test_post_grace_stale_heartbeat_returns_unhealthy(self) -> None:
        """Post-grace, tick age > 2x interval → 503 heartbeat_stale."""
        now = time.monotonic()
        self.state.startup_time = now - 300.0
        # age = 40s, threshold = 2*15 = 30s → stale
        self.state.last_heartbeat_tick = now - 40.0

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["reason"], "heartbeat_stale")

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
        # The immutable snapshot intentionally reports zero Feed grants.

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "healthy")
        self.assertEqual(body["active_feeds"], 0)

    async def test_healthy_worker_returns_spec_response_shape(self) -> None:
        """Response body has exactly the keys the spec requires, no extras."""
        now = time.monotonic()
        self.state.startup_time = now - 400.0
        self.state.last_heartbeat_tick = now - 2.0
        snapshot = _snapshot(
            feed_active=3,
            feed_retrying=1,
            feed_failing=2,
        )
        snapshot_provider = mock.Mock(return_value=snapshot)
        self.state.snapshot_provider = snapshot_provider

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(
            set(body.keys()),
            {
                "status",
                "active_feeds",
                "last_heartbeat_age_sec",
                "profile",
                "profile_digest",
                "grant_counts",
            },
        )
        self.assertEqual(body["status"], "healthy")
        self.assertEqual(body["active_feeds"], 3)
        snapshot_provider.assert_called_once_with()
        self.assertIsInstance(body["last_heartbeat_age_sec"], (int, float))
        self.assertEqual(body["profile"], "legacy")
        self.assertEqual(
            body["profile_digest"],
            worker_profiles.profile_digest(worker_profiles.LEGACY_PROFILE),
        )
        self.assertEqual(
            body["grant_counts"],
            {
                "feed": {
                    "authority_kind": "feed",
                    "active": 3,
                    "retrying": 1,
                    "durable_failing": 2,
                }
            },
        )
        serialized = str(body)
        for forbidden in (
            "worker_id",
            "fencing_token",
            "authorization",
            "signed_url",
        ):
            self.assertNotIn(forbidden, serialized.lower())

    async def test_sid_counts_do_not_change_legacy_active_feeds(self) -> None:
        now = time.monotonic()
        self.state.startup_time = now - 400.0
        self.state.last_heartbeat_tick = now - 2.0
        self.state.snapshot_provider = lambda: _snapshot(
            feed_active=2,
            sid_active=9,
        )

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["active_feeds"], 2)
        self.assertEqual(body["grant_counts"]["sid"]["active"], 9)
        self.assertEqual(
            body["grant_counts"]["sid"]["authority_kind"],
            "sid_lease",
        )


if __name__ == "__main__":
    unittest.main()
