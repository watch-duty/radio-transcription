from __future__ import annotations

import asyncio
import os
import signal
import time
import unittest
from unittest import mock

import aiohttp
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase

from backend.pipeline.ingestion import vm_health_agent


def _probe_result(
    url: str,
    *,
    healthy: bool,
    status_code: int | None,
    error: str | None = None,
) -> vm_health_agent.WorkerProbeResult:
    return vm_health_agent.WorkerProbeResult(
        url=url,
        healthy=healthy,
        status_code=status_code,
        error=error,
    )


class VMHealthSettingsTests(unittest.TestCase):
    def test_defaults_do_not_construct_collector_settings(self) -> None:
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch(
                "backend.pipeline.ingestion.settings.CollectorSettings",
                side_effect=AssertionError(
                    "VM Health must not construct CollectorSettings",
                ),
            ),
        ):
            settings = vm_health_agent.VMHealthSettings()

        self.assertEqual(
            settings.worker_endpoints,
            (
                "http://127.0.0.1:8081/healthz",
                "http://127.0.0.1:8082/healthz",
            ),
        )
        self.assertEqual(settings.probe_timeout_sec, 2.0)
        self.assertEqual(settings.probe_interval_sec, 5.0)
        self.assertEqual(settings.listen_host, "0.0.0.0")  # noqa: S104
        self.assertEqual(settings.listen_port, 8080)
        self.assertEqual(settings.hysteresis_sec, 600.0)

    def test_env_overrides_are_vm_health_specific(self) -> None:
        env = {
            "VM_HEALTH_WORKER_ENDPOINTS": (
                "http://127.0.0.1:9001/healthz,"
                "http://localhost:9002/healthz,"
                "http://127.0.0.1:9003/healthz"
            ),
            "VM_HEALTH_PROBE_TIMEOUT_SEC": "1.25",
            "VM_HEALTH_PROBE_INTERVAL_SEC": "7.5",
            "VM_HEALTH_LISTEN_HOST": "127.0.0.1",
            "VM_HEALTH_LISTEN_PORT": "9090",
            "VM_HEALTH_HYSTERESIS_SEC": "120.0",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            settings = vm_health_agent.VMHealthSettings()

        self.assertEqual(
            settings.worker_endpoints,
            (
                "http://127.0.0.1:9001/healthz",
                "http://localhost:9002/healthz",
                "http://127.0.0.1:9003/healthz",
            ),
        )
        self.assertEqual(settings.probe_timeout_sec, 1.25)
        self.assertEqual(settings.probe_interval_sec, 7.5)
        self.assertEqual(settings.listen_host, "127.0.0.1")
        self.assertEqual(settings.listen_port, 9090)
        self.assertEqual(settings.hysteresis_sec, 120.0)

    def test_timing_boundary_values_are_valid(self) -> None:
        cases = (
            {
                "VM_HEALTH_PROBE_TIMEOUT_SEC": "0.1",
                "VM_HEALTH_PROBE_INTERVAL_SEC": "1.0",
                "VM_HEALTH_HYSTERESIS_SEC": "60.0",
            },
            {
                "VM_HEALTH_PROBE_TIMEOUT_SEC": "10.0",
                "VM_HEALTH_PROBE_INTERVAL_SEC": "60.0",
                "VM_HEALTH_HYSTERESIS_SEC": "3600.0",
            },
        )
        for env in cases:
            with self.subTest(env=env):
                with mock.patch.dict(os.environ, env, clear=True):
                    settings = vm_health_agent.VMHealthSettings()

                self.assertEqual(
                    settings.probe_timeout_sec,
                    float(env["VM_HEALTH_PROBE_TIMEOUT_SEC"]),
                )
                self.assertEqual(
                    settings.probe_interval_sec,
                    float(env["VM_HEALTH_PROBE_INTERVAL_SEC"]),
                )
                self.assertEqual(
                    settings.hysteresis_sec,
                    float(env["VM_HEALTH_HYSTERESIS_SEC"]),
                )

    def test_rejects_invalid_numeric_settings(self) -> None:
        cases = (
            ("VM_HEALTH_PROBE_TIMEOUT_SEC", "0"),
            ("VM_HEALTH_PROBE_TIMEOUT_SEC", "0.09"),
            ("VM_HEALTH_PROBE_TIMEOUT_SEC", "10.01"),
            ("VM_HEALTH_PROBE_TIMEOUT_SEC", "inf"),
            ("VM_HEALTH_PROBE_INTERVAL_SEC", "-1"),
            ("VM_HEALTH_PROBE_INTERVAL_SEC", "0.99"),
            ("VM_HEALTH_PROBE_INTERVAL_SEC", "60.01"),
            ("VM_HEALTH_PROBE_INTERVAL_SEC", "nan"),
            ("VM_HEALTH_LISTEN_PORT", "not-an-int"),
            ("VM_HEALTH_LISTEN_PORT", "0"),
            ("VM_HEALTH_LISTEN_PORT", "65536"),
            ("VM_HEALTH_HYSTERESIS_SEC", "0"),
            ("VM_HEALTH_HYSTERESIS_SEC", "-10"),
            ("VM_HEALTH_HYSTERESIS_SEC", "59.99"),
            ("VM_HEALTH_HYSTERESIS_SEC", "3600.01"),
        )
        for name, value in cases:
            with self.subTest(name=name, value=value):
                with mock.patch.dict(os.environ, {name: value}, clear=True):
                    with self.assertRaises(ValueError):
                        vm_health_agent.VMHealthSettings()

    def test_invalid_listen_port_names_env_var_in_error(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"VM_HEALTH_LISTEN_PORT": "not-an-int"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "VM_HEALTH_LISTEN_PORT"):
                vm_health_agent.VMHealthSettings()

    def test_rejects_probe_timeout_at_or_above_probe_interval(self) -> None:
        cases = (
            {
                "VM_HEALTH_PROBE_TIMEOUT_SEC": "5.0",
                "VM_HEALTH_PROBE_INTERVAL_SEC": "5.0",
            },
            {
                "VM_HEALTH_PROBE_TIMEOUT_SEC": "6.0",
                "VM_HEALTH_PROBE_INTERVAL_SEC": "5.0",
            },
        )
        for env in cases:
            with self.subTest(env=env):
                with mock.patch.dict(os.environ, env, clear=True):
                    with self.assertRaises(ValueError):
                        vm_health_agent.VMHealthSettings()

    def test_probe_stale_after_sec_is_three_missed_probe_cycles(self) -> None:
        settings = vm_health_agent.VMHealthSettings(
            probe_interval_sec=7.5,
        )

        self.assertEqual(settings.probe_stale_after_sec, 22.5)

    def test_accepts_one_or_more_distinct_local_worker_endpoints(self) -> None:
        cases = (
            (
                "http://127.0.0.1:8081/healthz",
                ("http://127.0.0.1:8081/healthz",),
            ),
            (
                "http://127.0.0.1:8081/healthz,"
                "http://127.0.0.1:8082/healthz,"
                "http://127.0.0.1:8083/healthz",
                (
                    "http://127.0.0.1:8081/healthz",
                    "http://127.0.0.1:8082/healthz",
                    "http://127.0.0.1:8083/healthz",
                ),
            ),
            (
                " http://127.0.0.1:8081/healthz, ,"
                " http://127.0.0.1:8082/healthz ",
                (
                    "http://127.0.0.1:8081/healthz",
                    "http://127.0.0.1:8082/healthz",
                ),
            ),
        )
        for value, expected in cases:
            with self.subTest(value=value):
                with mock.patch.dict(
                    os.environ,
                    {"VM_HEALTH_WORKER_ENDPOINTS": value},
                    clear=True,
                ):
                    settings = vm_health_agent.VMHealthSettings()

                self.assertEqual(settings.worker_endpoints, expected)

    def test_rejects_invalid_worker_endpoints(self) -> None:
        cases = (
            "",
            ("http://127.0.0.1:8081/healthz,http://127.0.0.1:8081/healthz"),
            ("http://127.0.0.1:8081/healthz,http://localhost:8081/healthz"),
            "http://127.0.0.1:8080/healthz,http://127.0.0.1:8082/healthz",
            "http://127.0.0.1/healthz,http://127.0.0.1:8082/healthz",
            "https://127.0.0.1:8081/healthz",
            "http://example.com:8081/healthz",
            "http://169.254.169.254/latest/meta-data",
        )
        for value in cases:
            with self.subTest(value=value):
                with mock.patch.dict(
                    os.environ,
                    {"VM_HEALTH_WORKER_ENDPOINTS": value},
                    clear=True,
                ):
                    with self.assertRaises(ValueError):
                        vm_health_agent.VMHealthSettings()


class WorkerProbeTests(AioHTTPTestCase):
    async def get_application(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/ok", self._ok)
        app.router.add_get("/redirect", self._redirect)
        app.router.add_get("/missing", self._missing)
        app.router.add_get("/error", self._error)
        app.router.add_get("/slow", self._slow)
        return app

    async def _ok(self, request: web.Request) -> web.Response:
        return web.Response(text="not-json", status=204)

    async def _redirect(self, request: web.Request) -> web.Response:
        return web.Response(status=302)

    async def _missing(self, request: web.Request) -> web.Response:
        return web.Response(status=404)

    async def _error(self, request: web.Request) -> web.Response:
        return web.Response(status=503)

    async def _slow(self, request: web.Request) -> web.Response:
        await asyncio.sleep(0.1)
        return web.Response(status=200)

    async def test_status_code_only_probe_results(self) -> None:
        async with aiohttp.ClientSession() as session:
            ok = await vm_health_agent.probe_worker(
                session,
                str(self.server.make_url("/ok")),
                timeout_sec=1.0,
            )
            redirect = await vm_health_agent.probe_worker(
                session,
                str(self.server.make_url("/redirect")),
                timeout_sec=1.0,
            )
            missing = await vm_health_agent.probe_worker(
                session,
                str(self.server.make_url("/missing")),
                timeout_sec=1.0,
            )
            error = await vm_health_agent.probe_worker(
                session,
                str(self.server.make_url("/error")),
                timeout_sec=1.0,
            )

        self.assertTrue(ok.healthy)
        self.assertEqual(ok.status_code, 204)
        self.assertIsNone(ok.error)
        self.assertFalse(redirect.healthy)
        self.assertEqual(redirect.status_code, 302)
        self.assertFalse(missing.healthy)
        self.assertEqual(missing.status_code, 404)
        self.assertFalse(error.healthy)
        self.assertEqual(error.status_code, 503)

    async def test_timeout_and_client_errors_are_unhealthy(self) -> None:
        async with aiohttp.ClientSession() as session:
            timeout = await vm_health_agent.probe_worker(
                session,
                str(self.server.make_url("/slow")),
                timeout_sec=0.001,
            )
            client_error = await vm_health_agent.probe_worker(
                session,
                "http://127.0.0.1:1/healthz",
                timeout_sec=0.1,
            )

        self.assertFalse(timeout.healthy)
        self.assertIsNone(timeout.status_code)
        self.assertEqual(timeout.error, "TimeoutError")
        self.assertFalse(client_error.healthy)
        self.assertIsNone(client_error.status_code)
        self.assertIsNotNone(client_error.error)

    async def test_unexpected_probe_exception_is_unhealthy_result(self) -> None:
        session = mock.MagicMock(spec=aiohttp.ClientSession)
        session.get.side_effect = RuntimeError
        result = await vm_health_agent.probe_worker(
            session,
            "http://127.0.0.1:8081/healthz",
            timeout_sec=1.0,
        )

        self.assertFalse(result.healthy)
        self.assertIsNone(result.status_code)
        self.assertEqual(result.error, "RuntimeError")


class VMHealthStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.healthy = _probe_result(
            "http://127.0.0.1:8081/healthz",
            healthy=True,
            status_code=200,
        )
        self.unhealthy = _probe_result(
            "http://127.0.0.1:8082/healthz",
            healthy=False,
            status_code=503,
        )

    def test_new_state_has_no_persisted_all_down_timer(self) -> None:
        self.assertIsNone(
            vm_health_agent.VMHealthState().all_workers_unhealthy_since
        )

    def test_one_unhealthy_worker_keeps_vm_healthy_and_resets_timer(
        self,
    ) -> None:
        state = vm_health_agent.VMHealthState()
        decision = state.update(
            (self.unhealthy, self.healthy),
            now=100.0,
            hysteresis_sec=600.0,
        )

        self.assertIsNone(state.all_workers_unhealthy_since)
        self.assertTrue(decision.vm_healthy)
        self.assertEqual(decision.http_status, 200)
        self.assertEqual(decision.all_workers_unhealthy_for_sec, 0.0)

    def test_one_worker_unhealthy_grace_and_600_second_expiry(self) -> None:
        state = vm_health_agent.VMHealthState()
        # Pass startup first
        state.update((self.healthy,), now=50.0, hysteresis_sec=600.0)

        grace_start = state.update(
            (self.unhealthy,),
            now=100.0,
            hysteresis_sec=600.0,
        )
        expired = state.update(
            (self.unhealthy,),
            now=700.0,
            hysteresis_sec=600.0,
        )

        self.assertTrue(grace_start.vm_healthy)
        self.assertEqual(grace_start.http_status, 200)
        self.assertFalse(expired.vm_healthy)
        self.assertEqual(expired.http_status, 503)
        self.assertEqual(expired.all_workers_unhealthy_for_sec, 600.0)

    def test_all_workers_unhealthy_grace_and_600_second_expiry(self) -> None:
        state = vm_health_agent.VMHealthState()
        # Pass startup first
        state.update((self.healthy,), now=50.0, hysteresis_sec=600.0)

        grace_start = state.update(
            (self.unhealthy, self.unhealthy, self.unhealthy),
            now=100.0,
            hysteresis_sec=600.0,
        )
        almost_expired = state.update(
            (self.unhealthy, self.unhealthy, self.unhealthy),
            now=699.9,
            hysteresis_sec=600.0,
        )
        expired = state.update(
            (self.unhealthy, self.unhealthy, self.unhealthy),
            now=700.0,
            hysteresis_sec=600.0,
        )

        self.assertTrue(grace_start.vm_healthy)
        self.assertEqual(grace_start.http_status, 200)
        self.assertTrue(almost_expired.vm_healthy)
        self.assertEqual(almost_expired.http_status, 200)
        self.assertAlmostEqual(
            almost_expired.all_workers_unhealthy_for_sec,
            599.9,
        )
        self.assertFalse(expired.vm_healthy)
        self.assertEqual(expired.http_status, 503)
        self.assertEqual(expired.all_workers_unhealthy_for_sec, 600.0)

    def test_any_recovered_worker_resets_continuous_unhealthy_window(
        self,
    ) -> None:
        state = vm_health_agent.VMHealthState()
        # Pass startup first
        state.update((self.healthy,), now=50.0, hysteresis_sec=600.0)

        state.update(
            (self.unhealthy, self.unhealthy, self.unhealthy),
            now=100.0,
            hysteresis_sec=600.0,
        )
        recovered = state.update(
            (self.unhealthy, self.healthy, self.unhealthy),
            now=699.9,
            hysteresis_sec=600.0,
        )
        restarted = state.update(
            (self.unhealthy, self.unhealthy, self.unhealthy),
            now=700.0,
            hysteresis_sec=600.0,
        )

        self.assertIsNone(recovered.all_workers_unhealthy_since)
        self.assertEqual(recovered.all_workers_unhealthy_for_sec, 0.0)
        self.assertTrue(restarted.vm_healthy)
        self.assertEqual(restarted.all_workers_unhealthy_for_sec, 0.0)
        self.assertEqual(restarted.all_workers_unhealthy_since, 700.0)

    def test_startup_unhealthy_reports_unhealthy(self) -> None:
        state = vm_health_agent.VMHealthState()
        # Workers are unhealthy on startup
        decision = state.update(
            (self.unhealthy, self.unhealthy),
            now=100.0,
            hysteresis_sec=600.0,
        )
        # Should report unhealthy immediately (no grace period)
        self.assertFalse(decision.vm_healthy)
        self.assertEqual(decision.http_status, 503)
        self.assertFalse(state.has_passed_startup)

    def test_startup_becomes_healthy_reports_healthy(self) -> None:
        state = vm_health_agent.VMHealthState()
        # Start unhealthy
        decision1 = state.update(
            (self.unhealthy, self.unhealthy),
            now=100.0,
            hysteresis_sec=600.0,
        )
        self.assertFalse(decision1.vm_healthy)
        self.assertFalse(state.has_passed_startup)

        # One worker becomes healthy
        decision2 = state.update(
            (self.unhealthy, self.healthy),
            now=105.0,
            hysteresis_sec=600.0,
        )
        self.assertTrue(decision2.vm_healthy)
        self.assertTrue(state.has_passed_startup)

    def test_stale_probe_cycle_fails_closed_after_last_healthy_probe(
        self,
    ) -> None:
        state = vm_health_agent.VMHealthState()
        state.update(
            (self.healthy,),
            now=100.0,
            hysteresis_sec=600.0,
        )

        fresh = state.current_decision(
            now=114.9,
            hysteresis_sec=600.0,
            probe_stale_after_sec=15.0,
        )
        stale = state.current_decision(
            now=115.1,
            hysteresis_sec=600.0,
            probe_stale_after_sec=15.0,
        )

        self.assertTrue(fresh.vm_healthy)
        self.assertFalse(fresh.probe_stale)
        self.assertFalse(stale.vm_healthy)
        self.assertEqual(stale.http_status, 503)
        self.assertTrue(stale.probe_stale)
        last_probe_completed_ago_sec = stale.last_probe_completed_ago_sec
        assert last_probe_completed_ago_sec is not None
        self.assertAlmostEqual(last_probe_completed_ago_sec, 15.1)

    def test_missing_initial_probe_reports_unhealthy_before_and_after_grace(
        self,
    ) -> None:
        state = vm_health_agent.VMHealthState(started_at=100.0)

        fresh = state.current_decision(
            now=114.9,
            hysteresis_sec=600.0,
            probe_stale_after_sec=15.0,
        )
        stale = state.current_decision(
            now=115.1,
            hysteresis_sec=600.0,
            probe_stale_after_sec=15.0,
        )

        # No worker has ever been observed healthy, so both report unhealthy
        # regardless of the probe_stale grace window -- only probe_stale
        # itself (used for diagnostics) should flip at the boundary.
        self.assertFalse(fresh.vm_healthy)
        self.assertEqual(fresh.http_status, 503)
        self.assertFalse(fresh.probe_stale)
        self.assertFalse(stale.vm_healthy)
        self.assertEqual(stale.http_status, 503)
        self.assertTrue(stale.probe_stale)
        self.assertIsNone(stale.last_probe_completed_ago_sec)


class VMHealthHandlerTests(AioHTTPTestCase):
    async def get_application(self) -> web.Application:
        self.settings = vm_health_agent.VMHealthSettings(
            probe_interval_sec=60.0,
            hysteresis_sec=600.0,
        )
        self.state = vm_health_agent.VMHealthState()
        self.worker_1 = _probe_result(
            "http://127.0.0.1:8081/healthz",
            healthy=True,
            status_code=200,
        )
        self.worker_2 = _probe_result(
            "http://127.0.0.1:8082/healthz",
            healthy=False,
            status_code=503,
            error="ClientError",
        )
        self.state.update(
            (self.worker_1, self.worker_2),
            now=time.monotonic(),
            hysteresis_sec=self.settings.hysteresis_sec,
        )
        return vm_health_agent.build_app(self.settings, self.state)

    async def _get_healthz(self) -> tuple[int, dict]:
        response = await self.client.request("GET", "/healthz")
        return response.status, await response.json()

    async def test_response_body_exposes_raw_vm_health_state(self) -> None:
        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "healthy")
        self.assertTrue(body["has_passed_startup"])
        self.assertEqual(body["all_workers_unhealthy_for_sec"], 0.0)
        self.assertEqual(body["hysteresis_sec"], 600.0)
        self.assertFalse(body["probe_stale"])
        self.assertEqual(
            body["probe_stale_after_sec"],
            self.settings.probe_stale_after_sec,
        )
        self.assertLess(
            body["last_probe_completed_ago_sec"],
            body["probe_stale_after_sec"],
        )
        self.assertEqual(
            set(body.keys()),
            {
                "status",
                "has_passed_startup",
                "workers",
                "all_workers_unhealthy_for_sec",
                "hysteresis_sec",
                "probe_stale",
                "last_probe_completed_ago_sec",
                "probe_stale_after_sec",
            },
        )
        self.assertEqual(
            set(body["workers"][0].keys()),
            {"url", "healthy", "status_code", "error"},
        )
        self.assertEqual(
            body["workers"][1],
            {
                "url": "http://127.0.0.1:8082/healthz",
                "healthy": False,
                "status_code": 503,
                "error": "ClientError",
            },
        )

    async def test_stale_probe_loop_returns_503(self) -> None:
        self.state.last_probe_completed_at = (
            time.monotonic() - self.settings.probe_stale_after_sec - 1.0
        )

        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "unhealthy")
        self.assertTrue(body["probe_stale"])
        self.assertGreater(
            body["last_probe_completed_ago_sec"],
            self.settings.probe_stale_after_sec,
        )


class VMHealthHandlerStartupRaceTests(AioHTTPTestCase):
    """Regression test for the empty-worker_results startup race.

    Before the first probe cycle completes, VMHealthState.worker_results is
    still its default empty tuple. /healthz must not report the VM healthy
    in that window -- it must wait for a real probe to observe a healthy
    worker.
    """

    async def get_application(self) -> web.Application:
        self.settings = vm_health_agent.VMHealthSettings(
            probe_interval_sec=60.0,
            hysteresis_sec=600.0,
        )
        # No probe has run yet: worker_results is still the default ().
        self.state = vm_health_agent.VMHealthState()
        return vm_health_agent.build_app(self.settings, self.state)

    async def _get_healthz(self) -> tuple[int, dict]:
        response = await self.client.request("GET", "/healthz")
        return response.status, await response.json()

    async def test_unhealthy_until_first_probe_then_healthy(self) -> None:
        status, body = await self._get_healthz()

        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "unhealthy")
        self.assertFalse(body["has_passed_startup"])

        self.state.update(
            (
                _probe_result(
                    "http://127.0.0.1:8081/healthz",
                    healthy=True,
                    status_code=200,
                ),
            ),
            now=time.monotonic(),
            hysteresis_sec=self.settings.hysteresis_sec,
        )

        status, body = await self._get_healthz()

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "healthy")
        self.assertTrue(body["has_passed_startup"])


class VMHealthServeForeverTests(unittest.IsolatedAsyncioTestCase):
    async def test_serve_forever_cleans_up_after_shutdown_signal(self) -> None:
        settings = vm_health_agent.VMHealthSettings(
            worker_endpoints=("http://127.0.0.1:8081/healthz",),
        )
        runner = mock.AsyncMock()
        shutdown_event: asyncio.Event | None = None
        remove_handlers = mock.Mock()

        def install_handlers(shutdown: asyncio.Event) -> mock.Mock:
            nonlocal shutdown_event
            shutdown_event = shutdown
            return remove_handlers

        async def fake_start(
            _settings: vm_health_agent.VMHealthSettings,
            _state: vm_health_agent.VMHealthState,
        ) -> mock.AsyncMock:
            return runner

        with (
            mock.patch.object(vm_health_agent, "start", side_effect=fake_start),
            mock.patch.object(
                vm_health_agent,
                "_install_shutdown_signal_handlers",
                side_effect=install_handlers,
            ),
        ):
            task = asyncio.create_task(vm_health_agent._serve_forever(settings))
            await asyncio.sleep(0)
            assert shutdown_event is not None
            shutdown_event.set()
            await task

        runner.cleanup.assert_awaited_once_with()
        remove_handlers.assert_called_once_with()

    async def test_shutdown_signal_handlers_register_term_and_int(
        self,
    ) -> None:
        loop = asyncio.get_running_loop()
        shutdown = asyncio.Event()

        with (
            mock.patch.object(loop, "add_signal_handler") as add_handler,
            mock.patch.object(loop, "remove_signal_handler") as remove_handler,
        ):
            remove_handlers = vm_health_agent._install_shutdown_signal_handlers(
                shutdown
            )
            registered = {
                call.args[0]: call.args[1]
                for call in add_handler.call_args_list
            }
            registered[signal.SIGTERM]()
            remove_handlers()

        self.assertTrue(shutdown.is_set())
        self.assertEqual(
            set(registered),
            {signal.SIGTERM, signal.SIGINT},
        )
        self.assertEqual(
            [call.args[0] for call in remove_handler.call_args_list],
            [signal.SIGTERM, signal.SIGINT],
        )


if __name__ == "__main__":
    unittest.main()
