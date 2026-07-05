from __future__ import annotations

import asyncio
import os
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
            "VM_HEALTH_HYSTERESIS_SEC": "30.0",
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
        self.assertEqual(settings.hysteresis_sec, 30.0)

    def test_rejects_invalid_numeric_settings(self) -> None:
        cases = (
            ("VM_HEALTH_PROBE_TIMEOUT_SEC", "0"),
            ("VM_HEALTH_PROBE_TIMEOUT_SEC", "inf"),
            ("VM_HEALTH_PROBE_INTERVAL_SEC", "-1"),
            ("VM_HEALTH_PROBE_INTERVAL_SEC", "nan"),
            ("VM_HEALTH_LISTEN_PORT", "0"),
            ("VM_HEALTH_LISTEN_PORT", "65536"),
            ("VM_HEALTH_HYSTERESIS_SEC", "0"),
            ("VM_HEALTH_HYSTERESIS_SEC", "-10"),
        )
        for name, value in cases:
            with self.subTest(name=name, value=value):
                with mock.patch.dict(os.environ, {name: value}, clear=True):
                    with self.assertRaises(ValueError):
                        vm_health_agent.VMHealthSettings()

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
        class BrokenSession:
            def get(self, *args: object, **kwargs: object) -> object:
                raise RuntimeError

        result = await vm_health_agent.probe_worker(
            BrokenSession(),  # type: ignore[arg-type]
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
            now=100.0,
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
        self.assertEqual(body["all_workers_unhealthy_for_sec"], 0.0)
        self.assertEqual(body["hysteresis_sec"], 600.0)
        self.assertEqual(
            set(body.keys()),
            {
                "status",
                "workers",
                "all_workers_unhealthy_for_sec",
                "hysteresis_sec",
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


if __name__ == "__main__":
    unittest.main()
