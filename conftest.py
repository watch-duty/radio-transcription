from __future__ import annotations

import os
import socket
import time
import warnings
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest

LARGE = "large"
REQUIRES_DOCKER = "requires_docker"
REQUIRES_SERVICE_STACK = "requires_service_stack"

SERVICE_STACK_HOSTS = {
    "TRANSCRIPTS_API_HOST": "localhost:8087",
    "FEEDS_API_HOST": "localhost:8089",
    "RULES_API_HOST": "localhost:8086",
    "PUBSUB_EMULATOR_HOST": "localhost:8085",
    "MOCK_SERVER_HOST": "localhost:8082",
}
API_SERVICE_STACK_HOSTS = {
    "TRANSCRIPTS_API_HOST": SERVICE_STACK_HOSTS["TRANSCRIPTS_API_HOST"],
    "FEEDS_API_HOST": SERVICE_STACK_HOSTS["FEEDS_API_HOST"],
}
ALLOYDB_ENV_VARS = (
    "ALLOYDB_HOST",
    "ALLOYDB_PORT",
    "ALLOYDB_USER",
    "ALLOYDB_PASSWORD",
    "ALLOYDB_DB",
)
E2E_SERVICE_STACK_ENV_VARS = (
    *ALLOYDB_ENV_VARS,
    "AUDIO_STAGING_BUCKET",
    "CONTINUOUS_TOPIC",
    "SEGMENTED_TOPIC",
    "STORAGE_EMULATOR_HOST",
)
CONNECT_ATTEMPTS = 3
CONNECT_RETRY_DELAY_SECONDS = 0.25
CONNECT_TIMEOUT_SECONDS = 0.25


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "large: uses expensive local resources or externally managed services",
    )
    config.addinivalue_line(
        "markers",
        "requires_docker: starts Docker/testcontainers containers",
    )
    config.addinivalue_line(
        "markers",
        "requires_service_stack: requires the local Docker Compose service stack",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    selected_markers: set[str] = set()
    service_stack_paths: list[Path] = []
    for item in items:
        item_path = _item_path(item)
        markers = _resource_markers_for_path(item_path, config.rootpath)
        selected_markers.update(markers)
        if REQUIRES_SERVICE_STACK in markers:
            service_stack_paths.append(item_path)
        for marker in sorted(markers):
            item.add_marker(marker)

    if (
        LARGE in selected_markers
        and _xdist_enabled(config)
        and not _running_in_ci()
    ):
        warnings.warn(
            pytest.PytestWarning(
                "Large tests are selected while pytest-xdist is enabled. "
                "This is an explicit local override and may start multiple "
                "Docker/service-stack clients. Use `-n 0` for the safe local "
                "default."
            ),
            stacklevel=2,
        )

    if REQUIRES_DOCKER in selected_markers and not _docker_available():
        message = (
            "Docker is required for selected tests marked requires_docker, "
            "but the Docker daemon is not reachable. Start Docker and rerun, "
            "or target a safe subset such as "
            "`uv run pytest backend/ --ignore-glob='**/test_*_integration.py'`."
        )
        raise pytest.UsageError(message)

    if service_stack_paths:
        missing = _missing_service_stack_requirements(
            service_stack_paths,
            config.rootpath,
        )
        if missing:
            message = (
                "Service-stack tests require the local pipeline services and "
                "their host-side environment variables. "
                f"{' '.join(missing)} "
                "Run `mise run test:e2e` to let Docker Compose manage the "
                "stack, or start the stack yourself and export host-side env "
                "vars before running `mise run test:api` or "
                "`mise run test:e2e:local`."
            )
            raise pytest.UsageError(message)


def _item_path(item: pytest.Item) -> Path:
    path: Any = getattr(item, "path", None)
    if path is None:
        path = item.fspath
    return Path(str(path))


def _resource_markers_for_path(path: Path, rootpath: Path) -> set[str]:
    try:
        rel_path = _relative_test_path(path, rootpath)
    except ValueError:
        return set()

    markers: set[str] = set()

    if rel_path.parts and rel_path.parts[0] == "integration_tests":
        markers.add(LARGE)
        if len(rel_path.parts) > 1 and rel_path.parts[1] == "storage":
            markers.add(REQUIRES_DOCKER)
        elif len(rel_path.parts) > 1 and rel_path.parts[1] in {"api", "e2e"}:
            markers.add(REQUIRES_SERVICE_STACK)
    elif (
        rel_path.parts
        and rel_path.parts[0] == "backend"
        and rel_path.name.startswith("test_")
        and rel_path.name.endswith("_integration.py")
    ):
        markers.update({LARGE, REQUIRES_DOCKER})

    return markers


def _relative_test_path(path: Path, rootpath: Path) -> Path:
    if path.is_absolute():
        return path.resolve().relative_to(rootpath.resolve())
    try:
        return path.relative_to(rootpath)
    except ValueError:
        return path


def _xdist_enabled(config: pytest.Config) -> bool:
    numprocesses: Any = getattr(config.option, "numprocesses", None)
    return numprocesses not in (None, 0, "0")


def _running_in_ci() -> bool:
    return bool(os.environ.get("CI"))


def _docker_available() -> bool:
    try:
        import docker  # noqa: PLC0415

        client = docker.from_env()
        try:
            client.ping()
        finally:
            client.close()
    except Exception:
        return False
    return True


def _missing_service_stack_requirements(
    paths: list[Path],
    rootpath: Path,
) -> list[str]:
    env_vars, host_vars = _service_stack_requirements_for_paths(
        paths,
        rootpath,
    )
    missing_env_vars = [
        env_var for env_var in sorted(env_vars) if not os.environ.get(env_var)
    ]
    unreachable_hosts = [
        f"{env_var}={os.environ.get(env_var, default)}"
        for env_var, default in sorted(host_vars.items())
        if not _host_reachable(os.environ.get(env_var, default))
    ]

    messages: list[str] = []
    if missing_env_vars:
        messages.append(f"Missing env vars: {', '.join(missing_env_vars)}.")
    if unreachable_hosts:
        messages.append(
            f"Unreachable service hosts: {', '.join(unreachable_hosts)}."
        )
    return messages


def _service_stack_requirements_for_paths(
    paths: list[Path],
    rootpath: Path,
) -> tuple[set[str], dict[str, str]]:
    env_vars: set[str] = set()
    host_vars: dict[str, str] = {}

    for path in paths:
        try:
            rel_path = _relative_test_path(path, rootpath)
        except ValueError:
            continue
        if len(rel_path.parts) < 2 or rel_path.parts[0] != "integration_tests":
            continue
        if rel_path.parts[1] == "api":
            env_vars.update(ALLOYDB_ENV_VARS)
            host_vars.update(API_SERVICE_STACK_HOSTS)
        elif rel_path.parts[1] == "e2e":
            env_vars.update(E2E_SERVICE_STACK_ENV_VARS)
            host_vars.update(SERVICE_STACK_HOSTS)

    return env_vars, host_vars


def _host_reachable(raw_host: str) -> bool:
    host, port = _parse_host_port(raw_host)
    for attempt in range(CONNECT_ATTEMPTS):
        try:
            with socket.create_connection(
                (host, port),
                timeout=CONNECT_TIMEOUT_SECONDS,
            ):
                return True
        except OSError:
            if attempt < CONNECT_ATTEMPTS - 1:
                time.sleep(CONNECT_RETRY_DELAY_SECONDS)
    return False


def _parse_host_port(raw_host: str) -> tuple[str, int]:
    parsed = urlparse(raw_host if "://" in raw_host else f"//{raw_host}")
    if not parsed.hostname or parsed.port is None:
        msg = f"Expected host:port value, got {raw_host!r}"
        raise ValueError(msg)
    return parsed.hostname, parsed.port
