from __future__ import annotations

from pathlib import Path

import conftest


def test_storage_tests_require_docker() -> None:
    markers = conftest._resource_markers_for_path(
        Path("integration_tests/storage/test_feed_store_integration.py"),
        Path(),
    )

    assert markers == {"large", "requires_docker"}


def test_service_stack_tests_require_running_stack() -> None:
    markers = conftest._resource_markers_for_path(
        Path("integration_tests/e2e/test_notifications.py"),
        Path(),
    )

    assert markers == {"large", "requires_service_stack"}


def test_backend_integration_tests_require_docker() -> None:
    markers = conftest._resource_markers_for_path(
        Path(
            "backend/pipeline/ingestion/collectors/tests/"
            "test_bcfy_calls_collector_integration.py"
        ),
        Path(),
    )

    assert markers == {"large", "requires_docker"}


def test_backend_unit_tests_are_not_large() -> None:
    markers = conftest._resource_markers_for_path(
        Path("backend/pipeline/ingestion/collectors/tests/test_main.py"),
        Path(),
    )

    assert markers == set()


def test_host_port_parser_accepts_urls_and_plain_hostports() -> None:
    assert conftest._parse_host_port("http://localhost:8087") == (
        "localhost",
        8087,
    )
    assert conftest._parse_host_port("https://staging-api.watchduty.org") == (
        "staging-api.watchduty.org",
        443,
    )
    assert conftest._parse_host_port("http://staging-api.watchduty.org") == (
        "staging-api.watchduty.org",
        80,
    )
    assert conftest._parse_host_port("pubsub-emulator:8085") == (
        "pubsub-emulator",
        8085,
    )


def test_host_reachable_treats_invalid_host_as_unreachable() -> None:
    assert not conftest._host_reachable("localhost")


def test_api_service_stack_requirements_do_not_include_e2e_only_env() -> None:
    env_vars, host_vars = conftest._service_stack_requirements_for_paths(
        [Path("integration_tests/api/test_transcripts_api.py")],
        Path(),
    )

    assert env_vars == {
        "ALLOYDB_HOST",
        "ALLOYDB_PORT",
        "ALLOYDB_USER",
        "ALLOYDB_PASSWORD",
        "ALLOYDB_DB",
    }
    assert host_vars == {
        "TRANSCRIPTS_API_HOST": "localhost:8087",
        "FEEDS_API_HOST": "localhost:8089",
    }
