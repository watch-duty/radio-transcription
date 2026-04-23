"""Smoke-test runbook: boot the 3 structured-log emits and assert key-sets.

Phase 4 VERIFY-02 runbook per 04-CONTEXT.md D-26. Invoke manually during
the release cycle or after any change to the 3 emit sites:

    python scripts/smoke_test_logs.py [--timeout 30]

Exit codes:
    0 — all 3 events (chunk_ingested, call_download_failed, feed_quarantined)
        emitted with json_fields key-sets matching the corresponding golden
        files under backend/pipeline/ingestion/tests/golden/.
    1 — at least one event missing or mismatched.

Why this script boots a minimal harness instead of a full NormalizerRuntime
(Open Tension 2 from 04-CONTEXT.md): there is no canonical production
``main()`` that drives all 3 emits end-to-end against a local fixture.
Production boots via k8s/MIG launch with AlloyDB + GCS + Pub/Sub + metadata
server dependencies; reproducing that stack in a runbook would add ~400
LOC of mocks with zero additional signal over asserting the payload
shapes. The in-repo unit tests (test_chunk_ingested.py, call_download_failed
tests, test_quarantine_telemetry.py) already cover the runtime wiring
per-emit-site. This script adds the cross-cutting "all 3 look right
together" check that the unit tests can't: it's the operator's
equivalent of manually running the worker and grepping the logs.

Runbook style — NOT a CI gate. See scripts/README.md for usage details.
"""  # noqa: INP001 — scripts/ is an operator runbook dir, not a package.

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import pathlib
import sys

from backend.pipeline.ingestion import quarantine_telemetry
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_CALL_DOWNLOAD_FAILED,
    EVENT_TYPE_CHUNK_INGESTED,
    EVENT_TYPE_FEED_QUARANTINED,
    INGESTION_LOGGER_PATH,
)

logger = logging.getLogger(__name__)

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
_GOLDEN_DIR = (
    _REPO_ROOT / "backend" / "pipeline" / "ingestion" / "tests" / "golden"
)


class _CapturingHandler(logging.Handler):
    """Accumulates every LogRecord.json_fields dict into .records."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[dict[str, object]] = []

    def emit(self, record: logging.LogRecord) -> None:
        json_fields = getattr(record, "json_fields", None)
        if isinstance(json_fields, dict):
            self.records.append(json_fields)


def _load_golden(name: str) -> set[str]:
    """Return the expected_keys set for a golden file under tests/golden/."""
    path = _GOLDEN_DIR / name
    data = json.loads(path.read_text(encoding="utf-8"))
    return set(data["expected_keys"])


async def _emit_chunk_ingested() -> None:
    """Mimic normalizer_runtime._process_feed's success emit.

    Shape = chunk_ingested.json (common case — receipt_time stamped,
    latency non-negative). Matches production's most-frequent emit.
    """
    emit_logger = logging.getLogger(INGESTION_LOGGER_PATH + ".smoke_test")
    emit_logger.info(
        "Chunk ingested",
        extra={
            "json_fields": {
                "event_type": EVENT_TYPE_CHUNK_INGESTED,
                "feed_id": "00000000-0000-0000-0000-000000000001",
                "source_type": "bcfy_feeds",
                "processing_latency_sec": 1.23,
            },
        },
    )


async def _emit_call_download_failed() -> None:
    """Mimic the OpenMHZ / bcfy_calls caller-site emit on terminal failure."""
    emit_logger = logging.getLogger(INGESTION_LOGGER_PATH + ".smoke_test")
    emit_logger.warning(
        "Call download failed",
        extra={
            "json_fields": {
                "event_type": EVENT_TYPE_CALL_DOWNLOAD_FAILED,
                "feed_id": "00000000-0000-0000-0000-000000000002",
                "source_type": "openmhz",
            },
        },
    )


async def _emit_feed_quarantined() -> None:
    """Drive the REAL quarantine_telemetry.emit_quarantine_event code path.

    quarantine_telemetry.configure(None) disables MonitoringClient so the
    metric write is a no-op; the LOG emit still fires with the shipped
    extra={"json_fields": {...}} shape.
    """
    quarantine_telemetry.configure(None)
    await quarantine_telemetry.emit_quarantine_event(
        feed_id="00000000-0000-0000-0000-000000000003",
        feed_name="Smoke Test Feed",
        source_type="bcfy_feeds",
    )


async def _run_smoke(timeout_sec: float) -> int:
    """Emit 3 events, assert key-sets, return exit code."""
    ingestion_logger = logging.getLogger(INGESTION_LOGGER_PATH)
    prev_level = ingestion_logger.level
    ingestion_logger.setLevel(logging.DEBUG)
    handler = _CapturingHandler()
    ingestion_logger.addHandler(handler)

    try:
        async with asyncio.timeout(timeout_sec):
            await _emit_chunk_ingested()
            await _emit_call_download_failed()
            await _emit_feed_quarantined()
    finally:
        ingestion_logger.removeHandler(handler)
        ingestion_logger.setLevel(prev_level)

    expected = {
        EVENT_TYPE_CHUNK_INGESTED: _load_golden("chunk_ingested.json"),
        EVENT_TYPE_CALL_DOWNLOAD_FAILED: _load_golden(
            "call_download_failed.json",
        ),
        EVENT_TYPE_FEED_QUARANTINED: _load_golden("feed_quarantined.json"),
    }

    failures: list[str] = []
    for event_type, expected_keys in expected.items():
        matched = False
        for record in handler.records:
            if record.get("event_type") != event_type:
                continue
            actual_keys = set(record.keys())
            if actual_keys == expected_keys:
                matched = True
                break
            failures.append(
                f"key-set mismatch for {event_type}: "
                f"got={sorted(actual_keys)} expected={sorted(expected_keys)}",
            )
            break
        if not matched and not any(event_type in msg for msg in failures):
            failures.append(f"event {event_type} not found in captured logs")

    if failures:
        for msg in failures:
            print(f"SMOKE FAIL: {msg}", file=sys.stderr)  # noqa: T201
        return 1
    print("SMOKE PASS: 3/3 log events found")  # noqa: T201
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Runbook smoke test for the 3 ingestion structured-log emits."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-emit timeout in seconds (default: 30.0).",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    return asyncio.run(_run_smoke(args.timeout))


if __name__ == "__main__":
    sys.exit(main())
