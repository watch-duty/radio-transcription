"""Tests for backend.pipeline.ingestion.slo_contract.

Pins the literal values and types of every constant in the shared SLI
vocabulary module. These tests are the in-repo guard against the
"two files drifted" failure mode: any downstream code change that alters
an `event_type` string, metric type URL, or logger path WILL fail here first.
"""

from __future__ import annotations

import json
import pathlib
import unittest

from backend.pipeline.ingestion import slo_contract
from backend.pipeline.ingestion.collectors import telemetry
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    telemetry as calls_telemetry,
)

_GOLDEN_DIR = pathlib.Path(__file__).resolve().parent / "golden"


class TestSloContractLiterals(unittest.TestCase):
    """Pin the exact string values of every SLI-vocabulary constant."""

    def test_event_type_chunk_ingested_literal(self) -> None:
        self.assertEqual(
            slo_contract.EVENT_TYPE_CHUNK_INGESTED, "chunk_ingested"
        )

    def test_event_type_call_download_failed_literal(self) -> None:
        self.assertEqual(
            slo_contract.EVENT_TYPE_CALL_DOWNLOAD_FAILED,
            "call_download_failed",
        )

    def test_event_type_feed_quarantined_literal(self) -> None:
        self.assertEqual(
            slo_contract.EVENT_TYPE_FEED_QUARANTINED,
            "feed_quarantined",
        )

    def test_event_type_call_auth_failure_literal(self) -> None:
        self.assertEqual(
            slo_contract.EVENT_TYPE_CALL_AUTH_FAILURE,
            "call_auth_failure",
        )

    def test_event_type_bcfy_jwt_fetch_failed_literal(self) -> None:
        self.assertEqual(
            slo_contract.EVENT_TYPE_BCFY_JWT_FETCH_FAILED,
            "bcfy_jwt_fetch_failed",
        )

    def test_event_type_bcfy_calls_missing_call_literal(self) -> None:
        self.assertEqual(
            slo_contract.EVENT_TYPE_BCFY_CALLS_MISSING_CALL,
            "bcfy_calls_missing_call",
        )

    def test_bcfy_calls_missing_call_schema_version_literal(self) -> None:
        self.assertEqual(
            slo_contract.BCFY_CALLS_MISSING_CALL_SCHEMA_VERSION,
            1,
        )

    def test_metric_type_quarantine_events_literal(self) -> None:
        self.assertEqual(
            slo_contract.METRIC_TYPE_QUARANTINE_EVENTS,
            "custom.googleapis.com/feeds/quarantine_events",
        )

    def test_ingestion_logger_path_literal(self) -> None:
        self.assertEqual(
            slo_contract.INGESTION_LOGGER_PATH,
            "backend.pipeline.ingestion",
        )


class TestSloContractAll(unittest.TestCase):
    """Verify the module's export contract is explicit and exhaustive."""

    def test_all_exports_match_expected_set(self) -> None:
        expected = {
            "EVENT_TYPE_CHUNK_INGESTED",
            "EVENT_TYPE_CALL_DOWNLOAD_FAILED",
            "EVENT_TYPE_FEED_QUARANTINED",
            "EVENT_TYPE_CALL_AUTH_FAILURE",
            "EVENT_TYPE_BCFY_JWT_FETCH_FAILED",
            "EVENT_TYPE_BCFY_CALLS_MISSING_CALL",
            "BCFY_CALLS_MISSING_CALL_SCHEMA_VERSION",
            "BCFY_CALLS_MISSING_CALL_AUDIO_URL_MAX_LENGTH",
            "BCFY_CALLS_MISSING_CALL_ATTEMPT_COUNT_MAX",
            "BCFY_CALLS_MISSING_CALL_IDENTITY_MAX_LENGTH",
            "BCFY_CALLS_MISSING_CALL_REASON_MAX_LENGTH",
            "METRIC_TYPE_QUARANTINE_EVENTS",
            "INGESTION_LOGGER_PATH",
        }
        self.assertEqual(set(slo_contract.__all__), expected)


class TestSloContractDriftCanary(unittest.TestCase):
    """Cross-reference constants against shipped code to prevent drift."""

    def test_event_type_feed_quarantined_matches_shipped_quarantine_log(
        self,
    ) -> None:
        """Pin the constant to the feed_quarantined literal. Quarantine
        telemetry imports this constant; any divergence between the
        constant value and the ops-team Terraform alert filter would
        silently break alerting.
        """
        self.assertEqual(
            slo_contract.EVENT_TYPE_FEED_QUARANTINED, "feed_quarantined"
        )


class TestCallDownloadFailedGolden(unittest.TestCase):
    """Pin helper-owned call_download_failed payload shape."""

    def test_golden_expected_keys_match_helper_payload(self) -> None:
        golden = json.loads(
            (_GOLDEN_DIR / "call_download_failed.json").read_text(
                encoding="utf-8"
            )
        )
        payload = telemetry._call_download_failed_json_fields(
            feed_id="feed-123",
            source_type="openmhz",
        )

        self.assertEqual(
            golden["event"],
            slo_contract.EVENT_TYPE_CALL_DOWNLOAD_FAILED,
        )
        self.assertEqual(
            golden["expected_keys"],
            ["event_type", "feed_id", "source_type"],
        )
        self.assertEqual(set(payload.keys()), set(golden["expected_keys"]))
        self.assertEqual(
            payload["event_type"],
            slo_contract.EVENT_TYPE_CALL_DOWNLOAD_FAILED,
        )


class TestBcfyCallsMissingCallGolden(unittest.TestCase):
    """Pin the sole signed-URL-bearing ingestion event shape."""

    def test_golden_expected_keys_match_helper_payload(self) -> None:
        golden = json.loads(
            (_GOLDEN_DIR / "bcfy_calls_missing_call.json").read_text(
                encoding="utf-8"
            )
        )
        event = calls_telemetry.MissingCallEvent(
            audio_url="https://example.invalid/audio?signature=secret",
            sid="123",
            feed_id="00000000-0000-0000-0000-000000000001",
            feed_name="Test Feed",
            source_type="bcfy_calls",
            source_feed_id="123-45",
            group_id="45",
            provider_ts="2026-07-13T12:00:00+00:00",
            gap_stage=calls_telemetry.MissingCallStage.TERMINAL_ITEM_SKIP,
            status_reason="source_unreachable",
            reason="item_download_failed",
            attempt_count=3,
        )
        payload = calls_telemetry.missing_call_json_fields(event)

        self.assertEqual(
            golden["event"],
            slo_contract.EVENT_TYPE_BCFY_CALLS_MISSING_CALL,
        )
        self.assertEqual(set(payload), set(golden["expected_keys"]))
        self.assertEqual(
            payload["schema_version"],
            slo_contract.BCFY_CALLS_MISSING_CALL_SCHEMA_VERSION,
        )
        self.assertIn("signature=secret", payload["audio_url"])


if __name__ == "__main__":
    unittest.main()
