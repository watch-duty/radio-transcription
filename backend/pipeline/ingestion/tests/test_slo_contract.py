"""Tests for backend.pipeline.ingestion.slo_contract.

Pins the literal values and types of every constant in the shared SLI
vocabulary module. These tests are the in-repo guard against the
"two files drifted" failure mode: any downstream code change that alters
an `event_type` string, metric type URL, or logger path WILL fail here first.
"""

from __future__ import annotations

import datetime
import json
import pathlib
import unittest
import uuid

from backend.pipeline.ingestion import slo_contract
from backend.pipeline.ingestion.collectors import telemetry
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    cursor_policy,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    telemetry as calls_telemetry,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

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

    def test_bcfy_calls_sid_poll_literals(self) -> None:
        self.assertEqual(
            slo_contract.EVENT_TYPE_BCFY_CALLS_SID_POLL,
            "bcfy_calls_sid_poll",
        )
        self.assertEqual(
            slo_contract.BCFY_CALLS_SID_POLL_SCHEMA_VERSION,
            1,
        )

    def test_bcfy_calls_replay_window_truncated_literals(self) -> None:
        self.assertEqual(
            slo_contract.EVENT_TYPE_BCFY_CALLS_REPLAY_WINDOW_TRUNCATED,
            "bcfy_calls_replay_window_truncated",
        )
        self.assertEqual(
            slo_contract.BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_SCHEMA_VERSION,
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
            "EVENT_TYPE_BCFY_CALLS_REPLAY_WINDOW_TRUNCATED",
            "BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_SCHEMA_VERSION",
            "BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_REQUIRED_FIELDS",
            "BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_STRING_MAX_LENGTH",
            "BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_SECONDS_MAX",
            "EVENT_TYPE_BCFY_CALLS_SID_POLL",
            "BCFY_CALLS_SID_POLL_SCHEMA_VERSION",
            "BCFY_CALLS_SID_POLL_REQUIRED_FIELDS",
            "BCFY_CALLS_SID_POLL_OPTIONAL_FIELDS",
            "BCFY_CALLS_SID_POLL_STRING_MAX_LENGTH",
            "BCFY_CALLS_SID_POLL_COUNT_MAX",
            "BCFY_CALLS_SID_POLL_SECONDS_MAX",
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


class TestBcfyCallsSidPollGolden(unittest.TestCase):
    """Pin the exact required schema-v1 SID poll payload contract."""

    def test_golden_matches_required_fields_and_closed_defaults(self) -> None:
        golden = json.loads(
            (_GOLDEN_DIR / "bcfy_calls_sid_poll.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertEqual(
            golden["event"],
            slo_contract.EVENT_TYPE_BCFY_CALLS_SID_POLL,
        )
        self.assertEqual(
            golden["schema_version"],
            slo_contract.BCFY_CALLS_SID_POLL_SCHEMA_VERSION,
        )
        self.assertEqual(
            golden["expected_keys"],
            sorted(slo_contract.BCFY_CALLS_SID_POLL_REQUIRED_FIELDS),
        )
        self.assertEqual(
            golden["optional_keys"],
            list(slo_contract.BCFY_CALLS_SID_POLL_OPTIONAL_FIELDS),
        )
        self.assertEqual(
            golden["unobserved_provider"],
            {
                "http_attempt_count": 0,
                "provider_observed": False,
                "response_byte_count": None,
                "response_last_pos": None,
                "response_last_pos_state": "not_observed",
                "response_row_count": None,
            },
        )
        self.assertEqual(
            golden["no_scheduler_or_finalizer"],
            {
                "admitted_cohort_count": 0,
                "admitted_record_count": 0,
                "failed_feed_count": 0,
                "item_to_feed_promotion_count": 0,
                "oldest_queue_age_seconds": 0.0,
                "participating_feed_count": 0,
                "successful_feed_count": 0,
            },
        )


class TestBcfyCallsReplayWindowTruncatedGolden(unittest.TestCase):
    """Pin the exact separate replay-window event and CRITICAL severity."""

    def test_replay_window_truncated_golden_matches_helper_payload(
        self,
    ) -> None:
        golden = json.loads(
            (_GOLDEN_DIR / "bcfy_calls_replay_window_truncated.json").read_text(
                encoding="utf-8"
            )
        )
        now = datetime.datetime(
            2026,
            7,
            13,
            12,
            0,
            tzinfo=datetime.UTC,
        )
        decision = cursor_policy.apply_replay_floor(
            now - datetime.timedelta(minutes=5),
            now=now,
            cause=cursor_policy.ReplayFloorCause.RECOVERY,
        )
        assert decision.floor_reached is not None
        grant = ingestion_lease_store.LeaseGrant(
            feed_store.SourceType.BCFY_CALLS,
            "123",
            uuid.UUID("11111111-2222-3333-4444-555555555555"),
            7,
        )
        payload = calls_telemetry.replay_window_truncated_json_fields(
            calls_telemetry.ReplayWindowTruncatedEvent(
                grant,
                decision.floor_reached,
            )
        )

        self.assertEqual(
            golden["event"],
            slo_contract.EVENT_TYPE_BCFY_CALLS_REPLAY_WINDOW_TRUNCATED,
        )
        self.assertEqual(golden["log_severity"], "CRITICAL")
        self.assertEqual(
            golden["schema_version"],
            slo_contract.BCFY_CALLS_REPLAY_WINDOW_TRUNCATED_SCHEMA_VERSION,
        )
        self.assertEqual(set(payload), set(golden["expected_keys"]))
        self.assertEqual(payload["lost_duration_seconds"], 0.0)
        self.assertEqual(payload["cause"], "recovery")
        self.assertTrue(
            set(payload).isdisjoint(
                {
                    "audio_url",
                    "calls",
                    "feed_ids",
                    "outcome",
                    "response_row_count",
                }
            )
        )


if __name__ == "__main__":
    unittest.main()
