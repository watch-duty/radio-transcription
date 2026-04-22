"""Tests for backend.pipeline.ingestion.slo_contract.

Pins the literal values and types of every constant in the shared SLI
vocabulary module. These tests are the in-repo guard against the
"two files drifted" failure mode described in PITFALLS.md (Pitfall 2 /
Pitfall 10): any downstream code change that alters an `event_type` string,
metric type URL, resource type, label allowlist, or logger path WILL fail
here first.

Test strategy:
    Test 1 — literal values (drift canary for every constant)
    Test 2 — METRIC_LABEL_ALLOWLIST is a frozenset with the expected members
    Test 3 — __all__ lists exactly the 8 public constants
    Test 4 — frozenset immutability (no .add method)
    Test 5 — EVENT_TYPE_FEED_QUARANTINED matches the already-shipped
             quarantine log literal (see quarantine_telemetry.py line 52
             before migration in the same plan).
"""

from __future__ import annotations

import unittest

from backend.pipeline.ingestion import slo_contract


class TestSloContractLiterals(unittest.TestCase):
    """Pin the exact string values of every SLI-vocabulary constant."""

    def test_event_type_chunk_ingested_literal(self) -> None:
        self.assertEqual(slo_contract.EVENT_TYPE_CHUNK_INGESTED, "chunk_ingested")

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

    def test_metric_type_active_feed_count_literal(self) -> None:
        self.assertEqual(
            slo_contract.METRIC_TYPE_ACTIVE_FEED_COUNT,
            "custom.googleapis.com/ingestion/active_feed_count",
        )

    def test_metric_type_quarantine_events_literal(self) -> None:
        self.assertEqual(
            slo_contract.METRIC_TYPE_QUARANTINE_EVENTS,
            "custom.googleapis.com/feeds/quarantine_events",
        )

    def test_monitored_resource_type_literal(self) -> None:
        self.assertEqual(slo_contract.MONITORED_RESOURCE_TYPE, "gce_instance")

    def test_ingestion_logger_path_literal(self) -> None:
        self.assertEqual(
            slo_contract.INGESTION_LOGGER_PATH,
            "backend.pipeline.ingestion",
        )


class TestMetricLabelAllowlist(unittest.TestCase):
    """Type + membership checks for the runtime cardinality gate."""

    def test_is_frozenset_instance(self) -> None:
        """Must be a frozenset — downstream Phase 3 reporter relies on
        immutability to prevent accidental label-set mutation."""
        self.assertIsInstance(slo_contract.METRIC_LABEL_ALLOWLIST, frozenset)

    def test_equals_expected_members(self) -> None:
        self.assertEqual(
            slo_contract.METRIC_LABEL_ALLOWLIST,
            frozenset({"instance_id", "zone"}),
        )

    def test_excludes_forbidden_labels(self) -> None:
        """Labels that would blow the cardinality budget must NOT be in
        the allowlist (cardinality constraint in PROJECT.md)."""
        self.assertNotIn("feed_id", slo_contract.METRIC_LABEL_ALLOWLIST)
        self.assertNotIn("source_type", slo_contract.METRIC_LABEL_ALLOWLIST)

    def test_frozenset_has_no_add_method(self) -> None:
        """Immutability invariant: `.add` must raise AttributeError.

        Protects downstream code from accidentally mutating the allowlist
        and expanding the cardinality gate at runtime.
        """
        with self.assertRaises(AttributeError):
            slo_contract.METRIC_LABEL_ALLOWLIST.add("bad")  # type: ignore[attr-defined]


class TestSloContractAll(unittest.TestCase):
    """Verify the module's export contract is explicit and exhaustive."""

    def test_all_exports_match_expected_set(self) -> None:
        expected = {
            "EVENT_TYPE_CHUNK_INGESTED",
            "EVENT_TYPE_CALL_DOWNLOAD_FAILED",
            "EVENT_TYPE_FEED_QUARANTINED",
            "METRIC_TYPE_ACTIVE_FEED_COUNT",
            "METRIC_TYPE_QUARANTINE_EVENTS",
            "MONITORED_RESOURCE_TYPE",
            "METRIC_LABEL_ALLOWLIST",
            "INGESTION_LOGGER_PATH",
        }
        self.assertEqual(set(slo_contract.__all__), expected)


class TestSloContractDriftCanary(unittest.TestCase):
    """Cross-reference constants against shipped code to prevent drift."""

    def test_event_type_feed_quarantined_matches_shipped_quarantine_log(
        self,
    ) -> None:
        """Pin the constant to the shipped literal to prevent drift when
        quarantine_telemetry is migrated in the next task.

        This is the value currently emitted at quarantine_telemetry.py's
        `event_type` log extra before this plan migrates it. Must match
        exactly — any divergence silently breaks the Terraform alert.
        """
        self.assertEqual(slo_contract.EVENT_TYPE_FEED_QUARANTINED, "feed_quarantined")


if __name__ == "__main__":
    unittest.main()
