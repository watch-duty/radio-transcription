"""Tests for shared collector failure classification helpers."""

from __future__ import annotations

import ast
import pathlib
import unittest

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.ingestion.collectors.failure_classification import (
    FailureInfo,
    ItemBatchOutcome,
    ItemFailure,
    collector_failure,
    missing_source_feed_id_failure,
    owner_scope_for_status_reason,
    policy_evidence_for_status_reason,
)
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage.feed_store import FeedStatusReason

_COLLECTOR_ROOT = pathlib.Path(__file__).resolve().parents[1]
_COLLECTOR_FAILURE_CALLSITE_FILES = (
    _COLLECTOR_ROOT / "failure_classification.py",
    _COLLECTOR_ROOT / "bcfy_calls" / "bcfy_calls_collector.py",
    _COLLECTOR_ROOT / "openmhz" / "collector.py",
    _COLLECTOR_ROOT / "icecast" / "icecast_collector.py",
    _COLLECTOR_ROOT / "fire_notifications" / "collector.py",
)


def _require_item_failure(value: ItemFailure | None) -> ItemFailure:
    """Return a typed item failure for tests that intentionally expect one."""
    if value is None:
        msg = "Expected ItemFailure, got None"
        raise AssertionError(msg)
    return value


def _feed_config_evidence() -> failure_policy.FailurePolicyEvidence:
    return failure_policy.FailurePolicyEvidence(
        owner_scope=failure_policy.OwnerScope.FEED,
        failure_scope=failure_policy.FailureScope.FEED,
        endpoint_kind=failure_policy.EndpointKind.FEED_CONFIGURATION,
    )


class TestItemBatchOutcome(unittest.TestCase):
    """Shared item-failure promotion rules."""

    def test_failure_info_preserves_fields(self) -> None:
        info = FailureInfo(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )

        self.assertIs(
            info.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(info.reason, "download_failed")

    def test_item_failure_preserves_status_reason_and_reason(self) -> None:
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )

        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "download_failed")

    def test_no_attempted_items_returns_none(self) -> None:
        outcome = ItemBatchOutcome()

        self.assertIsNone(outcome.promoted_failure())

    def test_any_success_returns_none(self) -> None:
        outcome = ItemBatchOutcome()
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )
        outcome.record_attempt()
        outcome.record_failure(failure)
        outcome.record_chunk_produced()

        self.assertIsNone(outcome.promoted_failure())

    def test_missing_classified_failure_returns_none(self) -> None:
        outcome = ItemBatchOutcome()
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )
        outcome.record_attempt()
        outcome.record_failure(failure)
        outcome.record_attempt()

        self.assertIsNone(outcome.promoted_failure())

    def test_same_reason_all_failed_returns_that_failure(self) -> None:
        outcome = ItemBatchOutcome()
        failures = [
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "download_failed",
            ),
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "download_failed",
            ),
        ]
        for failure in failures:
            outcome.record_attempt()
            outcome.record_failure(failure)

        result = outcome.promoted_failure()

        result = _require_item_failure(result)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(result.reason, "download_failed")

    def test_mixed_reason_all_failed_returns_collector_error(self) -> None:
        outcome = ItemBatchOutcome()
        failures = [
            ItemFailure(
                FeedStatusReason.SOURCE_UNREACHABLE,
                "download_failed",
            ),
            ItemFailure(
                FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                "bad_url",
            ),
        ]
        for failure in failures:
            outcome.record_attempt()
            outcome.record_failure(failure)

        result = outcome.promoted_failure()

        result = _require_item_failure(result)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(result.reason, "mixed_item_failures")

    def test_collector_failure_helper_returns_typed_exception(self) -> None:
        evidence = _feed_config_evidence()

        result = collector_failure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "source_unreachable",
            policy_evidence=evidence,
        )

        self.assertIsInstance(result, FeedFailure)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(result), "source_unreachable")
        self.assertIs(result.policy_evidence, evidence)

    def test_collector_failure_helper_requires_policy_evidence(self) -> None:
        with self.assertRaises(TypeError):
            collector_failure(  # type: ignore[call-arg]
                FeedStatusReason.SOURCE_UNREACHABLE,
                "source_unreachable",
            )

    def test_missing_source_feed_id_failure(self) -> None:
        result = missing_source_feed_id_failure()

        self.assertIsInstance(result, FeedFailure)
        self.assertIs(
            result.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(result), "missing_source_feed_id")
        self.assertEqual(
            result.policy_evidence,
            failure_policy.FailurePolicyEvidence(
                owner_scope=failure_policy.OwnerScope.FEED,
                failure_scope=failure_policy.FailureScope.FEED,
                endpoint_kind=failure_policy.EndpointKind.FEED_CONFIGURATION,
            ),
        )

    def test_owner_scope_for_status_reason_uses_status_enum(self) -> None:
        self.assertIs(
            owner_scope_for_status_reason(FeedStatusReason.SOURCE_UNREACHABLE),
            failure_policy.OwnerScope.SOURCE_CLASS,
        )
        self.assertIs(
            owner_scope_for_status_reason(
                FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED
            ),
            failure_policy.OwnerScope.CREDENTIAL_SCOPE,
        )
        self.assertIs(
            owner_scope_for_status_reason(
                FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
            ),
            failure_policy.OwnerScope.FEED,
        )
        self.assertIs(
            owner_scope_for_status_reason(
                FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID
            ),
            failure_policy.OwnerScope.SOURCE_CLASS,
        )
        self.assertIs(
            owner_scope_for_status_reason(
                FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED
            ),
            failure_policy.OwnerScope.CREDENTIAL_SCOPE,
        )
        self.assertIs(
            owner_scope_for_status_reason(
                FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID
            ),
            failure_policy.OwnerScope.SOURCE_CLASS,
        )
        self.assertIs(
            owner_scope_for_status_reason(
                FeedStatusReason.SYSTEM_COLLECTOR_ERROR
            ),
            failure_policy.OwnerScope.UNKNOWN,
        )

    def test_policy_evidence_for_status_reason_preserves_endpoint_facts(
        self,
    ) -> None:
        result = policy_evidence_for_status_reason(
            FeedStatusReason.SOURCE_UNREACHABLE,
            failure_scope=failure_policy.FailureScope.ITEM,
            endpoint_kind=failure_policy.EndpointKind.CALLS_MEDIA,
        )

        self.assertEqual(
            result,
            failure_policy.FailurePolicyEvidence(
                owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
                failure_scope=failure_policy.FailureScope.ITEM,
                endpoint_kind=failure_policy.EndpointKind.CALLS_MEDIA,
            ),
        )


class TestCollectorFailureCallSites(unittest.TestCase):
    """All current collector_failure calls must carry policy evidence."""

    def test_current_collector_failure_calls_supply_policy_evidence(
        self,
    ) -> None:
        missing: list[str] = []
        for path in _COLLECTOR_FAILURE_CALLSITE_FILES:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if not _is_collector_failure_call(node):
                    continue
                if any(
                    keyword.arg == "policy_evidence"
                    for keyword in node.keywords
                ):
                    continue
                missing.append(
                    f"{path.relative_to(_COLLECTOR_ROOT)}:{node.lineno}"
                )

        self.assertEqual(missing, [])


def _is_collector_failure_call(node: ast.Call) -> bool:
    if isinstance(node.func, ast.Name):
        return node.func.id == "collector_failure"
    if isinstance(node.func, ast.Attribute):
        return node.func.attr == "collector_failure"
    return False


if __name__ == "__main__":
    unittest.main()
