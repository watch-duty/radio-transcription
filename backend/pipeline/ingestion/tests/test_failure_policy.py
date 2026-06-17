from __future__ import annotations

import inspect
import unittest

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.storage import feed_store


class TestFailurePolicySurface(unittest.TestCase):
    """Tests for the intentionally small failure policy API."""

    def test_classify_failure_policy_accepts_only_status_reason(self) -> None:
        signature = inspect.signature(failure_policy.classify_failure_policy)

        self.assertEqual(tuple(signature.parameters), ("status_reason",))


class TestClassifyFailurePolicy(unittest.TestCase):
    """Tests for pure status-to-action classification."""

    def test_current_status_reasons_have_explicit_policy_routes(
        self,
    ) -> None:
        """Every current status reason has an intended policy route."""
        cases = (
            (
                "pipeline_publish_after_bookmark_failed",
                failure_policy.ExecutedAction.RECORD_POST_BOOKMARK_PUBLISH_GAP,
            ),
            (
                "source_offline",
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                "source_unreachable",
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                "source_rate_limited",
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                "system_authentication_failed",
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                "system_configuration_invalid",
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
            ),
            (
                "system_source_configuration_invalid",
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                "system_runtime_configuration_invalid",
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
            ),
            (
                "system_credential_access_failed",
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                "system_source_payload_invalid",
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                "system_collector_error",
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                "system_pipeline_error",
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                "system_unexpected_error",
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
        )

        self.assertEqual(
            {case[0] for case in cases},
            {reason.value for reason in feed_store.FeedStatusReason},
        )
        for status_reason_value, executed_action in cases:
            with self.subTest(status_reason=status_reason_value):
                self.assertIs(
                    failure_policy.classify_failure_policy(
                        feed_store.FeedStatusReason(status_reason_value),
                    ),
                    executed_action,
                )


if __name__ == "__main__":
    unittest.main()
