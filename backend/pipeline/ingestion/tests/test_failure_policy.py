from __future__ import annotations

import dataclasses
import datetime
import inspect
import unittest
from unittest import mock

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.storage import feed_store

_NOW = datetime.datetime(2026, 7, 16, 12, 0, tzinfo=datetime.UTC)
_BUDGETED = failure_policy.ConsumeFailureBudget(
    failure_threshold=5,
    backoff_base_sec=15,
    backoff_max_sec=600,
)


class TestFailurePolicySurface(unittest.TestCase):
    """Tests for the deliberately small failure planning API."""

    def test_public_functions_expose_only_policy_inputs(self) -> None:
        self.assertEqual(
            tuple(
                inspect.signature(
                    failure_policy.consumes_failure_budget
                ).parameters
            ),
            ("status_reason",),
        )
        self.assertEqual(
            tuple(inspect.signature(failure_policy.plan_failure).parameters),
            ("status_reason", "reason", "budgeted", "non_budgeted"),
        )

    def test_plan_types_expose_only_feature_driving_fields(self) -> None:
        expected_fields = {
            failure_policy.ConsumeFailureBudget: (
                "failure_threshold",
                "backoff_base_sec",
                "backoff_max_sec",
            ),
            failure_policy.RetryWithoutBudget: ("retry_after",),
            failure_policy.FailurePersistencePlan: (
                "status_reason",
                "reason",
                "treatment",
            ),
        }

        for value_type, expected in expected_fields.items():
            with self.subTest(value_type=value_type.__name__):
                self.assertEqual(
                    tuple(
                        field.name for field in dataclasses.fields(value_type)
                    ),
                    expected,
                )


class TestFailurePlanning(unittest.TestCase):
    """Tests for shared classification and treatment materialization."""

    def test_current_status_reasons_share_one_budget_rule(self) -> None:
        expected_budgeted = {
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
        }

        self.assertEqual(
            {
                reason
                for reason in feed_store.FeedStatusReason
                if failure_policy.consumes_failure_budget(reason)
            },
            expected_budgeted,
        )

    def test_budgeted_plan_does_not_materialize_retry_treatment(self) -> None:
        non_budgeted = mock.Mock(
            side_effect=AssertionError("non-budgeted provider called")
        )

        plan = failure_policy.plan_failure(
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "invalid configuration",
            budgeted=_BUDGETED,
            non_budgeted=non_budgeted,
        )

        self.assertIs(plan.treatment, _BUDGETED)
        self.assertIs(
            plan.status_reason,
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(plan.reason, "invalid configuration")
        non_budgeted.assert_not_called()

    def test_non_budgeted_plan_materializes_one_exact_retry_treatment(
        self,
    ) -> None:
        retry = failure_policy.RetryWithoutBudget(
            _NOW + datetime.timedelta(minutes=8)
        )
        non_budgeted = mock.Mock(return_value=retry)

        plan = failure_policy.plan_failure(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "provider unavailable",
            budgeted=_BUDGETED,
            non_budgeted=non_budgeted,
        )

        self.assertIs(plan.treatment, retry)
        self.assertIs(
            plan.status_reason,
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(plan.reason, "provider unavailable")
        non_budgeted.assert_called_once_with()

    def test_post_bookmark_publish_failure_remains_non_budgeted(self) -> None:
        retry = failure_policy.RetryWithoutBudget(_NOW)

        plan = failure_policy.plan_failure(
            feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
            "publish failed",
            budgeted=_BUDGETED,
            non_budgeted=lambda: retry,
        )

        self.assertIs(plan.treatment, retry)

    def test_treatment_values_validate_durable_inputs(self) -> None:
        with self.assertRaises(ValueError):
            failure_policy.ConsumeFailureBudget(0, 15, 600)
        with self.assertRaises(ValueError):
            failure_policy.ConsumeFailureBudget(5, 601, 600)
        with self.assertRaises(ValueError):
            failure_policy.RetryWithoutBudget(_NOW.replace(tzinfo=None))

    def test_plan_rejects_treatment_outside_reason_classification(self) -> None:
        cases = (
            (
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                failure_policy.RetryWithoutBudget(_NOW),
            ),
            (
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                _BUDGETED,
            ),
        )

        for status_reason, treatment in cases:
            with self.subTest(status_reason=status_reason.value):
                with self.assertRaisesRegex(
                    ValueError,
                    "classification must match",
                ):
                    failure_policy.FailurePersistencePlan(
                        status_reason,
                        None,
                        treatment,
                    )


if __name__ == "__main__":
    unittest.main()
