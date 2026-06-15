from __future__ import annotations

import dataclasses
import unittest

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.storage import feed_store


class TestFailurePolicyEvidence(unittest.TestCase):
    """Tests for facts-only failure policy evidence."""

    def test_evidence_contains_only_fact_fields(self) -> None:
        """Evidence excludes policy verdict/action fields."""
        fields = {
            field.name
            for field in dataclasses.fields(
                failure_policy.FailurePolicyEvidence
            )
        }

        self.assertEqual(
            fields,
            {
                "owner_scope",
                "failure_scope",
                "endpoint_kind",
                "pipeline_stage",
            },
        )
        self.assertNotIn("policy_intent", fields)
        self.assertNotIn("executed_action", fields)
        self.assertNotIn("feed_budget_eligible", fields)
        self.assertNotIn("quarantine_feed", fields)
        forbidden_field = "reason_" + "family"
        self.assertFalse(
            hasattr(failure_policy.FailurePolicyEvidence, forbidden_field)
        )

    def test_decision_contains_policy_verdict_fields(self) -> None:
        """Decision owns policy intent, action, and budget verdicts."""
        fields = {
            field.name
            for field in dataclasses.fields(
                failure_policy.FailurePolicyDecision
            )
        }

        self.assertEqual(
            fields,
            {
                "status_reason",
                "evidence",
                "policy_intent",
                "executed_action",
                "feed_budget_eligible",
                "quarantine_feed",
            },
        )


class TestClassifyFailurePolicy(unittest.TestCase):
    """Tests for pure policy classification."""

    def test_feed_configuration_failure_is_quarantine_eligible(self) -> None:
        """Feed-owned config evidence maps to the feed budget lane."""
        evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.FEED,
            failure_scope=failure_policy.FailureScope.FEED,
            endpoint_kind=failure_policy.EndpointKind.FEED_CONFIGURATION,
        )

        decision = failure_policy.classify_failure_policy(
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            evidence,
        )

        self.assertIs(
            decision.policy_intent,
            failure_policy.PolicyIntent.QUARANTINE_FEED,
        )
        self.assertIs(
            decision.executed_action,
            failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
        )
        self.assertTrue(decision.feed_budget_eligible)
        self.assertTrue(decision.quarantine_feed)
        self.assertTrue(failure_policy.is_feed_quarantine(decision))
        self.assertTrue(failure_policy.is_feed_budget_eligible(decision))
        self.assertFalse(failure_policy.is_pipeline_hold(decision))
        self.assertFalse(failure_policy.is_source_class_breaker(decision))

    def test_pipeline_publish_gap_is_hold_for_replay(self) -> None:
        """Post-bookmark publish failures never consume feed budget."""
        evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.PIPELINE,
            failure_scope=failure_policy.FailureScope.PIPELINE,
            endpoint_kind=failure_policy.EndpointKind.PUBSUB_PUBLISH,
            pipeline_stage=failure_policy.PipelineStage.PUBSUB_PUBLISH,
        )

        decision = failure_policy.classify_failure_policy(
            feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
            evidence,
        )

        self.assertIs(
            decision.policy_intent,
            failure_policy.PolicyIntent.HOLD_FOR_REPLAY,
        )
        self.assertIs(
            decision.executed_action,
            failure_policy.ExecutedAction.SUPPRESS_FEED_QUARANTINE_RECORD_PUBLISH_GAP,
        )
        self.assertFalse(decision.feed_budget_eligible)
        self.assertFalse(decision.quarantine_feed)
        self.assertFalse(failure_policy.is_feed_quarantine(decision))
        self.assertFalse(failure_policy.is_feed_budget_eligible(decision))
        self.assertTrue(failure_policy.is_pipeline_hold(decision))
        self.assertFalse(failure_policy.is_source_class_breaker(decision))

    def test_shared_auth_failure_opens_source_class_lane(self) -> None:
        """Shared auth evidence routes outside the per-feed budget."""
        evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.CREDENTIAL_SCOPE,
            failure_scope=failure_policy.FailureScope.CLASS,
            endpoint_kind=failure_policy.EndpointKind.CALLS_API,
        )

        decision = failure_policy.classify_failure_policy(
            feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            evidence,
        )

        self.assertIs(
            decision.policy_intent,
            failure_policy.PolicyIntent.OPEN_BREAKER,
        )
        self.assertFalse(decision.feed_budget_eligible)
        self.assertFalse(decision.quarantine_feed)
        self.assertTrue(failure_policy.is_source_class_breaker(decision))
