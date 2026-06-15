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

    def _assert_decision(
        self,
        *,
        status_reason: feed_store.FeedStatusReason,
        evidence: failure_policy.FailurePolicyEvidence,
        policy_intent: failure_policy.PolicyIntent,
        executed_action: failure_policy.ExecutedAction,
        feed_budget_eligible: bool,
        quarantine_feed: bool,
    ) -> None:
        decision = failure_policy.classify_failure_policy(
            status_reason,
            evidence,
        )

        self.assertIs(decision.status_reason, status_reason)
        self.assertIs(decision.evidence, evidence)
        self.assertIs(decision.policy_intent, policy_intent)
        self.assertIs(decision.executed_action, executed_action)
        self.assertIs(decision.feed_budget_eligible, feed_budget_eligible)
        self.assertIs(decision.quarantine_feed, quarantine_feed)
        self.assertIs(
            failure_policy.is_feed_quarantine(decision),
            quarantine_feed,
        )
        self.assertIs(
            failure_policy.is_feed_budget_eligible(decision),
            feed_budget_eligible,
        )
        self.assertIs(
            failure_policy.is_pipeline_hold(decision),
            policy_intent is failure_policy.PolicyIntent.HOLD_FOR_REPLAY,
        )
        self.assertIs(
            failure_policy.is_source_class_breaker(decision),
            policy_intent is failure_policy.PolicyIntent.OPEN_BREAKER,
        )

    def test_current_status_reasons_have_explicit_policy_routes(
        self,
    ) -> None:
        """Every current status reason has an intended policy route."""
        pipeline_publish_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.PIPELINE,
            failure_scope=failure_policy.FailureScope.PIPELINE,
            endpoint_kind=failure_policy.EndpointKind.PUBSUB_PUBLISH,
            pipeline_stage=failure_policy.PipelineStage.PUBSUB_PUBLISH,
        )
        source_stream_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
            failure_scope=failure_policy.FailureScope.OBSERVATION,
            endpoint_kind=failure_policy.EndpointKind.STREAM,
        )
        source_api_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
            failure_scope=failure_policy.FailureScope.OBSERVATION,
            endpoint_kind=failure_policy.EndpointKind.CALLS_API,
        )
        source_class_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
            failure_scope=failure_policy.FailureScope.CLASS,
            endpoint_kind=failure_policy.EndpointKind.CALLS_API,
        )
        auth_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.CREDENTIAL_SCOPE,
            failure_scope=failure_policy.FailureScope.CLASS,
            endpoint_kind=failure_policy.EndpointKind.CALLS_API,
        )
        feed_config_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.FEED,
            failure_scope=failure_policy.FailureScope.FEED,
            endpoint_kind=failure_policy.EndpointKind.FEED_CONFIGURATION,
        )
        runtime_config_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
            failure_scope=failure_policy.FailureScope.CLASS,
            endpoint_kind=failure_policy.EndpointKind.FEED_CONFIGURATION,
        )
        credential_access_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.CREDENTIAL_SCOPE,
            failure_scope=failure_policy.FailureScope.FEED,
            endpoint_kind=failure_policy.EndpointKind.CALLS_API,
        )
        source_payload_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
            failure_scope=failure_policy.FailureScope.OBSERVATION,
            endpoint_kind=failure_policy.EndpointKind.CALLS_API,
        )
        pipeline_gcs_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.PIPELINE,
            failure_scope=failure_policy.FailureScope.PIPELINE,
            endpoint_kind=failure_policy.EndpointKind.GCS_UPLOAD,
            pipeline_stage=failure_policy.PipelineStage.GCS_UPLOAD,
        )
        unknown_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.UNKNOWN,
            failure_scope=failure_policy.FailureScope.UNKNOWN,
            endpoint_kind=failure_policy.EndpointKind.UNKNOWN,
        )

        cases = (
            (
                feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
                pipeline_publish_evidence,
                failure_policy.PolicyIntent.QUARANTINE_FEED,
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
                True,
                True,
            ),
            (
                feed_store.FeedStatusReason.SOURCE_OFFLINE,
                source_stream_evidence,
                failure_policy.PolicyIntent.SUPPRESS_RETRY,
                failure_policy.ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
                False,
                False,
            ),
            (
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                source_api_evidence,
                failure_policy.PolicyIntent.SUPPRESS_RETRY,
                failure_policy.ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
                False,
                False,
            ),
            (
                feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
                source_class_evidence,
                failure_policy.PolicyIntent.SUPPRESS_RETRY,
                failure_policy.ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
                False,
                False,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                auth_evidence,
                failure_policy.PolicyIntent.QUARANTINE_FEED,
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
                True,
                True,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                feed_config_evidence,
                failure_policy.PolicyIntent.QUARANTINE_FEED,
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
                True,
                True,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
                runtime_config_evidence,
                failure_policy.PolicyIntent.QUARANTINE_FEED,
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
                True,
                True,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED,
                credential_access_evidence,
                failure_policy.PolicyIntent.SUPPRESS_RETRY,
                failure_policy.ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
                False,
                False,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                source_payload_evidence,
                failure_policy.PolicyIntent.QUARANTINE_FEED,
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
                True,
                True,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                unknown_evidence,
                failure_policy.PolicyIntent.TELEMETRY_GAP,
                failure_policy.ExecutedAction.SUPPRESS_FEED_QUARANTINE_TELEMETRY_GAP,
                False,
                False,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                pipeline_gcs_evidence,
                failure_policy.PolicyIntent.SUPPRESS_RETRY,
                failure_policy.ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
                False,
                False,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_UNEXPECTED_ERROR,
                unknown_evidence,
                failure_policy.PolicyIntent.TELEMETRY_GAP,
                failure_policy.ExecutedAction.SUPPRESS_FEED_QUARANTINE_TELEMETRY_GAP,
                False,
                False,
            ),
        )

        self.assertEqual(
            {case[0] for case in cases},
            set(feed_store.FeedStatusReason),
        )
        for (
            status_reason,
            evidence,
            policy_intent,
            executed_action,
            feed_budget_eligible,
            quarantine_feed,
        ) in cases:
            with self.subTest(status_reason=status_reason.value):
                self._assert_decision(
                    status_reason=status_reason,
                    evidence=evidence,
                    policy_intent=policy_intent,
                    executed_action=executed_action,
                    feed_budget_eligible=feed_budget_eligible,
                    quarantine_feed=quarantine_feed,
                )

    def test_system_pipeline_error_bookmark_write_is_non_budgeted(
        self,
    ) -> None:
        """Bookmark pipeline evidence stays outside feed quarantine budget."""
        evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.PIPELINE,
            failure_scope=failure_policy.FailureScope.PIPELINE,
            endpoint_kind=failure_policy.EndpointKind.BOOKMARK_WRITE,
            pipeline_stage=failure_policy.PipelineStage.BOOKMARK_WRITE,
        )

        self._assert_decision(
            status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            evidence=evidence,
            policy_intent=failure_policy.PolicyIntent.SUPPRESS_RETRY,
            executed_action=(
                failure_policy.ExecutedAction.RELEASE_NON_BUDGETED_FAILURE
            ),
            feed_budget_eligible=False,
            quarantine_feed=False,
        )

    def test_promoted_item_scope_source_and_auth_routes_are_explicit(
        self,
    ) -> None:
        """All-items-failed item promotions keep their intended route."""
        item_source_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
            failure_scope=failure_policy.FailureScope.ITEM,
            endpoint_kind=failure_policy.EndpointKind.CALLS_MEDIA,
        )
        item_auth_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.CREDENTIAL_SCOPE,
            failure_scope=failure_policy.FailureScope.ITEM,
            endpoint_kind=failure_policy.EndpointKind.CALLS_MEDIA,
        )

        cases = (
            (
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                item_source_evidence,
                failure_policy.PolicyIntent.SUPPRESS_RETRY,
                failure_policy.ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
                False,
                False,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                item_auth_evidence,
                failure_policy.PolicyIntent.QUARANTINE_FEED,
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
                True,
                True,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED,
                item_auth_evidence,
                failure_policy.PolicyIntent.SUPPRESS_RETRY,
                failure_policy.ExecutedAction.RELEASE_NON_BUDGETED_FAILURE,
                False,
                False,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                item_source_evidence,
                failure_policy.PolicyIntent.QUARANTINE_FEED,
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
                True,
                True,
            ),
        )

        for (
            status_reason,
            evidence,
            policy_intent,
            executed_action,
            feed_budget_eligible,
            quarantine_feed,
        ) in cases:
            with self.subTest(status_reason=status_reason.value):
                self._assert_decision(
                    status_reason=status_reason,
                    evidence=evidence,
                    policy_intent=policy_intent,
                    executed_action=executed_action,
                    feed_budget_eligible=feed_budget_eligible,
                    quarantine_feed=quarantine_feed,
                )

    def test_wrong_evidence_combinations_fail_closed_to_telemetry_gap(
        self,
    ) -> None:
        """Mismatched status/evidence pairs fail closed, not by owner scope."""
        wrong_feed_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.FEED,
            failure_scope=failure_policy.FailureScope.FEED,
            endpoint_kind=failure_policy.EndpointKind.FEED_CONFIGURATION,
        )
        wrong_pipeline_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.PIPELINE,
            failure_scope=failure_policy.FailureScope.PIPELINE,
            endpoint_kind=failure_policy.EndpointKind.GCS_UPLOAD,
            pipeline_stage=failure_policy.PipelineStage.GCS_UPLOAD,
        )
        wrong_source_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
            failure_scope=failure_policy.FailureScope.CLASS,
            endpoint_kind=failure_policy.EndpointKind.STREAM,
        )
        wrong_unknown_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.UNKNOWN,
            failure_scope=failure_policy.FailureScope.UNKNOWN,
            endpoint_kind=failure_policy.EndpointKind.UNKNOWN,
        )
        wrong_credential_evidence = failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.CREDENTIAL_SCOPE,
            failure_scope=failure_policy.FailureScope.FEED,
            endpoint_kind=failure_policy.EndpointKind.CALLS_API,
        )

        cases = (
            (
                feed_store.FeedStatusReason.SOURCE_OFFLINE,
                wrong_feed_evidence,
            ),
            (
                feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
                wrong_pipeline_evidence,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                wrong_source_evidence,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                wrong_unknown_evidence,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
                wrong_feed_evidence,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED,
                wrong_source_evidence,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                wrong_credential_evidence,
            ),
        )

        for status_reason, evidence in cases:
            with self.subTest(
                status_reason=status_reason.value,
                owner_scope=evidence.owner_scope.value,
            ):
                self._assert_decision(
                    status_reason=status_reason,
                    evidence=evidence,
                    policy_intent=failure_policy.PolicyIntent.TELEMETRY_GAP,
                    executed_action=(
                        failure_policy.ExecutedAction.SUPPRESS_FEED_QUARANTINE_TELEMETRY_GAP
                    ),
                    feed_budget_eligible=False,
                    quarantine_feed=False,
                )
