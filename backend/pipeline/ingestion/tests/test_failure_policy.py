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

    def test_evidence_pattern_contains_only_matcher_fields(self) -> None:
        """Rule patterns keep wildcard matching separate from facts."""
        fields = {
            field.name
            for field in dataclasses.fields(failure_policy._EvidencePattern)
        }

        self.assertEqual(
            fields,
            {
                "owner_scopes",
                "failure_scopes",
                "endpoint_kinds",
                "pipeline_stages",
            },
        )

    def test_rule_contains_status_pattern_and_action(self) -> None:
        """Rules own status, evidence pattern, and action."""
        fields = {
            field.name
            for field in dataclasses.fields(failure_policy._FailurePolicyRule)
        }

        self.assertEqual(
            fields,
            {
                "status_reason",
                "evidence_pattern",
                "executed_action",
            },
        )

    def test_policy_module_does_not_export_boolean_action_wrappers(
        self,
    ) -> None:
        """Routing branches should compare ExecutedAction values directly."""
        self.assertFalse(hasattr(failure_policy, "is_feed_quarantine"))
        self.assertFalse(hasattr(failure_policy, "is_feed_budget_eligible"))
        self.assertFalse(hasattr(failure_policy, "is_pipeline_hold"))


class TestClassifyFailurePolicy(unittest.TestCase):
    """Tests for pure policy classification."""

    def _assert_action(
        self,
        *,
        status_reason: feed_store.FeedStatusReason,
        evidence: failure_policy.FailurePolicyEvidence,
        executed_action: failure_policy.ExecutedAction,
    ) -> None:
        action = failure_policy.classify_failure_policy(
            status_reason,
            evidence,
        )

        self.assertIs(action, executed_action)

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
                failure_policy.ExecutedAction.RECORD_POST_BOOKMARK_PUBLISH_GAP,
            ),
            (
                feed_store.FeedStatusReason.SOURCE_OFFLINE,
                source_stream_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                source_api_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
                source_class_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                auth_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                feed_config_evidence,
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
                runtime_config_evidence,
                failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED,
                credential_access_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                source_payload_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                unknown_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                pipeline_gcs_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_UNEXPECTED_ERROR,
                unknown_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
        )

        self.assertEqual(
            {case[0] for case in cases},
            set(feed_store.FeedStatusReason),
        )
        for (
            status_reason,
            evidence,
            executed_action,
        ) in cases:
            with self.subTest(status_reason=status_reason.value):
                self._assert_action(
                    status_reason=status_reason,
                    evidence=evidence,
                    executed_action=executed_action,
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

        self._assert_action(
            status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            evidence=evidence,
            executed_action=(
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET
            ),
        )

    def test_provider_control_config_failures_are_non_budgeted(
        self,
    ) -> None:
        """Provider/API config-looking failures stay retryable for v1."""
        for endpoint_kind in (
            failure_policy.EndpointKind.CALLS_API,
            failure_policy.EndpointKind.FIRE_POLL,
            failure_policy.EndpointKind.OPENMHZ_WS_UPGRADE,
        ):
            with self.subTest(endpoint_kind=endpoint_kind.value):
                evidence = failure_policy.FailurePolicyEvidence(
                    owner_scope=failure_policy.OwnerScope.FEED,
                    failure_scope=failure_policy.FailureScope.FEED,
                    endpoint_kind=endpoint_kind,
                )

                self._assert_action(
                    status_reason=(
                        feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
                    ),
                    evidence=evidence,
                    executed_action=(
                        failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET
                    ),
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
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                item_auth_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED,
                item_auth_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
            (
                feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                item_source_evidence,
                failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
            ),
        )

        for (
            status_reason,
            evidence,
            executed_action,
        ) in cases:
            with self.subTest(status_reason=status_reason.value):
                self._assert_action(
                    status_reason=status_reason,
                    evidence=evidence,
                    executed_action=executed_action,
                )

    def test_wrong_evidence_combinations_fail_closed_to_non_budgeted_release(
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
                self._assert_action(
                    status_reason=status_reason,
                    evidence=evidence,
                    executed_action=(
                        failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET
                    ),
                )

    def test_conflict_detector_finds_conflicting_policy_rules(self) -> None:
        """Overlapping rules with different actions are reported."""
        rules = (
            failure_policy._FailurePolicyRule(
                status_reason=(
                    feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
                ),
                evidence_pattern=failure_policy._EvidencePattern(
                    owner_scopes=frozenset({failure_policy.OwnerScope.FEED}),
                    failure_scopes=frozenset(
                        {failure_policy.FailureScope.FEED}
                    ),
                    endpoint_kinds=frozenset(
                        {failure_policy.EndpointKind.FEED_CONFIGURATION}
                    ),
                    pipeline_stages=frozenset({None}),
                ),
                executed_action=(
                    failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
                ),
            ),
            failure_policy._FailurePolicyRule(
                status_reason=(
                    feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
                ),
                evidence_pattern=failure_policy._EvidencePattern(
                    owner_scopes=frozenset({failure_policy.OwnerScope.FEED}),
                    failure_scopes=frozenset(
                        {failure_policy.FailureScope.FEED}
                    ),
                    endpoint_kinds=frozenset(
                        {failure_policy.EndpointKind.FEED_CONFIGURATION}
                    ),
                    pipeline_stages=frozenset({None}),
                ),
                executed_action=(
                    failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET
                ),
            ),
        )

        conflicts = failure_policy.find_policy_rule_conflicts(rules)

        self.assertEqual(len(conflicts), 1)
        conflict = conflicts[0]
        self.assertIs(
            conflict.status_reason,
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(conflict.matching_rule_indexes, (0, 1))
        self.assertEqual(
            conflict.executed_actions,
            frozenset(
                {
                    failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
                    failure_policy.ExecutedAction.RETRY_WITHOUT_FEED_BUDGET,
                }
            ),
        )
        self.assertEqual(
            conflict.evidence,
            failure_policy.FailurePolicyEvidence(
                owner_scope=failure_policy.OwnerScope.FEED,
                failure_scope=failure_policy.FailureScope.FEED,
                endpoint_kind=failure_policy.EndpointKind.FEED_CONFIGURATION,
            ),
        )

    def test_policy_rules_do_not_have_conflicting_actions(self) -> None:
        """No concrete evidence can route to two different actions."""
        self.assertEqual(failure_policy.find_policy_rule_conflicts(), ())
