from __future__ import annotations

import pathlib

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


def _read(path: str) -> str:
    return (_REPO_ROOT / path).read_text(encoding="utf-8")


def _assert_tokens(text: str, tokens: tuple[str, ...]) -> None:
    for token in tokens:
        assert token in text, token


def test_v1_audit_event_behavior_tests_are_registered() -> None:
    text = "\n".join(
        (
            _read("backend/pipeline/storage/tests/test_feed_store.py"),
            _read("integration_tests/storage/test_feed_store_integration.py"),
        )
    )

    for test_name, action in (
        ("test_create_feed_uses_combined_audit_sql", "feed.created"),
        (
            "test_meaningful_update_writes_feed_updated_audit_event",
            "feed.updated",
        ),
        (
            "test_success_writes_feed_deactivated_audit_event",
            "feed.deactivated",
        ),
        ("test_success_writes_feed_reset_audit_event", "feed.reset"),
        ("test_success_writes_feed_deleted_before_delete", "feed.deleted"),
        (
            "test_feed_deleted_uses_full_before_snapshot_and_empty_after",
            "feed.deleted",
        ),
        (
            "test_first_abnormal_failure_writes_failure_reported_audit_event",
            "feed.failure_reported",
        ),
        (
            "test_threshold_crossing_writes_quarantined_audit_event",
            "feed.quarantined",
        ),
        (
            "test_same_status_reason_failure_retry_writes_no_audit_event",
            "feed.failure_reported",
        ),
        (
            "test_successful_progress_writes_recovered_audit_event",
            "feed.recovered",
        ),
    ):
        assert test_name in text, test_name
        assert action in text, action


def test_v1_sync_echo_runtime_parity_tests_are_registered() -> None:
    text = _read("backend/pipeline/storage/tests/test_sync_feed_store.py")

    _assert_tokens(
        text,
        (
            "test_runtime_audit_insert_is_embedded_in_lifecycle_sql",
            "test_runtime_audit_actions_are_selected_in_sql",
            "feed.failure_reported",
            "feed.quarantined",
            "feed.recovered",
            "INSERT INTO feed_audit_events",
        ),
    )


def test_v1_diagnostic_detail_and_public_api_tests_are_registered() -> None:
    combined_text = "\n".join(
        (
            _read("backend/pipeline/storage/tests/test_feed_lifecycle.py"),
            _read("backend/services/feeds/tests/test_api.py"),
            _read("backend/services/feeds/tests/test_service.py"),
            _read("frontend/api/src/feeds/feedsController.test.ts"),
        )
    )

    _assert_tokens(
        combined_text,
        (
            "test_status_reason_detail_storage_value_caps_reason",
            "test_feed_model_exposes_status_reason_detail_not_quarantine_reason",
            "test_get_feed_with_status_reason_detail",
            'self.assertNotIn("quarantine_reason", data)',
            "test_admin_mutation_methods_require_keyword_only_actor_id",
            "status_reason_detail",
            "statusReasonDetail",
        ),
    )


def test_v1_no_noise_and_storage_ownership_tests_are_registered() -> None:
    combined_text = "\n".join(
        (
            _read("backend/pipeline/storage/tests/test_feed_store.py"),
            _read(
                "backend/pipeline/storage/tests/test_feed_query_contracts.py"
            ),
            _read("integration_tests/storage/test_feed_store_integration.py"),
        )
    )

    _assert_tokens(
        combined_text,
        (
            "test_same_status_reason_failure_retry_writes_no_audit_event",
            "test_noop_update_returns_current_feed_without_audit",
            "test_missing_update_target_returns_none_without_audit",
            "test_runtime_and_echo_sources_do_not_reference_audit_table",
            "feed_audit_events",
            "INSERT INTO feed_audit_events",
        ),
    )


def test_v1_pr_boundary_tests_are_registered() -> None:
    text = _read("backend/pipeline/storage/tests/test_feed_audit_contract.py")

    _assert_tokens(
        text,
        (
            "test_repository_glossary_defines_audit_terms",
            "feed-local `feed_revision`",
            "`user:google:<sub>`",
            "`service:<name>`",
            "test_audit_schema_defers_delivery_and_retention_work",
            "feed_event_outbox",
            "delivery_status",
            "cloudevents",
            "pg_cron",
            "test_status_reason_detail_helper_is_not_broad_normalizer",
            "_CREDENTIAL_RE",
            "_BEARER_TOKEN_RE",
        ),
    )
