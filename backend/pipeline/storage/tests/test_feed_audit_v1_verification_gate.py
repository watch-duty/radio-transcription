from __future__ import annotations

import pathlib

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


def _read(path: str) -> str:
    return (_REPO_ROOT / path).read_text(encoding="utf-8")


def _assert_tokens(text: str, tokens: tuple[str, ...]) -> None:
    for token in tokens:
        assert token in text, token


def test_v1_audit_event_behavior_tests_are_registered() -> None:
    text = _read("backend/pipeline/storage/tests/test_feed_store.py")

    for test_name, action in (
        ("test_writes_feed_created_audit_event", "feed.created"),
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
            "test_first_abnormal_failure_emits_failure_reported",
            "feed.failure_reported",
        ),
        ("test_reason_change_emits_failure_reported", "feed.failure_reported"),
        (
            "test_threshold_crossing_emits_only_quarantined",
            "feed.quarantined",
        ),
        (
            "test_successful_progress_from_failing_emits_recovered",
            "feed.recovered",
        ),
        (
            "test_successful_progress_from_quarantined_emits_recovered",
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
            "test_reason_change_emits_failure_reported",
            "feed.failure_reported",
            "test_threshold_crossing_emits_only_quarantined",
            "feed.quarantined",
            "test_recovery_heartbeat_from_failing_emits_recovered",
            "feed.recovered",
            "test_clean_heartbeat_emits_no_event",
            "test_detail_only_heartbeat_clear_from_normal_emits_no_event",
            "test_no_row_mutation_emits_no_event",
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
        )
    )

    _assert_tokens(
        combined_text,
        (
            "test_clean_progress_emits_no_event",
            "test_detail_only_progress_clear_from_normal_emits_no_event",
            "test_clean_source_observation_emits_no_event",
            "test_lease_style_noop_emits_no_event",
            "test_runtime_and_echo_sources_do_not_reference_audit_table",
            "feed_audit_events",
            "INSERT INTO feed_audit_events",
        ),
    )
