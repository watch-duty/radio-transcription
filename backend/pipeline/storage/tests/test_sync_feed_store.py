"""Tests for the SyncFeedStore."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import TypedDict, cast
from unittest.mock import MagicMock, patch

import pytest

from backend.pipeline.storage import quarantine_reason, sync_feed_queries
from backend.pipeline.storage.feed_store import FeedStatus, FeedStatusReason
from backend.pipeline.storage.sync_feed_store import SyncFeedStore

_ECHO_ACTOR_ID = "service:echo-ingestion"


def _make_store(
    mock_conn: MagicMock,
    *,
    failure_threshold: int = 5,
    base_backoff_sec: int = 15,
    max_backoff_sec: int = 600,
) -> SyncFeedStore:
    """Build a SyncFeedStore backed by a mock connection."""
    connect_db = MagicMock(return_value=mock_conn)
    return SyncFeedStore(
        connect_db,
        failure_threshold=failure_threshold,
        base_backoff_sec=base_backoff_sec,
        max_backoff_sec=max_backoff_sec,
    )


def _make_mock_conn() -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = None
    return conn


def _make_cursor(
    *,
    row: dict[str, object] | None = None,
    rowcount: int = 0,
) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    cursor.rowcount = rowcount
    return cursor


def _make_transactional_conn(*cursors: MagicMock) -> MagicMock:
    conn = _make_mock_conn()
    conn.execute.side_effect = list(cursors)
    tx = MagicMock()
    tx.__enter__ = MagicMock(return_value=tx)
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx
    return conn


def _audit_snapshot_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": uuid.uuid4(),
        "name": "Central Fire",
        "source_type": "echo",
        "status": "active",
        "failure_count": 0,
        "retry_after": None,
        "status_reason": None,
        "status_reason_updated_at": None,
        "status_reason_detail": None,
        "quarantine_reason": None,
        "last_bookmark_time": None,
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        "feed_properties.source_feed_id": "fire-ca",
        "feed_properties.tags": "[]",
    }
    row.update(overrides)
    return row


class _SyncPriorKwargs(TypedDict):
    actor_id: str
    previous_status: FeedStatus
    previous_failure_count: int
    previous_status_reason: FeedStatusReason | None


def _sync_prior_kwargs(
    *,
    previous_status: FeedStatus = FeedStatus.ACTIVE,
    previous_failure_count: int = 0,
    previous_status_reason: FeedStatusReason | None = None,
) -> _SyncPriorKwargs:
    return {
        "actor_id": _ECHO_ACTOR_ID,
        "previous_status": previous_status,
        "previous_failure_count": previous_failure_count,
        "previous_status_reason": previous_status_reason,
    }


def _load_json_object(value: object) -> dict[str, object]:
    return cast("dict[str, object]", json.loads(cast("str", value)))


def _audit_insert_calls(
    conn: MagicMock,
) -> list[tuple[str, tuple[object, ...]]]:
    return [
        cast("tuple[str, tuple[object, ...]]", call.args)
        for call in conn.execute.call_args_list
        if call.args[0] == sync_feed_queries.INSERT_FEED_AUDIT_EVENT_SQL
    ]


class TestResolveEchoFeed:
    def test_returns_typed_feed_row(self) -> None:
        conn = _make_mock_conn()
        feed_id = uuid.uuid4()
        created_at = datetime(2026, 1, 1, tzinfo=UTC)
        feed_row = {
            "id": feed_id,
            "name": "Fire CA",
            "status": "active",
            "failure_count": 2,
            "status_reason": "source_unreachable",
            "created_at": created_at,
        }
        conn.execute.return_value.fetchone.return_value = feed_row
        store = _make_store(conn)

        result = store.resolve_echo_feed("fire-ca")

        assert result == {
            "id": feed_id,
            "name": "Fire CA",
            "status": FeedStatus.ACTIVE,
            "failure_count": 2,
            "status_reason": FeedStatusReason.SOURCE_UNREACHABLE,
            "created_at": created_at,
        }
        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert "f.failure_count" in sql
        assert "f.status_reason" in sql
        assert "AND fp.source_type = 'echo'" in sql
        assert "AND f.source_type = 'echo'" in sql
        assert params == ("fire-ca",)

    def test_returns_none_for_unknown_channel(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)

        result = store.resolve_echo_feed("unknown")

        assert result is None

    def test_raises_value_error_for_unknown_status(self) -> None:
        conn = _make_mock_conn()
        conn.execute.return_value.fetchone.return_value = {
            "id": uuid.uuid4(),
            "name": "Fire CA",
            "status": "not-a-status",
            "failure_count": 0,
            "status_reason": None,
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
        store = _make_store(conn)

        with pytest.raises(ValueError, match="Unknown feed status"):
            store.resolve_echo_feed("fire-ca")


class TestRecordHeartbeat:
    def test_executes_heartbeat_sql(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        store.record_heartbeat(feed_id)

        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert (
            "status NOT IN ('quarantined'::feed_status, "
            "'deactivated'::feed_status)"
        ) in sql
        assert params == (feed_id,)


class TestRecordFailure:
    def test_executes_failure_sql_with_config(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(
            conn,
            failure_threshold=5,
            base_backoff_sec=15,
            max_backoff_sec=600,
        )
        feed_id = uuid.uuid4()

        store.record_failure(
            feed_id,
            reason="echo_pubsub_publish_failed",
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert (
            "status NOT IN ('quarantined'::feed_status, "
            "'deactivated'::feed_status)"
        ) in sql
        assert params == (
            5,
            5,
            600,
            15,
            "system_pipeline_error",
            "echo_pubsub_publish_failed",
            "system_pipeline_error",
            feed_id,
        )

    def test_uses_custom_thresholds(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(
            conn,
            failure_threshold=10,
            base_backoff_sec=30,
            max_backoff_sec=1200,
        )
        feed_id = uuid.uuid4()

        store.record_failure(
            feed_id,
            reason="echo_heartbeat_write_failed",
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

        params = conn.execute.call_args[0][1]
        assert params == (
            10,
            10,
            1200,
            30,
            "system_pipeline_error",
            "echo_heartbeat_write_failed",
            "system_pipeline_error",
            feed_id,
        )

    def test_record_failure_allows_omitted_status_reason_for_compatibility(
        self,
    ) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        store.record_failure(feed_id, reason="raw")

        assert conn.execute.call_args[0][1] == (
            5,
            5,
            600,
            15,
            None,
            "raw",
            None,
            feed_id,
        )

    def test_caps_status_reason_detail_at_persistence_boundary(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()
        long_reason = "x" * (quarantine_reason.MAX_QUARANTINE_REASON_LENGTH + 1)

        store.record_failure(feed_id, reason=long_reason)

        reason_arg = conn.execute.call_args[0][1][5]
        assert len(reason_arg) == quarantine_reason.MAX_QUARANTINE_REASON_LENGTH
        assert reason_arg.endswith("[truncated]")

    def test_always_logs_failure(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        with patch(
            "backend.pipeline.storage.sync_feed_store.logger"
        ) as mock_logger:
            store.record_failure(feed_id)

        mock_logger.warning.assert_called_once()
        extra = mock_logger.warning.call_args[1]["extra"]
        assert extra["feed_id"] == str(feed_id)


class TestRecordNonBudgetedFailure:
    def test_executes_non_budgeted_failure_sql(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        store.record_non_budgeted_failure(
            feed_id,
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert "failure_count = 0" in sql
        assert "retry_after = NULL" in sql
        assert "status_reason_updated_at = CASE" in sql
        assert "WHEN status_reason IS DISTINCT FROM %s THEN NOW()" in sql
        assert (
            "status NOT IN ('quarantined'::feed_status, "
            "'deactivated'::feed_status)"
        ) in sql
        assert "quarantine_reason" not in sql
        assert params == (
            "system_pipeline_error",
            None,
            "system_pipeline_error",
            feed_id,
        )

    def test_logs_non_budgeted_failure(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        with patch(
            "backend.pipeline.storage.sync_feed_store.logger"
        ) as mock_logger:
            store.record_non_budgeted_failure(
                feed_id,
                status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
            )

        mock_logger.info.assert_called_once()
        extra = mock_logger.info.call_args[1]["extra"]
        assert extra == {
            "feed_id": str(feed_id),
            "status_reason": "system_collector_error",
        }


class TestSyncRuntimeAuditEvents:
    def test_same_combo_retry_emits_no_event(self) -> None:
        feed_id = uuid.uuid4()
        conn = _make_transactional_conn(
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="failing",
                    failure_count=1,
                    status_reason="system_collector_error",
                )
            ),
            _make_cursor(rowcount=1),
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="failing",
                    failure_count=2,
                    status_reason="system_collector_error",
                    status_reason_detail="new detail",
                )
            ),
        )
        store = _make_store(conn)

        store.record_failure(
            feed_id,
            **_sync_prior_kwargs(
                previous_status=FeedStatus.FAILING,
                previous_failure_count=1,
                previous_status_reason=(
                    FeedStatusReason.SYSTEM_COLLECTOR_ERROR
                ),
            ),
            reason="new detail",
            status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )

        assert _audit_insert_calls(conn) == []

    def test_reason_change_emits_failure_reported(self) -> None:
        feed_id = uuid.uuid4()
        conn = _make_transactional_conn(
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="failing",
                    failure_count=1,
                    status_reason="system_authentication_failed",
                )
            ),
            _make_cursor(rowcount=1),
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="failing",
                    failure_count=2,
                    status_reason="system_configuration_invalid",
                )
            ),
            _make_cursor(row={"feed_sequence": 8}),
            _make_cursor(rowcount=1),
        )
        store = _make_store(conn)

        store.record_failure(
            feed_id,
            **_sync_prior_kwargs(
                previous_status=FeedStatus.FAILING,
                previous_failure_count=1,
                previous_status_reason=(
                    FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED
                ),
            ),
            status_reason=FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )

        audit_params = _audit_insert_calls(conn)[0][1]
        assert audit_params[1] == "feed.failure_reported"
        assert audit_params[2] == _ECHO_ACTOR_ID
        metadata = _load_json_object(audit_params[6])
        assert metadata["previous_status_reason"] == (
            "system_authentication_failed"
        )

    def test_threshold_crossing_emits_only_quarantined(self) -> None:
        feed_id = uuid.uuid4()
        conn = _make_transactional_conn(
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="failing",
                    failure_count=4,
                    status_reason="source_unreachable",
                )
            ),
            _make_cursor(rowcount=1),
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="quarantined",
                    failure_count=5,
                    status_reason="source_unreachable",
                )
            ),
            _make_cursor(row={"feed_sequence": 9}),
            _make_cursor(rowcount=1),
        )
        store = _make_store(conn)

        store.record_failure(
            feed_id,
            **_sync_prior_kwargs(
                previous_status=FeedStatus.FAILING,
                previous_failure_count=4,
                previous_status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
            ),
            status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
        )

        actions = [call[1][1] for call in _audit_insert_calls(conn)]
        assert actions == ["feed.quarantined"]

    def test_recovery_heartbeat_from_failing_emits_recovered(self) -> None:
        feed_id = uuid.uuid4()
        conn = _make_transactional_conn(
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="failing",
                    failure_count=2,
                    status_reason="source_unreachable",
                )
            ),
            _make_cursor(rowcount=1),
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="active",
                    failure_count=0,
                    status_reason=None,
                )
            ),
            _make_cursor(row={"feed_sequence": 10}),
            _make_cursor(rowcount=1),
        )
        store = _make_store(conn)

        store.record_heartbeat(
            feed_id,
            **_sync_prior_kwargs(
                previous_status=FeedStatus.FAILING,
                previous_failure_count=2,
                previous_status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
            ),
        )

        audit_params = _audit_insert_calls(conn)[0][1]
        assert audit_params[1] == "feed.recovered"
        after_values = _load_json_object(audit_params[5])
        assert after_values["status"] == "active"
        assert after_values["status_reason"] is None

    def test_second_heartbeat_with_stale_resolved_prior_emits_no_event(
        self,
    ) -> None:
        feed_id = uuid.uuid4()
        conn = _make_transactional_conn(
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="active",
                    failure_count=0,
                    status_reason=None,
                )
            ),
            _make_cursor(rowcount=1),
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="active",
                    failure_count=0,
                    status_reason=None,
                )
            ),
        )
        store = _make_store(conn)

        store.record_heartbeat(
            feed_id,
            **_sync_prior_kwargs(
                previous_status=FeedStatus.FAILING,
                previous_failure_count=2,
                previous_status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
            ),
        )

        assert _audit_insert_calls(conn) == []

    def test_failure_after_recovery_with_stale_resolved_prior_emits_event(
        self,
    ) -> None:
        feed_id = uuid.uuid4()
        conn = _make_transactional_conn(
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="active",
                    failure_count=0,
                    status_reason=None,
                )
            ),
            _make_cursor(rowcount=1),
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="failing",
                    failure_count=1,
                    status_reason="source_unreachable",
                )
            ),
            _make_cursor(row={"feed_sequence": 11}),
            _make_cursor(rowcount=1),
        )
        store = _make_store(conn)

        store.record_failure(
            feed_id,
            **_sync_prior_kwargs(
                previous_status=FeedStatus.FAILING,
                previous_failure_count=2,
                previous_status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
            ),
            status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
        )

        audit_params = _audit_insert_calls(conn)[0][1]
        assert audit_params[1] == "feed.failure_reported"
        metadata = _load_json_object(audit_params[6])
        assert metadata["previous_status"] == "active"
        assert metadata["previous_failure_count"] == 0
        assert metadata["previous_status_reason"] is None

    def test_clean_heartbeat_emits_no_event(self) -> None:
        feed_id = uuid.uuid4()
        conn = _make_transactional_conn(
            _make_cursor(row=_audit_snapshot_row(id=feed_id)),
            _make_cursor(rowcount=1),
            _make_cursor(row=_audit_snapshot_row(id=feed_id)),
        )
        store = _make_store(conn)

        store.record_heartbeat(
            feed_id,
            **_sync_prior_kwargs(previous_status=FeedStatus.ACTIVE),
        )

        assert _audit_insert_calls(conn) == []

    def test_detail_only_heartbeat_clear_from_normal_emits_no_event(
        self,
    ) -> None:
        feed_id = uuid.uuid4()
        conn = _make_transactional_conn(
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="active",
                    failure_count=0,
                    status_reason=None,
                    status_reason_detail="stale detail",
                )
            ),
            _make_cursor(rowcount=1),
            _make_cursor(
                row=_audit_snapshot_row(
                    id=feed_id,
                    status="active",
                    failure_count=0,
                    status_reason=None,
                    status_reason_detail=None,
                )
            ),
        )
        store = _make_store(conn)

        store.record_heartbeat(
            feed_id,
            **_sync_prior_kwargs(previous_status=FeedStatus.ACTIVE),
        )

        assert _audit_insert_calls(conn) == []

    def test_no_row_mutation_emits_no_event(self) -> None:
        feed_id = uuid.uuid4()
        conn = _make_transactional_conn(
            _make_cursor(row=_audit_snapshot_row(id=feed_id)),
            _make_cursor(rowcount=0),
        )
        store = _make_store(conn)

        store.record_failure(
            feed_id,
            **_sync_prior_kwargs(),
            status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )

        assert _audit_insert_calls(conn) == []
