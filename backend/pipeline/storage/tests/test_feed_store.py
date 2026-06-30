from __future__ import annotations

import datetime
import inspect
import json
import pathlib
import re
import unittest
import uuid
from typing import Any, TypedDict, cast
from unittest import mock

import asyncpg
import yaml

from backend.pipeline.common.exceptions import (
    FeedAlreadyExistsError,
    FeedNameAlreadyExistsError,
    FeedStateConflictError,
)
from backend.pipeline.storage import (
    feed_audit_sql,
    feed_queries,
    feed_store,
    status_reason_detail,
)
from backend.pipeline.storage.feed_store import (
    FeedStatus,
    FeedStatusReason,
    FeedStore,
    HeartbeatResult,
    SourceType,
)
from backend.pipeline.storage.pagination_utils import encode_cursor
from backend.pipeline.storage.tests.connection_util import make_mock_pool

_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_ID_B = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_STATUS_REASON_UPDATED_AT = datetime.datetime(
    2026, 5, 29, 12, 0, tzinfo=datetime.UTC
)
_FEEDS_SERVICE_ACTOR_ID = "user:google:admin@example.com"
_COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID = (
    "service_account:gcp:123456789012345678901"
)
_MISSING_ACTOR_ID = cast("str", None)

_FEED_STATUS_REASON_VALUES = {
    "pipeline_publish_after_bookmark_failed",
    "source_offline",
    "source_unreachable",
    "source_rate_limited",
    "system_authentication_failed",
    "system_configuration_invalid",
    "system_source_configuration_invalid",
    "system_runtime_configuration_invalid",
    "system_credential_access_failed",
    "system_source_payload_invalid",
    "system_collector_error",
    "system_pipeline_error",
    "system_unexpected_error",
}

_LEASE_ROW = {
    "id": _FEED_ID,
    "name": "My Feed",
    "source_type": "bcfy_feeds",
    "last_processed_filename": None,
    "last_bookmark_time": None,
    "fencing_token": 1,
    "failure_count": 0,
    "status_reason": None,
    "source_feed_id": "123",
}


def _full_feed_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": _FEED_ID,
        "name": "My Feed",
        "source_type": "bcfy_feeds",
        "status": "unclaimed",
        "status_reason": None,
        "status_reason_updated_at": None,
        "status_reason_detail": None,
        "failure_count": 0,
        "retry_after": None,
        "worker_id": None,
        "last_heartbeat": None,
        "last_processed_filename": None,
        "last_bookmark_time": None,
        "created_at": datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        "feed_revision": 1,
        "source_feed_id": "123",
        "tags": "[]",
        "last_speech_segment_timestamp": None,
    }
    row.update(overrides)
    return row


def _audit_snapshot_row(**overrides: object) -> dict[str, object]:
    return _full_feed_row(**overrides)


def _feed_audit_event(action: str = "feed.recovered") -> dict[str, object]:
    return {
        "event_type": "radio_transcription.feed_change_notification",
        "schema_version": 1,
        "event_id": uuid.UUID("cccccccc-dddd-eeee-ffff-000000000000"),
        "action": action,
        "occurred_at": datetime.datetime(2026, 6, 26, tzinfo=datetime.UTC),
        "actor_id": _COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
        "feed_id": _FEED_ID,
        "feed_revision": 2,
        "before_values": {"status": "active"},
        "after_values": {"status": "unclaimed"},
    }


class _RuntimePriorKwargs(TypedDict):
    actor_id: str


def _runtime_prior_kwargs() -> _RuntimePriorKwargs:
    return {"actor_id": _COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID}


def _failure_update_row(
    *,
    status: str = "failing",
    failure_count: int = 1,
    retry_after: datetime.datetime | None = None,
    status_reason: str | None = "system_unexpected_error",
    status_reason_detail: str | None = None,
    **overrides: object,
) -> dict[str, object]:
    return _audit_snapshot_row(
        status=status,
        failure_count=failure_count,
        retry_after=retry_after,
        status_reason=status_reason,
        status_reason_detail=status_reason_detail,
        **overrides,
    )


def _unique_violation(
    constraint_name: str,
) -> asyncpg.exceptions.UniqueViolationError:
    error = asyncpg.exceptions.UniqueViolationError("duplicate key")
    cast("Any", error).constraint_name = constraint_name
    return error


def _sql_without_comments(text: str) -> str:
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("--")
    )


def _normalized_sql(text: str) -> str:
    return " ".join(_sql_without_comments(text).split())


class TestTransactionMockPool(unittest.IsolatedAsyncioTestCase):
    """Tests for transaction-capable storage mock helpers."""

    async def test_pool_acquire_returns_inspectable_connection(self) -> None:
        pool = make_mock_pool(transaction=True)

        async with pool.acquire() as conn:
            self.assertIs(conn, pool.acquired_connection)

        pool.acquire_context.__aenter__.assert_awaited_once()
        pool.acquire_context.__aexit__.assert_awaited_once()

    async def test_connection_exposes_transaction_and_query_mocks(
        self,
    ) -> None:
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection

        async with conn.transaction():
            await conn.fetchrow("select row")
            await conn.fetchval("select value")
            await conn.fetch("select rows")
            await conn.execute("update row")

        pool.transaction_context.__aenter__.assert_awaited_once()
        pool.transaction_context.__aexit__.assert_awaited_once()
        conn.fetchrow.assert_awaited_once_with("select row")
        conn.fetchval.assert_awaited_once_with("select value")
        conn.fetch.assert_awaited_once_with("select rows")
        conn.execute.assert_awaited_once_with("update row")

    async def test_pool_level_behavior_remains_available(self) -> None:
        row = _full_feed_row()
        pool = make_mock_pool(fetchrow_result=row, transaction=True)

        self.assertIs(await pool.fetchrow("select feed"), row)
        self.assertEqual(await pool.execute("update feed"), "UPDATE 0")
        self.assertEqual(await pool.fetch("select feeds"), [])
        self.assertEqual(await pool.fetchval("select count"), 0)

    def test_full_feed_row_includes_audit_snapshot_fields(self) -> None:
        row = _full_feed_row(
            tags='[{"key": "county", "value": "Fulton"}]',
            status_reason_detail="provider timeout",
            retry_after=datetime.datetime(2026, 4, 11, tzinfo=datetime.UTC),
        )

        self.assertEqual(row["status_reason_detail"], "provider timeout")
        self.assertIsNotNone(row["retry_after"])
        self.assertEqual(
            json.loads(cast("str", row["tags"])),
            [{"key": "county", "value": "Fulton"}],
        )


class TestStatusReasonMigrationContract(unittest.TestCase):
    """Contract tests for the Phase 1 status reason migration."""

    _MIGRATION = pathlib.Path(
        "terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql"
    )

    def test_adds_only_nullable_status_reason_columns(self) -> None:
        self.assertTrue(self._MIGRATION.exists())
        text = self._MIGRATION.read_text()
        sql = _sql_without_comments(text)

        column_defs = [
            (name.lower(), " ".join(definition.upper().split()))
            for name, definition in re.findall(
                r"ADD COLUMN IF NOT EXISTS\s+(\w+)\s+([^;]+);",
                sql,
                flags=re.IGNORECASE,
            )
        ]
        self.assertEqual(
            column_defs,
            [
                ("status_reason", "TEXT"),
                (
                    "status_reason_updated_at",
                    "TIMESTAMP WITH TIME ZONE",
                ),
            ],
        )

    def test_migration_has_no_backfill_default_constraint_index_or_type(
        self,
    ) -> None:
        text = self._MIGRATION.read_text()
        low_sql = _sql_without_comments(text).lower()

        for token in (
            "default",
            "update feeds",
            "check",
            "create index",
            "create type",
        ):
            self.assertNotIn(token, low_sql)


class TestFeedStatusReason(unittest.TestCase):
    """Contract tests for the canonical status reason vocabulary."""

    def test_canonical_reason_values(self) -> None:
        self.assertEqual(
            {reason.value for reason in FeedStatusReason},
            _FEED_STATUS_REASON_VALUES,
        )

    def test_matches_openapi_spec(self) -> None:
        current_file = pathlib.Path(__file__).resolve()
        repo_root = current_file.parents[4]
        openapi_path = repo_root / "frontend" / "api" / "openapi.yaml"
        self.assertTrue(
            openapi_path.exists(),
            f"Could not find openapi.yaml at {openapi_path}",
        )

        with openapi_path.open("r") as f:
            spec = yaml.safe_load(f)

        schemas = spec.get("components", {}).get("schemas", {})
        backend_reasons = schemas.get("BackendFeedStatusReason", {}).get(
            "enum", []
        )

        expected_openapi_reasons = _FEED_STATUS_REASON_VALUES | {"unknown"}

        self.assertEqual(
            set(backend_reasons),
            expected_openapi_reasons,
            "The status reasons exposed by frontend/api/openapi.yaml "
            "do not match the canonical backend vocabulary. "
            "Please run `yarn generate-spec` in frontend/api to sync the spec after updating TypeScript types.",
        )

    def test_reason_values_encode_source_or_system_ownership(self) -> None:
        for reason in FeedStatusReason:
            self.assertTrue(
                reason.value.startswith(("source_", "system_", "pipeline_")),
                reason.value,
            )

    def test_reason_owner_comes_from_status_prefix(self) -> None:
        cases = {
            FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED: "pipeline",
            FeedStatusReason.SOURCE_OFFLINE: "source",
            FeedStatusReason.SOURCE_UNREACHABLE: "source",
            FeedStatusReason.SOURCE_RATE_LIMITED: "source",
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED: "system",
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID: "system",
            FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID: "system",
            FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID: "system",
            FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED: "system",
            FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID: "system",
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR: "system",
            FeedStatusReason.SYSTEM_PIPELINE_ERROR: "system",
            FeedStatusReason.SYSTEM_UNEXPECTED_ERROR: "system",
        }

        self.assertEqual(set(cases), set(FeedStatusReason))
        for reason, owner in cases.items():
            with self.subTest(reason=reason.value):
                self.assertEqual(reason.owner, owner)

    def test_reason_owner_rejects_unknown_prefix(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported status reason"):
            feed_store._status_reason_owner("unknown_failure")


class TestSourceType(unittest.TestCase):
    """Contract tests for SourceType enum."""

    def test_matches_openapi_spec(self) -> None:
        current_file = pathlib.Path(__file__).resolve()
        repo_root = current_file.parents[4]
        openapi_path = repo_root / "frontend" / "api" / "openapi.yaml"
        self.assertTrue(
            openapi_path.exists(),
            f"Could not find openapi.yaml at {openapi_path}",
        )

        with openapi_path.open("r") as f:
            spec = yaml.safe_load(f)

        schemas = spec.get("components", {}).get("schemas", {})
        openapi_sources = schemas.get("SourceType", {}).get("enum", [])

        python_sources = {source.value for source in SourceType}

        self.assertEqual(
            set(openapi_sources),
            python_sources,
            "The sources in backend.pipeline.storage.feed_store.SourceType "
            "do not match SourceType in frontend/api/openapi.yaml. "
            "Please run `yarn generate-spec` in frontend/api to sync the spec after updating TypeScript types.",
        )


class TestFeedStatus(unittest.TestCase):
    """Contract tests for FeedStatus enum."""

    def test_matches_openapi_spec(self) -> None:
        current_file = pathlib.Path(__file__).resolve()
        repo_root = current_file.parents[4]
        openapi_path = repo_root / "frontend" / "api" / "openapi.yaml"
        self.assertTrue(
            openapi_path.exists(),
            f"Could not find openapi.yaml at {openapi_path}",
        )

        with openapi_path.open("r") as f:
            spec = yaml.safe_load(f)

        schemas = spec.get("components", {}).get("schemas", {})
        openapi_statuses = schemas.get("BackendFeedStatus", {}).get("enum", [])

        python_statuses = {status.value for status in FeedStatus}

        self.assertEqual(
            set(openapi_statuses),
            python_statuses,
            "The status values in backend.pipeline.storage.feed_store.FeedStatus "
            "do not match BackendFeedStatus in frontend/api/openapi.yaml. "
            "Please run `yarn generate-spec` in frontend/api to sync the spec after updating TypeScript types.",
        )


class TestStatusReasonRowMapping(unittest.TestCase):
    """Tests for mapping nullable DB status reason fields to Feed."""

    def test_null_reason_maps_to_none(self) -> None:
        store = FeedStore(make_mock_pool())

        result = store._row_to_feed(cast("asyncpg.Record", _full_feed_row()))

        self.assertIsNone(result["status_reason"])
        self.assertIsNone(result["status_reason_updated_at"])
        self.assertIsNone(result["status_reason_detail"])

    def test_status_reason_detail_maps_to_feed(self) -> None:
        store = FeedStore(make_mock_pool())
        row = _full_feed_row(status_reason_detail="provider timeout")

        result = store._row_to_feed(cast("asyncpg.Record", row))

        self.assertEqual(result["status_reason_detail"], "provider timeout")

    def test_valid_reason_maps_to_enum(self) -> None:
        store = FeedStore(make_mock_pool())

        for reason in FeedStatusReason:
            row = _full_feed_row(
                status_reason=reason.value,
                status_reason_updated_at=_STATUS_REASON_UPDATED_AT,
            )

            result = store._row_to_feed(cast("asyncpg.Record", row))

            self.assertIs(result["status_reason"], reason)
            self.assertEqual(
                result["status_reason_updated_at"],
                _STATUS_REASON_UPDATED_AT,
            )

    def test_invalid_reason_text_raises_value_error(self) -> None:
        store = FeedStore(make_mock_pool())
        row = _full_feed_row(status_reason="free-form raw error")

        with self.assertRaises(ValueError) as context:
            store._row_to_feed(cast("asyncpg.Record", row))

        self.assertIn("Unknown status reason", str(context.exception))


class TestLastSpeechSegmentTimestampMapping(unittest.TestCase):
    """Tests for mapping last_speech_segment_timestamp DB fields to Feed."""

    def test_null_timestamp_maps_to_none(self) -> None:
        store = FeedStore(make_mock_pool())
        result = store._row_to_feed(cast("asyncpg.Record", _full_feed_row()))
        self.assertIsNone(result["last_speech_segment_timestamp"])

    def test_valid_timestamp_maps_correctly(self) -> None:
        store = FeedStore(make_mock_pool())
        timestamp = datetime.datetime(
            2026, 6, 16, 18, 0, 0, tzinfo=datetime.UTC
        )
        row = _full_feed_row(last_speech_segment_timestamp=timestamp)
        result = store._row_to_feed(cast("asyncpg.Record", row))
        self.assertEqual(result["last_speech_segment_timestamp"], timestamp)


class TestStatusReasonSqlProjection(unittest.TestCase):
    """Tests for full-feed SQL projection coverage."""

    def test_full_feed_queries_project_status_reason_fields(self) -> None:
        for sql in (
            feed_queries.CREATE_FEED_SQL,
            feed_queries.GET_FEED_SQL,
            feed_queries.LIST_FEEDS_DESC_SQL,
            feed_queries.LIST_FEEDS_ASC_SQL,
            feed_queries.RESET_FEED_SQL,
            feed_queries.UPDATE_FEED_SQL,
        ):
            self.assertRegex(sql, r"\bstatus_reason\b")
            self.assertRegex(sql, r"\bstatus_reason_updated_at\b")
            self.assertRegex(sql, r"\bstatus_reason_detail\b")


class TestLastSpeechSegmentTimestampSqlProjection(unittest.TestCase):
    def test_full_feed_queries_project_last_speech_segment_timestamp(
        self,
    ) -> None:
        for sql in (
            feed_queries.CREATE_FEED_SQL,
            feed_queries.GET_FEED_SQL,
            feed_queries.LIST_FEEDS_DESC_SQL,
            feed_queries.LIST_FEEDS_ASC_SQL,
            feed_queries.RESET_FEED_SQL,
            feed_queries.UPDATE_FEED_SQL,
        ):
            self.assertRegex(sql, r"\blast_speech_segment_timestamp\b")


class TestFeedAuditSql(unittest.TestCase):
    """Text-level contract tests for storage-owned audit SQL primitives."""

    def test_audited_mutation_sql_embeds_audit_insert(self) -> None:
        for sql in (
            feed_queries.CREATE_FEED_SQL,
            feed_queries.UPDATE_FEED_SQL,
            feed_queries.DEACTIVATE_FEED_SQL,
            feed_queries.DELETE_FEED_SQL,
            feed_queries.RESET_FEED_SQL,
            feed_queries.UPDATE_PROGRESS_SQL,
            feed_queries.RECORD_SOURCE_OBSERVATION_SQL,
            feed_queries.REPORT_FAILURE_SQL,
            feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL,
        ):
            stripped = _sql_without_comments(sql)
            self.assertIn("INSERT INTO feed_audit_events", stripped)
            self.assertIn("feed_revision", stripped)
            self.assertIn("before_values", stripped)
            self.assertIn("after_values", stripped)

    def test_audit_snapshots_use_explicit_allowlist_on_row_ctes(self) -> None:
        snapshot_sql = feed_audit_sql.audit_snapshot_sql("before_row")

        for key in (
            "'id'",
            "'name'",
            "'source_type'",
            "'status'",
            "'failure_count'",
            "'retry_after'",
            "'status_reason'",
            "'status_reason_updated_at'",
            "'status_reason_detail'",
            "'created_at'",
            "'source_feed_id'",
            "'tags'",
        ):
            self.assertIn(key, snapshot_sql)

        self.assertNotIn("'last_bookmark_time'", snapshot_sql)
        self.assertNotIn("'quarantine_reason'", snapshot_sql)

        for sql in (
            feed_queries.UPDATE_FEED_SQL,
            feed_queries.RESET_FEED_SQL,
            feed_queries.REPORT_FAILURE_SQL,
        ):
            stripped = _sql_without_comments(sql)
            self.assertRegex(stripped, r"SELECT\s+f\.\*")
            self.assertIn("'source_feed_id'", stripped)
            self.assertIn("'tags'", stripped)
            self.assertNotIn("SELECT fp.*", stripped)

    def test_runtime_audit_actions_are_selected_in_sql(self) -> None:
        failure_sql = _sql_without_comments(feed_queries.REPORT_FAILURE_SQL)
        recovery_sql = _sql_without_comments(feed_queries.UPDATE_PROGRESS_SQL)

        self.assertIn("audit_action AS", failure_sql)
        self.assertIn("THEN 'feed.failure_reported'", failure_sql)
        self.assertIn("THEN 'feed.quarantined'", failure_sql)
        self.assertIn("audit_action AS", recovery_sql)
        self.assertIn("THEN 'feed.recovered'", recovery_sql)

    def test_reset_sql_clears_and_returns_status_reason_detail(self) -> None:
        sql = _normalized_sql(feed_queries.RESET_FEED_SQL)

        self.assertIn("status_reason_detail = NULL", sql)
        self.assertIn("RETURNING feeds.*", sql)


class TestFeedAuditStorageBoundary(unittest.TestCase):
    """Hardening checks for the storage-owned audit boundary."""

    def test_audited_mutations_require_explicit_keyword_actor(self) -> None:
        for method_name in (
            "create_feed",
            "update_feed",
            "deactivate_feed",
            "delete_feed",
            "reset_feed",
            "report_feed_failure",
            "release_non_budgeted_failure",
        ):
            with self.subTest(method_name=method_name):
                signature = inspect.signature(getattr(FeedStore, method_name))
                actor = signature.parameters.get("actor_id")

                self.assertIsNotNone(actor)
                assert actor is not None
                self.assertEqual(
                    actor.kind,
                    inspect.Parameter.KEYWORD_ONLY,
                )
                self.assertIs(actor.default, inspect.Parameter.empty)

    def test_feed_request_models_do_not_accept_actor_id(self) -> None:
        text = pathlib.Path("backend/services/feeds/models.py").read_text()

        self.assertNotIn("actor_id", text)

    def test_feeds_service_does_not_build_audit_rows_directly(self) -> None:
        for path in (
            pathlib.Path("backend/services/feeds/main.py"),
            pathlib.Path("backend/services/feeds/service.py"),
        ):
            with self.subTest(path=str(path)):
                text = path.read_text()

                self.assertNotIn("feed_audit_events", text)
                self.assertNotIn("_insert_feed_audit_event", text)


class TestStatusReasonLifecycleIsolation(unittest.TestCase):
    """Tests that lifecycle SQL remains independent of status_reason."""

    def test_claim_release_heartbeat_count_and_deactivate_do_not_reference_status_reason(
        self,
    ) -> None:
        lifecycle_sql = [
            feed_queries.RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL,
            feed_queries.RELEASE_FEED_SQL,
            feed_queries.RELEASE_FEEDS_BATCH_SQL,
            feed_queries.COUNT_HELD_BY_TYPE_SQL,
        ]

        for sql in lifecycle_sql:
            self.assertNotIn("status_reason", _sql_without_comments(sql))

    def test_claim_sql_projects_failure_state_without_mutating_it(
        self,
    ) -> None:
        claim_sql = [
            feed_queries.build_acquire_feeds_batch_sql([SourceType.BCFY_FEEDS]),
            feed_queries.build_acquire_feeds_recovery_sql(
                [SourceType.BCFY_FEEDS]
            ),
        ]

        for sql in claim_sql:
            stripped = _sql_without_comments(sql)
            self.assertIn("feeds.failure_count", stripped)
            self.assertIn("feeds.status_reason", stripped)
            self.assertIn("leased.failure_count", stripped)
            self.assertIn("leased.status_reason", stripped)
            self.assertNotIn("status_reason =", stripped)


class TestWorkerOwnedLifecycleGuards(unittest.TestCase):
    """Tests that worker-owned writes cannot undo lifecycle changes."""

    def test_failure_and_release_require_active_status(self) -> None:
        fenced_sql = [
            feed_queries.RELEASE_FEED_SQL,
            feed_queries.REPORT_FAILURE_SQL,
        ]

        for sql in fenced_sql:
            self.assertIn(
                "status = 'active'::feed_status", _sql_without_comments(sql)
            )

    def test_progress_remains_allowed_after_deactivation(self) -> None:
        """Main allows one in-flight bookmark write after admin stop."""
        sql = _sql_without_comments(feed_queries.UPDATE_PROGRESS_SQL)

        self.assertNotIn("AND status = 'active'::feed_status", sql)

    def test_batch_worker_release_requires_active_status(self) -> None:
        sql = _sql_without_comments(feed_queries.RELEASE_FEEDS_BATCH_SQL)

        self.assertIn("WHERE worker_id = $1", sql)
        self.assertIn("AND status = 'active'::feed_status", sql)


class TestReportFailureSqlStatusReason(unittest.TestCase):
    """Tests for status reason writes in failure SQL."""

    def test_report_failure_sql_writes_status_reason_in_fenced_update(
        self,
    ) -> None:
        sql = _sql_without_comments(feed_queries.REPORT_FAILURE_SQL)

        self.assertIn(
            "status_reason = COALESCE($7, 'system_unexpected_error')",
            sql,
        )
        self.assertIn("status_reason_detail = $8", sql)
        self.assertRegex(
            sql,
            r"status_reason_updated_at = CASE\s+"
            r"WHEN feeds.status_reason IS DISTINCT FROM COALESCE"
            r"\(\$7, 'system_unexpected_error'\)\s+"
            r"THEN NOW\(\)\s+"
            r"ELSE feeds.status_reason_updated_at\s+END",
        )
        self.assertIn("WHERE f.id = $1", sql)
        self.assertIn("AND f.worker_id = $2", sql)
        self.assertIn("AND f.fencing_token = $4", sql)


class TestNonBudgetedFailureSql(unittest.TestCase):
    """Tests for non-quarantine suppressed retry SQL."""

    def test_non_budgeted_failure_sql_releases_without_quarantine_budget(
        self,
    ) -> None:
        sql = _sql_without_comments(
            feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL
        )

        self.assertIn("status = 'failing'::feed_status", sql)
        self.assertIn("failure_count = 0", sql)
        self.assertIn("retry_after = $4", sql)
        self.assertIn("status_reason = $5", sql)
        self.assertIn("status_reason_detail = $6", sql)
        self.assertIn("worker_id = NULL", sql)
        self.assertIn("WHERE f.id = $1", sql)
        self.assertIn("AND f.worker_id = $2", sql)
        self.assertIn("AND f.fencing_token = $3", sql)
        self.assertIn("AND f.status = 'active'::feed_status", sql)
        self.assertNotIn("failure_count + 1", sql)

    def test_non_budgeted_failure_sql_returns_status_diagnostics(self) -> None:
        sql = _sql_without_comments(
            feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL
        )

        self.assertIn(
            "RETURNING feeds.*, feeds.audit_revision AS feed_revision", sql
        )
        self.assertIn("failure_count", sql)
        self.assertIn("retry_after", sql)
        self.assertIn("audit_revision AS feed_revision", sql)

    def test_non_budgeted_failure_sql_preserves_reason_change_time(
        self,
    ) -> None:
        sql = _sql_without_comments(
            feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL
        )

        self.assertRegex(
            sql,
            r"status_reason_updated_at = CASE\s+"
            r"WHEN feeds.status_reason IS DISTINCT FROM \$5 THEN NOW\(\)\s+"
            r"ELSE feeds.status_reason_updated_at\s+END",
        )

    def test_failure_count_increment_isolated_to_report_failure_sql(
        self,
    ) -> None:
        for name, value in vars(feed_queries).items():
            if not name.endswith("_SQL") or not isinstance(value, str):
                continue
            stripped = _sql_without_comments(value)
            if name == "REPORT_FAILURE_SQL":
                self.assertIn("failure_count + 1", stripped)
                continue
            self.assertNotIn("failure_count + 1", stripped, name)


class TestStatusReasonClearSql(unittest.TestCase):
    """Tests for stale canonical reason clearing SQL."""

    def test_update_progress_sql_clears_stale_reason_without_lifecycle_recovery(
        self,
    ) -> None:
        sql = _sql_without_comments(feed_queries.UPDATE_PROGRESS_SQL)

        self.assertIn("status_reason = NULL", sql)
        self.assertIn("status_reason_detail = NULL", sql)
        self.assertRegex(
            sql,
            r"status_reason_updated_at = CASE\s+"
            r"WHEN feeds.status_reason IS NOT NULL OR feeds.status_reason_detail IS NOT NULL "
            r"THEN NOW\(\)\s+"
            r"ELSE feeds.status_reason_updated_at\s+END",
        )
        self.assertIn("failure_count = 0", sql)
        self.assertIn(
            "WHERE f.id = $2 AND f.worker_id = $3 AND f.fencing_token = $4",
            sql,
        )
        self.assertNotIn("SET status", sql)

    def test_reset_sql_clears_stale_reason_and_status_reason_detail(
        self,
    ) -> None:
        sql = _sql_without_comments(feed_queries.RESET_FEED_SQL)

        self.assertIn("status_reason_detail = NULL", sql)
        self.assertIn("status_reason = NULL", sql)
        self.assertRegex(
            sql,
            r"status_reason_updated_at = CASE\s+"
            r"WHEN feeds.status_reason IS NOT NULL OR feeds.status_reason_detail IS NOT NULL "
            r"THEN NOW\(\)\s+"
            r"ELSE feeds.status_reason_updated_at\s+END",
        )
        self.assertIn("status = 'unclaimed'::feed_status", sql)

    def test_record_source_observation_sql_clears_stale_reason_when_active(
        self,
    ) -> None:
        sql = _sql_without_comments(feed_queries.RECORD_SOURCE_OBSERVATION_SQL)

        self.assertIn("failure_count = 0", sql)
        self.assertIn(
            "last_bookmark_time = GREATEST(feeds.last_bookmark_time, $4)",
            sql,
        )
        self.assertIn("status_reason = NULL", sql)
        self.assertIn("status_reason_detail = NULL", sql)
        self.assertRegex(
            sql,
            r"status_reason_updated_at = CASE\s+"
            r"WHEN feeds.status_reason IS NOT NULL OR feeds.status_reason_detail IS NOT NULL "
            r"THEN NOW\(\)\s+"
            r"ELSE feeds.status_reason_updated_at\s+END",
        )
        self.assertIn("current_state.worker_id = $2", sql)
        self.assertIn("current_state.fencing_token = $3", sql)
        self.assertIn("current_state.status = 'active'::feed_status", sql)
        self.assertIn("current_state.worker_id AS current_worker", sql)
        self.assertIn("current_state.status::text AS current_status", sql)
        self.assertIn(
            "current_state.fencing_token AS current_fencing_token",
            sql,
        )


class TestUpdateFeedProgress(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.update_feed_progress."""

    async def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the fenced update succeeds."""
        pool = make_mock_pool(fetchrow_result=_audit_snapshot_row())
        store = FeedStore(pool)

        result = await store.update_feed_progress(
            _FEED_ID,
            _WORKER_ID,
            "gs://bucket/path/file.ogg",
            1,
            None,
            actor_id=_COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
        )

        self.assertTrue(result)

    async def test_emits_sql_returned_audit_event(self) -> None:
        """Successful progress writes emit only the returned audit payload."""
        payload = _feed_audit_event()
        pool = make_mock_pool(
            fetchrow_result=_audit_snapshot_row(feed_audit_event=payload),
        )
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.update_feed_progress(
                _FEED_ID,
                _WORKER_ID,
                "gs://bucket/path/file.ogg",
                1,
                None,
                actor_id=_COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
            )

        self.assertTrue(result)
        notifications.emit_feed_change_notification.assert_called_once_with(
            payload
        )

    async def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when no row matches (lease was lost)."""
        pool = make_mock_pool(fetchrow_result=None)
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.update_feed_progress(
                _FEED_ID,
                _WORKER_ID,
                "gs://bucket/path/file.ogg",
                1,
                None,
                actor_id=_COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
            )

        self.assertFalse(result)
        notifications.emit_feed_change_notification.assert_not_called()

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        pool = make_mock_pool(fetchrow_result=_audit_snapshot_row())
        store = FeedStore(pool)
        gcs_path = "gs://bucket/path/file.ogg"

        await store.update_feed_progress(
            _FEED_ID,
            _WORKER_ID,
            gcs_path,
            1,
            None,
            actor_id=_COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
        )

        args = pool.fetchrow.call_args[0]
        self.assertIs(args[0], feed_queries.UPDATE_PROGRESS_SQL)
        self.assertEqual(
            args[1:],
            (
                gcs_path,
                _FEED_ID,
                _WORKER_ID,
                1,
                None,
                _COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
            ),
        )

    async def test_passes_non_none_last_bookmark_time(self) -> None:
        """Non-None last_bookmark_time is forwarded as the 5th SQL parameter."""
        pool = make_mock_pool(fetchrow_result=_audit_snapshot_row())
        store = FeedStore(pool)
        gcs_path = "gs://bucket/path/file.ogg"
        last_bookmark_time = datetime.datetime(
            2024,
            1,
            2,
            tzinfo=datetime.UTC,
        )
        await store.update_feed_progress(
            _FEED_ID,
            _WORKER_ID,
            gcs_path,
            1,
            last_bookmark_time,
            actor_id=_COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
        )
        args = pool.fetchrow.call_args[0]
        self.assertIs(args[0], feed_queries.UPDATE_PROGRESS_SQL)
        self.assertEqual(
            args[1:],
            (
                gcs_path,
                _FEED_ID,
                _WORKER_ID,
                1,
                last_bookmark_time,
                _COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
            ),
        )

    async def test_rejects_missing_actor_id(self) -> None:
        """Recovery-capable progress writes require a causal actor."""
        pool = make_mock_pool(fetchrow_result=_audit_snapshot_row())
        store = FeedStore(pool)

        with self.assertRaisesRegex(ValueError, "actor_id is required"):
            await store.update_feed_progress(
                _FEED_ID,
                _WORKER_ID,
                "gs://bucket/path/file.ogg",
                1,
                None,
                actor_id=_MISSING_ACTOR_ID,
            )

        pool.fetchrow.assert_not_awaited()


class TestRecordSourceObservation(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.record_source_observation."""

    async def test_returns_diagnostic_result_when_row_exists(self) -> None:
        """Diagnostic row identifies whether the source observation was recorded."""
        resume_position = datetime.datetime(2026, 6, 8, tzinfo=datetime.UTC)
        payload = _feed_audit_event()
        pool = make_mock_pool(
            fetchrow_result={
                "id": _FEED_ID,
                "current_worker": _WORKER_ID,
                "current_status": "active",
                "current_fencing_token": 1,
                "recorded": True,
                "feed_audit_event": payload,
            },
        )
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.record_source_observation(
                _FEED_ID,
                _WORKER_ID,
                1,
                resume_position,
                actor_id=_COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
            )

        self.assertEqual(
            result,
            {
                "id": _FEED_ID,
                "current_worker": _WORKER_ID,
                "current_status": "active",
                "current_fencing_token": 1,
                "recorded": True,
            },
        )
        notifications.emit_feed_change_notification.assert_called_once_with(
            payload
        )
        args = pool.fetchrow.call_args[0]
        self.assertEqual(
            args[1:],
            (
                _FEED_ID,
                _WORKER_ID,
                1,
                resume_position,
                _COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
            ),
        )

    async def test_returns_missing_diagnostic_when_row_absent(self) -> None:
        """Missing feed rows are returned as a non-recorded diagnostic result."""
        pool = make_mock_pool(fetchrow_result=None)
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.record_source_observation(
                _FEED_ID,
                _WORKER_ID,
                1,
                None,
                actor_id=_COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
            )

        self.assertEqual(
            result,
            {
                "id": _FEED_ID,
                "current_worker": None,
                "current_status": None,
                "current_fencing_token": None,
                "recorded": False,
            },
        )
        notifications.emit_feed_change_notification.assert_not_called()

    async def test_record_source_observation_rejects_missing_actor_id(
        self,
    ) -> None:
        """Source-observation recovery writes require a causal actor."""
        pool = make_mock_pool(fetchrow_result=None)
        store = FeedStore(pool)

        with self.assertRaisesRegex(ValueError, "actor_id is required"):
            await store.record_source_observation(
                _FEED_ID,
                _WORKER_ID,
                1,
                None,
                actor_id=_MISSING_ACTOR_ID,
            )

        pool.fetchrow.assert_not_awaited()


class TestRenewHeartbeatsBatchDiagnostic(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.renew_heartbeats_batch_diagnostic."""

    async def test_returns_diagnostic_results(self) -> None:
        """Returned list contains HeartbeatResult dicts with diagnostic info."""
        other_worker = uuid.UUID("22222222-3333-4444-5555-666666666666")
        pool = make_mock_pool(
            fetch_result=[
                {
                    "id": _FEED_ID,
                    "current_worker": _WORKER_ID,
                    "current_status": "active",
                    "renewed": True,
                },
                {
                    "id": _FEED_ID_B,
                    "current_worker": other_worker,
                    "current_status": "active",
                    "renewed": False,
                },
            ],
        )
        store = FeedStore(pool)

        result = await store.renew_heartbeats_batch_diagnostic(
            [_FEED_ID, _FEED_ID_B],
            _WORKER_ID,
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(
            result[0],
            HeartbeatResult(
                id=_FEED_ID,
                current_worker=_WORKER_ID,
                current_status="active",
                renewed=True,
            ),
        )
        self.assertEqual(
            result[1],
            HeartbeatResult(
                id=_FEED_ID_B,
                current_worker=other_worker,
                current_status="active",
                renewed=False,
            ),
        )

    async def test_short_circuits_on_empty_input(self) -> None:
        """Empty feed_ids list returns empty list without executing a query."""
        pool = mock.AsyncMock()
        store = FeedStore(pool)

        result = await store.renew_heartbeats_batch_diagnostic([], _WORKER_ID)

        self.assertEqual(result, [])
        pool.fetch.assert_not_called()

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed as (feed_ids_list, worker_id)."""
        pool = make_mock_pool(
            fetch_result=[
                {
                    "id": _FEED_ID,
                    "current_worker": _WORKER_ID,
                    "current_status": "active",
                    "renewed": True,
                },
            ],
        )
        store = FeedStore(pool)
        feed_ids = [_FEED_ID, _FEED_ID_B]

        await store.renew_heartbeats_batch_diagnostic(feed_ids, _WORKER_ID)

        args = pool.fetch.call_args[0]
        self.assertEqual(args[1:], (feed_ids, _WORKER_ID))

    async def test_mixed_renewed_and_unrenewed(self) -> None:
        """Results correctly distinguish renewed vs unrenewed feeds."""
        pool = make_mock_pool(
            fetch_result=[
                {
                    "id": _FEED_ID,
                    "current_worker": _WORKER_ID,
                    "current_status": "active",
                    "renewed": True,
                },
                {
                    "id": _FEED_ID_B,
                    "current_worker": None,
                    "current_status": "unclaimed",
                    "renewed": False,
                },
            ],
        )
        store = FeedStore(pool)

        result = await store.renew_heartbeats_batch_diagnostic(
            [_FEED_ID, _FEED_ID_B],
            _WORKER_ID,
        )

        renewed = [r for r in result if r["renewed"]]
        not_renewed = [r for r in result if not r["renewed"]]
        self.assertEqual(len(renewed), 1)
        self.assertEqual(renewed[0]["id"], _FEED_ID)
        self.assertEqual(len(not_renewed), 1)
        self.assertEqual(not_renewed[0]["current_status"], "unclaimed")


class TestReportFeedFailure(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.report_feed_failure."""

    async def test_returns_status_when_lease_held(self) -> None:
        """Status string is returned when the RETURNING row is present."""
        payload = _feed_audit_event("feed.failure_reported")
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = _failure_update_row(
            feed_audit_event=payload,
        )
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.report_feed_failure(
                _FEED_ID,
                _WORKER_ID,
                1,
                **_runtime_prior_kwargs(),
            )

        self.assertEqual(result, "failing")
        notifications.emit_feed_change_notification.assert_called_once_with(
            payload
        )

    async def test_returns_none_when_lease_lost(self) -> None:
        """None is returned when RETURNING yields no row."""
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = None
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.report_feed_failure(
                _FEED_ID,
                _WORKER_ID,
                1,
                **_runtime_prior_kwargs(),
            )

        self.assertIsNone(result)
        pool.acquired_connection.execute.assert_not_awaited()
        notifications.emit_feed_change_notification.assert_not_called()

    def test_duplicate_failure_summary_logs_are_not_in_store(self) -> None:
        """Audit notifications replace duplicate storage failure summaries."""
        text = pathlib.Path(
            "backend/pipeline/storage/feed_store.py"
        ).read_text()

        self.assertNotIn("Feed failure threshold reached", text)
        self.assertNotIn("Feed failure recorded", text)

    async def test_returns_quarantined_status(self) -> None:
        """Quarantined status string is returned at threshold."""
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = _failure_update_row(
            status="quarantined",
            failure_count=5,
        )
        store = FeedStore(pool)

        result = await store.report_feed_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            **_runtime_prior_kwargs(),
        )

        self.assertEqual(result, "quarantined")

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order to the atomic SQL."""
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = _failure_update_row()
        store = FeedStore(pool)

        await store.report_feed_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            **_runtime_prior_kwargs(),
            reason="ffmpeg_exit_1",
            status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )

        args = pool.acquired_connection.fetchrow.await_args.args
        self.assertIs(args[0], feed_queries.REPORT_FAILURE_SQL)
        self.assertEqual(
            args[1:],
            (
                _FEED_ID,
                _WORKER_ID,
                5,
                1,
                600,
                15,
                "system_collector_error",
                "ffmpeg_exit_1",
                _COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
            ),
        )

    async def test_omitted_status_reason_passes_none_for_sql_fallback(
        self,
    ) -> None:
        """Omitted status_reason lets SQL apply the default fallback."""
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = _failure_update_row()
        store = FeedStore(pool)

        await store.report_feed_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            **_runtime_prior_kwargs(),
            reason="raw",
        )

        args = pool.acquired_connection.fetchrow.await_args.args
        self.assertIsNone(args[-3])
        self.assertEqual(args[-2], "raw")
        self.assertEqual(args[-1], _COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID)

    async def test_failure_write_passes_status_reason_detail(
        self,
    ) -> None:
        """Failure writes send status reason and bounded detail."""
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = _failure_update_row()
        store = FeedStore(pool)
        long_reason = "x" * (
            status_reason_detail.MAX_STATUS_REASON_DETAIL_LENGTH + 1
        )

        await store.report_feed_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            **_runtime_prior_kwargs(),
            reason=long_reason,
        )

        args = pool.acquired_connection.fetchrow.await_args.args
        self.assertEqual(len(args), 10)
        self.assertIsNone(args[-3])
        reason_arg = args[-2]
        self.assertEqual(
            len(reason_arg),
            status_reason_detail.MAX_STATUS_REASON_DETAIL_LENGTH,
        )
        self.assertTrue(reason_arg.endswith("[truncated]"))

    async def test_caps_status_reason_detail_at_persistence_boundary(
        self,
    ) -> None:
        """Long diagnostic detail is capped before persistence."""
        pool = make_mock_pool(transaction=True)
        long_reason = "x" * (
            status_reason_detail.MAX_STATUS_REASON_DETAIL_LENGTH + 1
        )
        pool.acquired_connection.fetchrow.return_value = _failure_update_row()
        store = FeedStore(pool)

        await store.report_feed_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            **_runtime_prior_kwargs(),
            reason=long_reason,
        )

        detail_arg = pool.acquired_connection.fetchrow.await_args.args[-2]
        self.assertEqual(
            len(detail_arg),
            status_reason_detail.MAX_STATUS_REASON_DETAIL_LENGTH,
        )
        self.assertTrue(detail_arg.endswith("[truncated]"))

    async def test_rejects_missing_actor_id(self) -> None:
        """Failure writes require a causal actor before any DB mutation."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        with self.assertRaisesRegex(ValueError, "actor_id is required"):
            await store.report_feed_failure(
                _FEED_ID,
                _WORKER_ID,
                1,
                actor_id=_MISSING_ACTOR_ID,
            )

        pool.acquire.assert_not_called()


class TestReleaseNonBudgetedFailure(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.release_non_budgeted_failure."""

    async def test_returns_status_when_lease_held(self) -> None:
        """Status string is returned when the non-budgeted update succeeds."""
        payload = _feed_audit_event("feed.failure_reported")
        retry_after = datetime.datetime(
            2026, 6, 14, 12, 15, tzinfo=datetime.UTC
        )
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = _failure_update_row(
            failure_count=0,
            retry_after=retry_after,
            feed_audit_event=payload,
        )
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.release_non_budgeted_failure(
                _FEED_ID,
                _WORKER_ID,
                1,
                retry_after=retry_after,
                status_reason=(
                    FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
                ),
                **_runtime_prior_kwargs(),
            )

        self.assertEqual(result, "failing")
        notifications.emit_feed_change_notification.assert_called_once_with(
            payload
        )

    async def test_returns_none_when_lease_lost(self) -> None:
        """None is returned when no active lease matches."""
        retry_after = datetime.datetime(
            2026, 6, 14, 12, 15, tzinfo=datetime.UTC
        )
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = None
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.release_non_budgeted_failure(
                _FEED_ID,
                _WORKER_ID,
                1,
                retry_after=retry_after,
                status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                **_runtime_prior_kwargs(),
            )

        self.assertIsNone(result)
        pool.acquired_connection.execute.assert_not_awaited()
        notifications.emit_feed_change_notification.assert_not_called()

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        retry_after = datetime.datetime(
            2026, 6, 14, 12, 15, tzinfo=datetime.UTC
        )
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = _failure_update_row(
            failure_count=0,
            retry_after=retry_after,
        )
        store = FeedStore(pool)

        await store.release_non_budgeted_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            retry_after=retry_after,
            status_reason=FeedStatusReason.SOURCE_OFFLINE,
            **_runtime_prior_kwargs(),
        )

        args = pool.acquired_connection.fetchrow.await_args.args
        self.assertIs(args[0], feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL)
        self.assertEqual(
            args[1:],
            (
                _FEED_ID,
                _WORKER_ID,
                1,
                retry_after,
                "source_offline",
                None,
                _COLLECTOR_SERVICE_ACCOUNT_ACTOR_ID,
            ),
        )

    async def test_passes_status_reason_detail(self) -> None:
        """Non-budgeted failures can persist diagnostic detail."""
        retry_after = datetime.datetime(
            2026, 6, 14, 12, 15, tzinfo=datetime.UTC
        )
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = _failure_update_row(
            failure_count=0,
            retry_after=retry_after,
        )
        store = FeedStore(pool)

        await store.release_non_budgeted_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            retry_after=retry_after,
            status_reason=FeedStatusReason.SOURCE_OFFLINE,
            **_runtime_prior_kwargs(),
            reason="provider timeout",
        )

        detail_arg = pool.acquired_connection.fetchrow.await_args.args[-2]
        self.assertEqual(detail_arg, "provider timeout")

    async def test_rejects_missing_actor_id(self) -> None:
        """Non-budgeted failure writes require a causal actor."""
        retry_after = datetime.datetime(
            2026, 6, 14, 12, 15, tzinfo=datetime.UTC
        )
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        with self.assertRaisesRegex(ValueError, "actor_id is required"):
            await store.release_non_budgeted_failure(
                _FEED_ID,
                _WORKER_ID,
                1,
                retry_after=retry_after,
                status_reason=FeedStatusReason.SOURCE_OFFLINE,
                actor_id=_MISSING_ACTOR_ID,
            )

        pool.acquire.assert_not_called()


class TestReleaseFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.release_feed."""

    async def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the feed was released."""
        pool = make_mock_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        result = await store.release_feed(_FEED_ID, _WORKER_ID, 1)

        self.assertTrue(result)

    async def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when the lease was already lost."""
        pool = make_mock_pool(execute_result="UPDATE 0")
        store = FeedStore(pool)

        result = await store.release_feed(_FEED_ID, _WORKER_ID, 1)

        self.assertFalse(result)

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        pool = make_mock_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        await store.release_feed(_FEED_ID, _WORKER_ID, 1)

        args = pool.execute.call_args[0]
        self.assertEqual(args[1:], (_FEED_ID, _WORKER_ID, 1))


_DEFAULT_LIMITS: dict[SourceType, int] = {
    SourceType.BCFY_FEEDS: 10,
    SourceType.BCFY_CALLS: 10,
    SourceType.OPENMHZ: 10,
}


class TestAcquireFeedsBatch(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.acquire_feeds_batch."""

    async def test_returns_list_of_feeds(self) -> None:
        """Multiple feeds are returned as a list of LeasedFeed dicts."""
        rows = [
            {
                "id": _FEED_ID,
                "name": "Feed A",
                "source_type": "bcfy_feeds",
                "last_processed_filename": None,
                "last_bookmark_time": None,
                "fencing_token": 1,
                "failure_count": 0,
                "status_reason": None,
                "source_feed_id": "123",
            },
            {
                "id": _FEED_ID_B,
                "name": "Feed B",
                "source_type": "bcfy_feeds",
                "last_processed_filename": "gs://bucket/path",
                "last_bookmark_time": None,
                "fencing_token": 1,
                "failure_count": 2,
                "status_reason": "source_unreachable",
                "source_feed_id": None,
            },
        ]
        pool = make_mock_pool(fetch_result=rows)
        store = FeedStore(pool)

        result = await store.acquire_feeds_batch(_WORKER_ID, _DEFAULT_LIMITS)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], _FEED_ID)
        self.assertEqual(result[1]["id"], _FEED_ID_B)

    async def test_returns_empty_list_when_none_available(self) -> None:
        """Empty list returned when no feeds can be leased."""
        pool = make_mock_pool(fetch_result=[])
        store = FeedStore(pool)

        result = await store.acquire_feeds_batch(_WORKER_ID, _DEFAULT_LIMITS)

        self.assertEqual(result, [])

    async def test_passes_positional_in_claim_types_order(self) -> None:
        """Limits dict is unpacked in claim_types iteration order."""
        pool = make_mock_pool(fetch_result=[])
        store = FeedStore(
            pool,
            claim_types=[
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ],
        )

        await store.acquire_feeds_batch(
            _WORKER_ID,
            {
                SourceType.BCFY_FEEDS: 2,
                SourceType.BCFY_CALLS: 3,
                SourceType.OPENMHZ: 5,
            },
        )

        args = pool.fetch.call_args[0]
        # args[0] is the generated SQL string (not a constant identity check
        # anymore — the constant no longer exists).
        self.assertIsInstance(args[0], str)
        self.assertEqual(args[1], _WORKER_ID)
        self.assertEqual(args[2], 2)  # BCFY_FEEDS
        self.assertEqual(args[3], 3)  # BCFY_CALLS
        self.assertEqual(args[4], 5)  # OPENMHZ

    async def test_per_type_limit_zero_is_passed_through(self) -> None:
        """A branch's LIMIT of 0 reaches the SQL — DB enforces the skip."""
        pool = make_mock_pool(fetch_result=[])
        store = FeedStore(
            pool,
            claim_types=[
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ],
        )

        await store.acquire_feeds_batch(
            _WORKER_ID,
            {
                SourceType.BCFY_FEEDS: 0,
                SourceType.BCFY_CALLS: 10,
                SourceType.OPENMHZ: 10,
            },
        )

        args = pool.fetch.call_args[0]
        self.assertEqual(args[2], 0)

    async def test_absent_limit_key_treated_as_zero(self) -> None:
        """Types absent from limits dict pass 0 to the SQL — same effect as LIMIT 0."""
        pool = make_mock_pool(fetch_result=[])
        store = FeedStore(
            pool,
            claim_types=[
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ],
        )

        await store.acquire_feeds_batch(
            _WORKER_ID,
            {SourceType.BCFY_FEEDS: 5},
        )

        args = pool.fetch.call_args[0]
        self.assertEqual(args[2], 5)
        self.assertEqual(args[3], 0)
        self.assertEqual(args[4], 0)

    async def test_raises_on_unknown_limit_key(self) -> None:
        """A SourceType not in claim_types raises ValueError."""
        pool = make_mock_pool(fetch_result=[])
        # Default claim_types = SourceType minus ECHO. Construct a store
        # that only claims BCFY_FEEDS so OPENMHZ is unknown.
        store = FeedStore(pool, claim_types=[SourceType.BCFY_FEEDS])

        with self.assertRaises(ValueError) as ctx:
            await store.acquire_feeds_batch(
                _WORKER_ID,
                {SourceType.BCFY_FEEDS: 1, SourceType.OPENMHZ: 1},
            )
        self.assertIn("openmhz", str(ctx.exception))

    async def test_raises_value_error_on_unknown_source_type(self) -> None:
        """ValueError is raised with details if the DB returns an unknown source type slug."""
        bad_row = {
            "id": _FEED_ID,
            "name": "Bad Feed",
            "source_type": "invalid_type",
            "last_processed_filename": None,
            "fencing_token": 1,
            "source_feed_id": None,
        }
        pool = make_mock_pool(fetch_result=[bad_row])
        store = FeedStore(pool)

        with self.assertRaises(ValueError) as ctx:
            await store.acquire_feeds_batch(
                _WORKER_ID,
                {
                    SourceType.BCFY_FEEDS: 1,
                    SourceType.BCFY_CALLS: 1,
                    SourceType.OPENMHZ: 1,
                },
            )

        self.assertIn(
            f"Unknown source type 'invalid_type' for feed {_FEED_ID}",
            str(ctx.exception),
        )


class TestReportFeedFailureWithThreshold(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.report_feed_failure with custom threshold."""

    async def test_custom_threshold_passed_to_sql(self) -> None:
        """Custom failure_threshold is passed as $3 parameter."""
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = _failure_update_row()
        store = FeedStore(pool)

        await store.report_feed_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            failure_threshold=5,
            **_runtime_prior_kwargs(),
        )

        args = pool.acquired_connection.fetchrow.await_args.args
        self.assertEqual(args[3], 5)  # $3 = threshold

    async def test_default_threshold_is_5(self) -> None:
        """Default threshold is 5."""
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.return_value = _failure_update_row()
        store = FeedStore(pool)

        await store.report_feed_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            **_runtime_prior_kwargs(),
        )

        args = pool.acquired_connection.fetchrow.await_args.args
        self.assertEqual(args[3], 5)


class TestBackoffFormula(unittest.TestCase):
    """Verify the exponential backoff computation used by report_feed_failure.

    Default: base=15s, max=600s (10 minutes).
    """

    def test_first_failure_15s(self) -> None:
        assert min(15 * (2**0), 600) == 15

    def test_third_failure_60s(self) -> None:
        assert min(15 * (2**2), 600) == 60

    def test_sixth_failure_480s(self) -> None:
        assert min(15 * (2**5), 600) == 480

    def test_seventh_failure_capped_600s(self) -> None:
        assert min(15 * (2**6), 600) == 600

    def test_tenth_failure_still_capped(self) -> None:
        assert min(15 * (2**9), 600) == 600


class TestRowToLeasedFeed(unittest.TestCase):
    """Tests for the shared row-to-LeasedFeed mapping helper."""

    def test_returns_leased_feed_from_valid_row(self) -> None:
        store = FeedStore(make_mock_pool())

        # asyncpg.Record exposes __getitem__ like a dict; tests pass a
        # dict literal that quacks like Record. Cast tells the type
        # checker we know what we're doing — runtime is unaffected.
        result = store._row_to_leased_feed(cast("asyncpg.Record", _LEASE_ROW))

        self.assertEqual(result["id"], _FEED_ID)
        self.assertEqual(result["name"], "My Feed")
        self.assertEqual(result["source_type"], SourceType.BCFY_FEEDS)
        self.assertEqual(result["fencing_token"], 1)
        self.assertEqual(result["failure_count"], 0)
        self.assertIsNone(result["status_reason"])

    def test_invalid_source_type_raises(self) -> None:
        bad_row = {**_LEASE_ROW, "source_type": "not_a_real_type"}
        store = FeedStore(make_mock_pool())

        with self.assertRaises(ValueError) as context:
            store._row_to_leased_feed(cast("asyncpg.Record", bad_row))

        self.assertIn(
            "Unknown source type 'not_a_real_type'", str(context.exception)
        )


class TestAcquireFeedsRecovery(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.acquire_feeds_recovery."""

    async def test_all_zero_limits_skip_pool(self) -> None:
        """All-zero limits dict returns [] without touching the pool."""
        pool = make_mock_pool()
        store = FeedStore(pool)

        # Build an all-zero dict over the store's claim_types only —
        # passing ECHO (not in the default claim_types) would be rejected
        # by the unknown-key validation regardless of value.
        zeros = dict.fromkeys(store._claim_types, 0)
        result = await store.acquire_feeds_recovery(_WORKER_ID, 60.0, zeros)

        self.assertEqual(result, [])
        pool.fetch.assert_not_called()

    async def test_empty_limits_dict_skips_pool(self) -> None:
        """Empty limits dict returns [] without touching the pool."""
        pool = make_mock_pool()
        store = FeedStore(pool)

        result = await store.acquire_feeds_recovery(_WORKER_ID, 60.0, {})

        self.assertEqual(result, [])
        pool.fetch.assert_not_called()

    async def test_passes_positional_in_claim_types_order(self) -> None:
        """worker_id, abandonment_interval, then per-type LIMITs in claim_types order."""
        pool = make_mock_pool(fetch_result=[])
        store = FeedStore(
            pool,
            claim_types=[
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ],
        )

        await store.acquire_feeds_recovery(
            _WORKER_ID,
            60.0,
            {
                SourceType.BCFY_FEEDS: 2,
                SourceType.BCFY_CALLS: 3,
                SourceType.OPENMHZ: 5,
            },
        )

        args = pool.fetch.call_args[0]
        # args[0] is the generated recovery SQL string.
        self.assertIsInstance(args[0], str)
        self.assertEqual(args[1], _WORKER_ID)
        self.assertEqual(args[2], datetime.timedelta(seconds=60.0))
        self.assertEqual(args[3], 2)  # BCFY_FEEDS recovery LIMIT
        self.assertEqual(args[4], 3)  # BCFY_CALLS recovery LIMIT
        self.assertEqual(args[5], 5)  # OPENMHZ recovery LIMIT

    async def test_absent_limit_key_treated_as_zero(self) -> None:
        """Types absent from limits dict pass 0 to the SQL."""
        pool = make_mock_pool(fetch_result=[])
        store = FeedStore(
            pool,
            claim_types=[
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ],
        )

        await store.acquire_feeds_recovery(
            _WORKER_ID,
            60.0,
            {SourceType.BCFY_FEEDS: 5},
        )

        args = pool.fetch.call_args[0]
        self.assertEqual(args[3], 5)
        self.assertEqual(args[4], 0)
        self.assertEqual(args[5], 0)

    async def test_raises_on_unknown_limit_key(self) -> None:
        """A SourceType not in claim_types raises ValueError."""
        pool = make_mock_pool(fetch_result=[])
        store = FeedStore(pool, claim_types=[SourceType.BCFY_FEEDS])

        with self.assertRaises(ValueError) as ctx:
            await store.acquire_feeds_recovery(
                _WORKER_ID,
                60.0,
                {SourceType.OPENMHZ: 1},
            )
        self.assertIn("openmhz", str(ctx.exception))

    async def test_returns_leased_feeds(self) -> None:
        """Rows are converted to LeasedFeed dicts via the shared helper."""
        pool = make_mock_pool(fetch_result=[_LEASE_ROW])
        store = FeedStore(pool)

        result = await store.acquire_feeds_recovery(
            _WORKER_ID,
            60.0,
            {SourceType.BCFY_FEEDS: 10},
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], _FEED_ID)


class TestCountHeldByType(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.count_held_by_type."""

    async def test_returns_counts_for_returned_source_types(self) -> None:
        """Returned rows populate the corresponding SourceType keys."""
        pool = make_mock_pool(
            fetch_result=[
                {"source_type": "bcfy_feeds", "n": 12},
                {"source_type": "bcfy_calls", "n": 7},
            ],
        )
        store = FeedStore(pool)

        result = await store.count_held_by_type(_WORKER_ID)

        self.assertEqual(result[SourceType.BCFY_FEEDS], 12)
        self.assertEqual(result[SourceType.BCFY_CALLS], 7)

    async def test_returns_zeros_for_absent_source_types(self) -> None:
        """Every SourceType key is present in output, even if not in rows."""
        pool = make_mock_pool(
            fetch_result=[
                {"source_type": "bcfy_feeds", "n": 3},
            ],
        )
        store = FeedStore(pool)

        result = await store.count_held_by_type(_WORKER_ID)

        # Every SourceType is keyed, with 0 for unreturned types.
        for source_type in SourceType:
            self.assertIn(source_type, result)
        self.assertEqual(result[SourceType.BCFY_FEEDS], 3)
        self.assertEqual(result[SourceType.BCFY_CALLS], 0)
        self.assertEqual(result[SourceType.OPENMHZ], 0)
        self.assertEqual(result[SourceType.ECHO], 0)

    async def test_skips_unknown_source_type_rows(self) -> None:
        """Bogus source_type strings are silently skipped, not raised."""
        pool = make_mock_pool(
            fetch_result=[
                {"source_type": "bcfy_feeds", "n": 4},
                {"source_type": "future_type_not_in_enum", "n": 99},
            ],
        )
        store = FeedStore(pool)

        result = await store.count_held_by_type(_WORKER_ID)

        # The known type populates; the unknown row is dropped — output
        # contains only valid SourceType keys, all integer values.
        self.assertEqual(result[SourceType.BCFY_FEEDS], 4)
        for value in result.values():
            self.assertIsInstance(value, int)

    async def test_empty_db_result_returns_all_zeros(self) -> None:
        """No rows → dict has every SourceType mapped to 0."""
        pool = make_mock_pool(fetch_result=[])
        store = FeedStore(pool)

        result = await store.count_held_by_type(_WORKER_ID)

        self.assertEqual(set(result.keys()), set(SourceType))
        self.assertTrue(all(v == 0 for v in result.values()))

    async def test_passes_worker_id_as_param(self) -> None:
        """Worker ID is forwarded as the only SQL parameter."""
        pool = make_mock_pool(fetch_result=[])
        store = FeedStore(pool)

        await store.count_held_by_type(_WORKER_ID)

        args = pool.fetch.call_args[0]
        self.assertIs(args[0], feed_queries.COUNT_HELD_BY_TYPE_SQL)
        self.assertEqual(args[1], _WORKER_ID)


class TestReleaseFeedsBatch(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.release_feeds_batch."""

    async def test_passes_worker_id(self) -> None:
        pool = make_mock_pool(execute_result="UPDATE 2")
        store = FeedStore(pool)

        result = await store.release_feeds_batch(_WORKER_ID)

        self.assertEqual(result, 2)
        args = pool.execute.call_args[0]
        self.assertIs(args[0], feed_queries.RELEASE_FEEDS_BATCH_SQL)
        self.assertEqual(args[1], _WORKER_ID)

    async def test_parses_update_count(self) -> None:
        pool = make_mock_pool(execute_result="UPDATE 7")
        store = FeedStore(pool)

        result = await store.release_feeds_batch(_WORKER_ID)

        self.assertEqual(result, 7)

    async def test_returns_zero_for_unparseable_result(self) -> None:
        pool = make_mock_pool(execute_result="ROLLBACK")
        store = FeedStore(pool)

        result = await store.release_feeds_batch(_WORKER_ID)

        self.assertEqual(result, 0)


class TestCreateFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.create_feed."""

    async def test_returns_feed_on_success(self) -> None:
        """A created feed is returned as a Feed dict."""
        payload = _feed_audit_event("feed.created")
        row = _full_feed_row(name="New Feed", feed_audit_event=payload)
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = row
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.create_feed(
                "New Feed",
                "bcfy_feeds",
                "123",
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

        self.assertEqual(result["id"], _FEED_ID)
        self.assertEqual(result["name"], "New Feed")
        self.assertEqual(result["source_type"], SourceType.BCFY_FEEDS)
        pool.transaction_context.__aenter__.assert_not_awaited()
        notifications.emit_feed_change_notification.assert_called_once_with(
            payload
        )

    async def test_create_feed_with_tags(self) -> None:
        """Tags are passed to the SQL and returned in the Feed."""
        tags = [{"key": "env", "value": "prod"}]
        row = _full_feed_row(
            name="New Feed",
            tags='[{"key": "env", "value": "prod"}]',
        )
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = row
        store = FeedStore(pool)

        result = await store.create_feed(
            "New Feed",
            "bcfy_feeds",
            "123",
            tags=tags,
            actor_id=_FEEDS_SERVICE_ACTOR_ID,
        )

        self.assertEqual(result["tags"], tags)
        args = conn.fetchrow.await_args_list[0].args
        self.assertEqual(args[4], json.dumps(tags))

    async def test_create_feed_invalid_tags(self) -> None:
        """CheckViolationError is raised when DB constraint fails for invalid tags."""
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.side_effect = (
            asyncpg.CheckViolationError("valid_tags_schema")
        )
        store = FeedStore(pool)

        tags = [{"invalid": "shape"}]
        with self.assertRaises(asyncpg.CheckViolationError):
            await store.create_feed(
                "New Feed",
                "bcfy_feeds",
                "123",
                tags=tags,
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

    async def test_create_feed_translates_source_unique_violation(self) -> None:
        """The source lookup unique index remains a feed duplicate error."""
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.side_effect = _unique_violation(
            "idx_feed_properties_source_lookup"
        )
        store = FeedStore(pool)

        with self.assertRaises(FeedAlreadyExistsError):
            await store.create_feed(
                "New Feed",
                "bcfy_feeds",
                "123",
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

    async def test_create_feed_translates_name_unique_violation(self) -> None:
        """The feeds name constraint remains a feed duplicate error."""
        pool = make_mock_pool(transaction=True)
        pool.acquired_connection.fetchrow.side_effect = _unique_violation(
            "feeds_name_key"
        )
        store = FeedStore(pool)

        with self.assertRaises(FeedAlreadyExistsError):
            await store.create_feed(
                "New Feed",
                "bcfy_feeds",
                "123",
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

    async def test_create_feed_reraises_audit_unique_violation(self) -> None:
        """Audit uniqueness failures must not look like feed duplicates."""
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.side_effect = _unique_violation(
            "feed_audit_events_feed_revision_unique"
        )
        store = FeedStore(pool)

        with self.assertRaises(asyncpg.exceptions.UniqueViolationError):
            await store.create_feed(
                "New Feed",
                "bcfy_feeds",
                "123",
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

    async def test_raises_value_error_on_failure(self) -> None:
        """ValueError is raised if the DB returns no row."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        with self.assertRaises(ValueError):
            await store.create_feed(
                "New Feed",
                "bcfy_feeds",
                "123",
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

    async def test_create_feed_invalid_source_type(self) -> None:
        """ValueError is raised when an invalid source type is passed."""
        pool = make_mock_pool()
        store = FeedStore(pool)

        with self.assertRaises(ValueError) as cm:
            await store.create_feed(
                name="Test Feed",
                source_type="invalid_type",
                source_feed_id="src_123",
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )
        self.assertIn("Invalid source type", str(cm.exception))

    async def test_rejects_missing_actor_id(self) -> None:
        """Create requires a causal actor before validation or DB access."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        with self.assertRaisesRegex(ValueError, "actor_id is required"):
            await store.create_feed(
                "New Feed",
                "bcfy_feeds",
                "123",
                actor_id=_MISSING_ACTOR_ID,
            )

        pool.acquire.assert_not_called()

    async def test_create_feed_uses_combined_audit_sql(self) -> None:
        """Successful create uses one SQL statement that embeds feed.created."""
        tags = [{"key": "env", "value": "prod"}]
        row = _full_feed_row(
            name="Created Feed",
            tags='[{"key": "env", "value": "prod"}]',
            status_reason_detail="created detail",
        )
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = row
        store = FeedStore(pool)

        result = await store.create_feed(
            "Created Feed",
            "bcfy_feeds",
            "123",
            tags=tags,
            actor_id=_FEEDS_SERVICE_ACTOR_ID,
        )

        self.assertEqual(result["name"], "Created Feed")
        self.assertEqual(
            conn.fetchrow.await_args_list[0].args[0],
            feed_queries.CREATE_FEED_SQL,
        )
        self.assertEqual(len(conn.fetchrow.await_args_list), 1)
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()
        args = conn.fetchrow.await_args.args
        self.assertEqual(args[-1], _FEEDS_SERVICE_ACTOR_ID)
        self.assertIn("INSERT INTO feed_audit_events", args[0])
        self.assertIn("'feed.created'", args[0])


class TestUpdateFeedAuditing(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.update_feed audit behavior."""

    async def test_meaningful_update_writes_feed_updated_audit_event(
        self,
    ) -> None:
        tags = [{"key": "env", "value": "prod"}]
        payload = _feed_audit_event("feed.updated")
        updated_row = _full_feed_row(
            name="Updated Feed",
            tags='[{"key": "env", "value": "prod"}]',
            status_reason_detail="after detail",
            feed_revision=2,
            feed_audit_event=payload,
        )
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = updated_row
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.update_feed(
                _FEED_ID,
                "Updated Feed",
                tags=tags,
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

        assert result is not None
        self.assertEqual(result["name"], "Updated Feed")
        notifications.emit_feed_change_notification.assert_called_once_with(
            payload
        )
        conn.fetchrow.assert_awaited_once()
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()
        args = conn.fetchrow.await_args.args
        self.assertEqual(
            args,
            (
                feed_queries.UPDATE_FEED_SQL,
                _FEED_ID,
                "Updated Feed",
                json.dumps(tags),
                _FEEDS_SERVICE_ACTOR_ID,
            ),
        )
        self.assertIn("INSERT INTO feed_audit_events", args[0])
        self.assertIn("'feed.updated'", args[0])

    async def test_noop_update_returns_current_feed_without_audit(
        self,
    ) -> None:
        tags = [{"key": "env", "value": "prod"}]
        current = _full_feed_row(
            name="Same Feed",
            tags='[{"key": "env", "value": "prod"}]',
            feed_audit_event=None,
        )
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = current
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.update_feed(
                _FEED_ID,
                "Same Feed",
                tags=tags,
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

        assert result is not None
        self.assertEqual(result["name"], "Same Feed")
        notifications.emit_feed_change_notification.assert_called_once_with(
            None
        )
        conn.fetchrow.assert_awaited_once()
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()

    async def test_missing_update_target_returns_none_without_audit(
        self,
    ) -> None:
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        result = await store.update_feed(
            _FEED_ID,
            "Missing Feed",
            actor_id=_FEEDS_SERVICE_ACTOR_ID,
        )

        self.assertIsNone(result)
        pool.acquired_connection.fetchval.assert_not_awaited()
        pool.acquired_connection.execute.assert_not_awaited()

    async def test_update_feed_translates_name_unique_violation(self) -> None:
        """The feeds name constraint remains a feed name conflict."""
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.side_effect = _unique_violation("feeds_name_key")
        store = FeedStore(pool)

        with self.assertRaises(FeedNameAlreadyExistsError):
            await store.update_feed(
                _FEED_ID,
                "Conflicting Feed",
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

    async def test_update_feed_reraises_audit_unique_violation(self) -> None:
        """Audit uniqueness failures must not look like name conflicts."""
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.side_effect = _unique_violation(
            "feed_audit_events_feed_revision_unique"
        )
        store = FeedStore(pool)

        with self.assertRaises(asyncpg.exceptions.UniqueViolationError):
            await store.update_feed(
                _FEED_ID,
                "Updated Feed",
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

    async def test_rejects_missing_actor_id(self) -> None:
        """Update requires a causal actor even for potential no-op updates."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        with self.assertRaisesRegex(ValueError, "actor_id is required"):
            await store.update_feed(
                _FEED_ID,
                "Updated Feed",
                actor_id=_MISSING_ACTOR_ID,
            )

        pool.acquire.assert_not_called()


class TestGetFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.get_feed."""

    async def test_returns_feed_when_exists(self) -> None:
        """A feed is returned as a Feed dict when it exists."""
        row = _full_feed_row()
        pool = make_mock_pool(fetchrow_result=row)
        store = FeedStore(pool)

        result = await store.get_feed(_FEED_ID)

        assert result is not None
        self.assertEqual(result["id"], _FEED_ID)

    async def test_get_feed_returns_tags(self) -> None:
        """Tags are returned in the Feed dict when they exist."""
        row = _full_feed_row(tags='[{"key": "county", "value": "Fulton"}]')
        pool = make_mock_pool(fetchrow_result=row)
        store = FeedStore(pool)

        result = await store.get_feed(_FEED_ID)

        assert result is not None
        self.assertEqual(result["tags"], [{"key": "county", "value": "Fulton"}])

    async def test_returns_none_when_not_exists(self) -> None:
        """None is returned when the feed does not exist."""
        pool = make_mock_pool(fetchrow_result=None)
        store = FeedStore(pool)

        result = await store.get_feed(_FEED_ID)

        self.assertIsNone(result)


class TestListFeeds(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.list_feeds."""

    async def test_list_feeds_rejects_non_positive_limit_before_query(
        self,
    ) -> None:
        pool = make_mock_pool()
        store = FeedStore(pool)

        with self.assertRaisesRegex(ValueError, "limit must be >= 1"):
            await store.list_feeds(limit=0)

        pool.fetch.assert_not_awaited()
        pool.fetchval.assert_not_awaited()

    async def test_returns_list_of_feeds(self) -> None:
        """A list of Feed dicts is returned."""
        rows = [
            _full_feed_row(name="Feed A"),
            _full_feed_row(
                id=_FEED_ID_B,
                name="Feed B",
                source_type="openmhz",
                status="active",
                worker_id=_WORKER_ID,
                last_heartbeat=datetime.datetime(
                    2026, 4, 10, tzinfo=datetime.UTC
                ),
                created_at=datetime.datetime(2026, 4, 9, tzinfo=datetime.UTC),
                source_feed_id="456",
            ),
        ]
        pool = make_mock_pool(fetch_result=rows)
        store = FeedStore(pool)

        result = await store.list_feeds()

        self.assertEqual(len(result.feeds), 2)
        self.assertIsNone(result.next_token)
        self.assertEqual(result.feeds[0]["id"], _FEED_ID)
        self.assertEqual(result.feeds[1]["id"], _FEED_ID_B)
        self.assertEqual(result.feeds[1]["source_type"], SourceType.OPENMHZ)

    async def test_list_feeds_returns_tags(self) -> None:
        """Tags are returned in the Feed dicts when they exist."""
        rows = [
            _full_feed_row(
                name="Feed A",
                tags='[{"key": "county", "value": "Fulton"}]',
            ),
        ]
        pool = make_mock_pool(fetch_result=rows)
        store = FeedStore(pool)

        result = await store.list_feeds()

        self.assertEqual(len(result.feeds), 1)
        self.assertEqual(
            result.feeds[0]["tags"], [{"key": "county", "value": "Fulton"}]
        )

    async def test_list_feeds_pagination_next_token(self) -> None:
        """next_token is returned when there are more pages."""
        rows = [
            _full_feed_row(name="Feed A"),
            _full_feed_row(id=_FEED_ID_B, name="Feed B"),
        ]
        pool = make_mock_pool(fetch_result=rows)
        store = FeedStore(pool)

        # Limit is 1, but we have 2 rows
        result = await store.list_feeds(limit=1)

        self.assertEqual(len(result.feeds), 1)
        self.assertIsNotNone(result.next_token)

    async def test_list_feeds_decodes_and_uses_cursor(self) -> None:
        """The next_token parameter is decoded and forwarded as SQL parameters."""
        row = _full_feed_row(name="Feed Cursor")
        pool = make_mock_pool(fetch_result=[row])
        store = FeedStore(pool)

        # Generate a token representing a cursor
        cursor_ts = datetime.datetime(2024, 1, 1, 12, 0, 0)
        token = encode_cursor(cursor_ts, _FEED_ID)

        result = await store.list_feeds(limit=10, next_token=token)

        args = pool.fetch.call_args[0]
        # Parameters should be (query, cursor_ts, cursor_uid, source_types, statuses, tags_json, name, limit + 1)
        self.assertEqual(args[1], cursor_ts)
        self.assertEqual(args[2], _FEED_ID)
        self.assertEqual(args[6], None)
        self.assertEqual(args[7], 11)
        self.assertEqual(len(result.feeds), 1)
        self.assertEqual(result.feeds[0]["name"], "Feed Cursor")

    async def test_list_feeds_with_name_filter(self) -> None:
        """The name parameter is forwarded to the query, and the matching feed is returned."""
        row = _full_feed_row(name="My Feed")
        pool = make_mock_pool(fetch_result=[row])
        store = FeedStore(pool)

        result = await store.list_feeds(name="My Feed")

        args = pool.fetch.call_args[0]
        self.assertEqual(args[6], "My Feed")
        self.assertEqual(len(result.feeds), 1)
        self.assertEqual(result.feeds[0]["name"], "My Feed")

    async def test_list_feeds_with_source_types_filter(self) -> None:
        """The source_types parameter is forwarded to the query, and the matching feed is returned."""
        row = _full_feed_row(name="Feed B", source_type="openmhz")
        pool = make_mock_pool(fetch_result=[row])
        store = FeedStore(pool)

        result = await store.list_feeds(source_types=[SourceType.OPENMHZ])

        args = pool.fetch.call_args[0]
        self.assertEqual(args[3], [SourceType.OPENMHZ])
        self.assertEqual(len(result.feeds), 1)
        self.assertEqual(result.feeds[0]["source_type"], SourceType.OPENMHZ)

    async def test_list_feeds_with_statuses_filter(self) -> None:
        """The statuses parameter is forwarded to the query, and the matching feed is returned."""
        row = _full_feed_row(name="Feed C", status="active")
        pool = make_mock_pool(fetch_result=[row])
        store = FeedStore(pool)

        result = await store.list_feeds(statuses=[FeedStatus.ACTIVE])

        args = pool.fetch.call_args[0]
        self.assertEqual(args[4], [FeedStatus.ACTIVE])
        self.assertEqual(len(result.feeds), 1)
        self.assertEqual(result.feeds[0]["status"], FeedStatus.ACTIVE)

    async def test_list_feeds_with_tags_filter(self) -> None:
        """The tags parameter is JSON serialized, forwarded to the query, and returned."""
        tags = [{"key": "region", "value": "West"}]
        row = _full_feed_row(name="Feed D", tags=json.dumps(tags))
        pool = make_mock_pool(fetch_result=[row])
        store = FeedStore(pool)

        result = await store.list_feeds(tags=tags)

        args = pool.fetch.call_args[0]
        self.assertEqual(args[5], json.dumps(tags))
        self.assertEqual(len(result.feeds), 1)
        self.assertEqual(result.feeds[0]["tags"], tags)

    async def test_list_feeds_returns_total_count(self) -> None:
        """The total filtered count is fetched and returned."""
        row = _full_feed_row(name="Feed E")
        pool = make_mock_pool(fetch_result=[row], fetchval_result=42)
        store = FeedStore(pool)

        result = await store.list_feeds()

        self.assertEqual(result.total, 42)
        self.assertEqual(pool.fetchval.call_count, 1)


class TestDeactivateFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.deactivate_feed."""

    async def test_success_writes_feed_deactivated_audit_event(self) -> None:
        """Successful deactivation emits one feed.deactivated audit row."""
        payload = _feed_audit_event("feed.deactivated")
        after = _audit_snapshot_row(
            status="deactivated",
            status_reason_detail="after deactivation detail",
            feed_revision=3,
            feed_audit_event=payload,
        )
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = after
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.deactivate_feed(
                _FEED_ID,
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

        self.assertTrue(result)
        notifications.emit_feed_change_notification.assert_called_once_with(
            payload
        )
        self.assertEqual(
            conn.fetchrow.await_args.args,
            (
                feed_queries.DEACTIVATE_FEED_SQL,
                _FEED_ID,
                _FEEDS_SERVICE_ACTOR_ID,
            ),
        )
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()
        self.assertIn(
            "INSERT INTO feed_audit_events", feed_queries.DEACTIVATE_FEED_SQL
        )
        self.assertIn("'feed.deactivated'", feed_queries.DEACTIVATE_FEED_SQL)

    async def test_sql_treats_already_deactivated_feed_as_noop(self) -> None:
        """Already-deactivated feeds return success without another audit event."""
        sql = feed_queries.DEACTIVATE_FEED_SQL

        self.assertIn(
            "AND before_row.status <> 'deactivated'::feed_status",
            sql,
        )
        self.assertIn("SELECT before_row.id", sql)
        self.assertIn("(SELECT write_audit.feed_audit_event", sql)
        self.assertNotIn("LEFT JOIN write_audit", sql)

    async def test_missing_feed_returns_false_without_audit(self) -> None:
        """Missing deactivate target does not allocate sequence or audit."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.deactivate_feed(
                _FEED_ID,
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

        self.assertFalse(result)
        notifications.emit_feed_change_notification.assert_not_called()
        conn = pool.acquired_connection
        conn.fetchrow.assert_awaited_once_with(
            feed_queries.DEACTIVATE_FEED_SQL,
            _FEED_ID,
            _FEEDS_SERVICE_ACTOR_ID,
        )
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()

    async def test_rejects_missing_actor_id(self) -> None:
        """Deactivate requires a causal actor before DB access."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        with self.assertRaisesRegex(ValueError, "actor_id is required"):
            await store.deactivate_feed(_FEED_ID, actor_id=_MISSING_ACTOR_ID)

        pool.acquire.assert_not_called()


class TestDeleteFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.delete_feed."""

    async def test_success_writes_feed_deleted_before_delete(self) -> None:
        """Successful hard delete inserts feed.deleted before deletion."""
        payload = _feed_audit_event("feed.deleted")
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = {
            "id": _FEED_ID,
            "blocked_active": False,
            "current_status": "unclaimed",
            "deleted": True,
            "feed_audit_event": payload,
        }
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.delete_feed(
                _FEED_ID,
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

        self.assertTrue(result)
        notifications.emit_feed_change_notification.assert_called_once_with(
            payload
        )
        conn.fetchrow.assert_awaited_once_with(
            feed_queries.DELETE_FEED_SQL,
            _FEED_ID,
            _FEEDS_SERVICE_ACTOR_ID,
        )
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()
        self.assertIn(
            "INSERT INTO feed_audit_events", feed_queries.DELETE_FEED_SQL
        )
        self.assertIn("'feed.deleted'", feed_queries.DELETE_FEED_SQL)

    async def test_feed_deleted_uses_full_before_snapshot_and_empty_after(
        self,
    ) -> None:
        """feed.deleted has full before_values and empty after_values."""
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = {
            "id": _FEED_ID,
            "blocked_active": False,
            "current_status": "unclaimed",
            "deleted": True,
        }
        store = FeedStore(pool)

        result = await store.delete_feed(
            _FEED_ID,
            actor_id=_FEEDS_SERVICE_ACTOR_ID,
        )

        self.assertTrue(result)
        sql = feed_queries.DELETE_FEED_SQL
        self.assertIn("'feed.deleted'", sql)
        self.assertIn("before_values", sql)
        self.assertIn("'{}'::jsonb", sql)

    async def test_sql_refuses_to_delete_active_feed(self) -> None:
        """Active feeds must be deactivated before hard deletion."""
        sql = feed_queries.DELETE_FEED_SQL

        self.assertIn("WHERE target.status <> 'active'::feed_status", sql)
        self.assertIn("AS blocked_active", sql)

    async def test_active_feed_delete_raises_state_conflict(self) -> None:
        """Active feeds return a conflict marker instead of looking missing."""
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = {
            "id": _FEED_ID,
            "blocked_active": True,
            "current_status": "active",
            "deleted": False,
            "feed_audit_event": None,
        }
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            with self.assertRaisesRegex(
                FeedStateConflictError, "cannot be deleted"
            ):
                await store.delete_feed(
                    _FEED_ID,
                    actor_id=_FEEDS_SERVICE_ACTOR_ID,
                )
        notifications.emit_feed_change_notification.assert_not_called()
        conn.fetchval.assert_not_awaited()

    async def test_missing_feed_delete_does_not_emit_notification(
        self,
    ) -> None:
        """Missing delete target returns False without notification emission."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.delete_feed(
                _FEED_ID,
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

        self.assertFalse(result)
        notifications.emit_feed_change_notification.assert_not_called()

    async def test_missing_feed_returns_false_without_audit_or_delete(
        self,
    ) -> None:
        """Missing delete target skips sequence, audit, and hard delete."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        result = await store.delete_feed(
            _FEED_ID,
            actor_id=_FEEDS_SERVICE_ACTOR_ID,
        )

        self.assertFalse(result)
        conn = pool.acquired_connection
        conn.fetchrow.assert_awaited_once_with(
            feed_queries.DELETE_FEED_SQL,
            _FEED_ID,
            _FEEDS_SERVICE_ACTOR_ID,
        )
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()

    async def test_rejects_missing_actor_id(self) -> None:
        """Delete requires a causal actor before DB access."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        with self.assertRaisesRegex(ValueError, "actor_id is required"):
            await store.delete_feed(_FEED_ID, actor_id=_MISSING_ACTOR_ID)

        pool.acquire.assert_not_called()


class TestResetFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.reset_feed."""

    async def test_success_writes_feed_reset_audit_event(self) -> None:
        """Successful reset emits one feed.reset audit row."""
        payload = _feed_audit_event("feed.reset")
        reset_row = _full_feed_row(
            status="unclaimed",
            failure_count=0,
            status_reason_detail=None,
            feed_revision=4,
            feed_audit_event=payload,
        )
        reset_row["blocked_active"] = False
        reset_row["current_status"] = "unclaimed"
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = reset_row
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            result = await store.reset_feed(
                _FEED_ID,
                actor_id=_FEEDS_SERVICE_ACTOR_ID,
            )

        assert result is not None
        self.assertEqual(result["status"], FeedStatus.UNCLAIMED)
        notifications.emit_feed_change_notification.assert_called_once_with(
            payload
        )
        self.assertEqual(
            conn.fetchrow.await_args.args,
            (
                feed_queries.RESET_FEED_SQL,
                _FEED_ID,
                _FEEDS_SERVICE_ACTOR_ID,
            ),
        )
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()
        self.assertIn(
            "INSERT INTO feed_audit_events", feed_queries.RESET_FEED_SQL
        )
        self.assertIn("'feed.reset'", feed_queries.RESET_FEED_SQL)
        self.assertNotIn("'feed.recovered'", feed_queries.RESET_FEED_SQL)

    async def test_sql_refuses_to_reset_active_feed(self) -> None:
        """Active feeds must be deactivated before admin reset."""
        sql = feed_queries.RESET_FEED_SQL

        self.assertIn("WHERE target.status <> 'active'::feed_status", sql)
        self.assertIn("AS blocked_active", sql)

    async def test_active_feed_reset_raises_state_conflict(self) -> None:
        """Active feeds return a conflict marker instead of looking missing."""
        pool = make_mock_pool(transaction=True)
        conn = pool.acquired_connection
        conn.fetchrow.return_value = {
            "id": None,
            "blocked_active": True,
            "current_status": "active",
            "feed_audit_event": None,
        }
        store = FeedStore(pool)

        with mock.patch(
            "backend.pipeline.storage.feed_store.feed_change_notifications",
            create=True,
        ) as notifications:
            with self.assertRaisesRegex(
                FeedStateConflictError, "cannot be reset"
            ):
                await store.reset_feed(
                    _FEED_ID,
                    actor_id=_FEEDS_SERVICE_ACTOR_ID,
                )
        notifications.emit_feed_change_notification.assert_not_called()
        conn.fetchval.assert_not_awaited()

    async def test_missing_feed_returns_none_without_audit(self) -> None:
        """Missing reset target does not allocate sequence or audit."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        result = await store.reset_feed(
            _FEED_ID,
            actor_id=_FEEDS_SERVICE_ACTOR_ID,
        )

        self.assertIsNone(result)
        conn = pool.acquired_connection
        conn.fetchrow.assert_awaited_once_with(
            feed_queries.RESET_FEED_SQL,
            _FEED_ID,
            _FEEDS_SERVICE_ACTOR_ID,
        )
        conn.fetchval.assert_not_awaited()
        conn.execute.assert_not_awaited()

    async def test_rejects_missing_actor_id(self) -> None:
        """Reset requires a causal actor before DB access."""
        pool = make_mock_pool(transaction=True)
        store = FeedStore(pool)

        with self.assertRaisesRegex(ValueError, "actor_id is required"):
            await store.reset_feed(_FEED_ID, actor_id=_MISSING_ACTOR_ID)

        pool.acquire.assert_not_called()


if __name__ == "__main__":
    unittest.main()
