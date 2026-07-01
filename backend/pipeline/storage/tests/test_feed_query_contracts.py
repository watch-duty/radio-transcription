"""Pure SQL contract tests for feed lifecycle queries."""

from __future__ import annotations

import pathlib
import unittest

from backend.pipeline.common.feed_change_notifications_contract import (
    FeedChangeNotificationPayload,
)
from backend.pipeline.storage import (
    feed_audit_sql,
    feed_queries,
    sync_feed_queries,
)
from backend.pipeline.storage.feed_store import SourceType


def _sql_without_comments(text: str) -> str:
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("--")
    )


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


class TestFeedAuditEventSqlContract(unittest.TestCase):
    """Tests for the feed-local audit event insert contract."""

    def test_shared_failure_action_builder_renders_runtime_actions(
        self,
    ) -> None:
        sql = feed_audit_sql.failure_audit_action_cte()

        self.assertIn("audit_action AS", sql)
        self.assertIn("THEN 'feed.failure_reported'", sql)
        self.assertIn("THEN 'feed.quarantined'", sql)
        self.assertIn("before_row.status", sql)
        self.assertIn("after_row.status", sql)

    def test_shared_recovery_action_builder_renders_recovered_action(
        self,
    ) -> None:
        sql = feed_audit_sql.recovery_audit_action_cte(
            before_alias="current_state",
        )

        self.assertIn("audit_action AS", sql)
        self.assertIn("THEN 'feed.recovered'", sql)
        self.assertIn("current_state.status", sql)
        self.assertIn("after_row.status", sql)

    def test_shared_insert_builder_renders_canonical_columns(self) -> None:
        sql = feed_audit_sql.insert_feed_audit_event_cte(
            feed_id_sql="after_row.id",
            action_sql="audit_action.action",
            actor_id_sql="$1::text",
            feed_revision_sql="after_row.feed_revision",
            before_values_sql=feed_audit_sql.audit_snapshot_sql("before_row"),
            after_values_sql=feed_audit_sql.audit_snapshot_sql("after_row"),
            from_sql="FROM before_row JOIN after_row ON after_row.id = before_row.id",
            where_sql="audit_action.action IS NOT NULL",
        )

        self.assertIn("write_audit AS", sql)
        self.assertIn("INSERT INTO feed_audit_events", sql)
        self.assertIn("feed_id, action, actor_id, feed_revision", sql)
        self.assertIn("before_values, after_values", sql)
        self.assertIn("RETURNING id", sql)

    def test_shared_notification_payload_builder_renders_flat_contract(
        self,
    ) -> None:
        sql = feed_audit_sql.feed_audit_event_payload_sql()

        expected_keys = [
            f"{field_name!r}"
            for field_name in FeedChangeNotificationPayload.model_fields
        ]
        rendered_keys = [
            line.strip().split(",", maxsplit=1)[0]
            for line in sql.splitlines()
            if line.strip().startswith("'")
        ]

        self.assertIn("jsonb_build_object(", sql)
        self.assertEqual(expected_keys, rendered_keys)
        self.assertIn(
            "'event_type', 'radio_transcription.feed_change_notification'",
            sql,
        )
        self.assertIn("'schema_version', 1", sql)
        for column in (
            "id",
            "action",
            "occurred_at",
            "actor_id",
            "feed_id",
            "feed_revision",
            "before_values",
            "after_values",
        ):
            self.assertIn(f"feed_audit_events.{column}", sql)

    def test_audited_mutation_sql_embeds_audit_insert(self) -> None:
        audited_sql = (
            feed_queries.CREATE_FEED_SQL,
            feed_queries.UPDATE_FEED_SQL,
            feed_queries.DEACTIVATE_FEED_SQL,
            feed_queries.DELETE_FEED_SQL,
            feed_queries.RESET_FEED_SQL,
            feed_queries.UPDATE_PROGRESS_SQL,
            feed_queries.RECORD_SOURCE_OBSERVATION_SQL,
            feed_queries.REPORT_FAILURE_SQL,
            feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL,
            sync_feed_queries.HEARTBEAT_SQL,
            sync_feed_queries.RECORD_FAILURE_SQL,
            sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL,
        )

        for sql in audited_sql:
            stripped = _sql_without_comments(sql)
            self.assertIn("INSERT INTO feed_audit_events", stripped)
            self.assertIn("feed_revision", stripped)

    def test_async_audited_mutation_sql_returns_feed_audit_event(
        self,
    ) -> None:
        audited_sql = {
            "CREATE_FEED_SQL": feed_queries.CREATE_FEED_SQL,
            "UPDATE_FEED_SQL": feed_queries.UPDATE_FEED_SQL,
            "DEACTIVATE_FEED_SQL": feed_queries.DEACTIVATE_FEED_SQL,
            "DELETE_FEED_SQL": feed_queries.DELETE_FEED_SQL,
            "RESET_FEED_SQL": feed_queries.RESET_FEED_SQL,
            "UPDATE_PROGRESS_SQL": feed_queries.UPDATE_PROGRESS_SQL,
            "RECORD_SOURCE_OBSERVATION_SQL": (
                feed_queries.RECORD_SOURCE_OBSERVATION_SQL
            ),
            "REPORT_FAILURE_SQL": feed_queries.REPORT_FAILURE_SQL,
            "RELEASE_NON_BUDGETED_FAILURE_SQL": (
                feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL
            ),
        }

        for name, sql in audited_sql.items():
            with self.subTest(sql=name):
                stripped = _sql_without_comments(sql)
                self.assertIn("AS feed_audit_event", stripped)
                self.assertIn("write_audit.feed_audit_event", stripped)
                self.assertIn("(SELECT write_audit.feed_audit_event", stripped)
                self.assertNotIn("LEFT JOIN write_audit", stripped)

    def test_delete_feed_sql_keeps_audit_feed_id_for_child_deletes(
        self,
    ) -> None:
        sql = _sql_without_comments(feed_queries.DELETE_FEED_SQL)

        self.assertIn("RETURNING feed_id,", sql)
        self.assertIn("WHERE feed_id IN (SELECT feed_id FROM write_audit)", sql)

    def test_sync_audited_mutation_sql_returns_feed_audit_event(
        self,
    ) -> None:
        audited_sql = {
            "HEARTBEAT_SQL": sync_feed_queries.HEARTBEAT_SQL,
            "RECORD_FAILURE_SQL": sync_feed_queries.RECORD_FAILURE_SQL,
            "RECORD_NON_BUDGETED_FAILURE_SQL": (
                sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL
            ),
        }

        for name, sql in audited_sql.items():
            with self.subTest(sql=name):
                stripped = _sql_without_comments(sql)
                self.assertIn("AS feed_audit_event", stripped)
                self.assertIn("write_audit.feed_audit_event", stripped)
                self.assertIn("(SELECT write_audit.feed_audit_event", stripped)
                self.assertNotIn("LEFT JOIN write_audit", stripped)
                self.assertIn("%s", stripped)
                self.assertNotIn("$", stripped)


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
            stripped = _sql_without_comments(sql)
            self.assertIn("status = 'active'::feed_status", stripped)

    def test_progress_remains_allowed_after_deactivation(self) -> None:
        """Main allows one in-flight bookmark write after admin stop."""
        sql = _sql_without_comments(feed_queries.UPDATE_PROGRESS_SQL)

        self.assertNotIn("AND status = 'active'::feed_status", sql)

    def test_batch_worker_release_requires_active_status(self) -> None:
        sql = _sql_without_comments(feed_queries.RELEASE_FEEDS_BATCH_SQL)

        self.assertIn("WHERE worker_id = $1", sql)
        self.assertIn("AND status = 'active'::feed_status", sql)

    def test_admin_reset_and_delete_refuse_active_feeds(self) -> None:
        """Active feeds must be deactivated before reset or hard delete."""
        for sql in (
            feed_queries.RESET_FEED_SQL,
            feed_queries.DELETE_FEED_SQL,
        ):
            stripped = _sql_without_comments(sql)
            self.assertIn(
                "WHERE target.status <> 'active'::feed_status",
                stripped,
            )
            self.assertIn(
                "target_feed.status = 'active'::feed_status AS blocked_active",
                stripped,
            )

        deactivate_sql = _sql_without_comments(feed_queries.DEACTIVATE_FEED_SQL)
        self.assertNotIn(
            "AND f.status <> 'active'::feed_status",
            deactivate_sql,
        )


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


class TestBuildAcquireFeedsBatchSql(unittest.TestCase):
    """Tests for build_acquire_feeds_batch_sql pure helper."""

    def test_one_branch_per_claim_type(self) -> None:
        sql = feed_queries.build_acquire_feeds_batch_sql(
            [SourceType.BCFY_FEEDS]
        )
        self.assertEqual(
            sql.count("AS MATERIALIZED ("), 2
        )  # 1 branch + claimed

    def test_three_branches_for_production_set(self) -> None:
        sql = feed_queries.build_acquire_feeds_batch_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertEqual(
            sql.count("AS MATERIALIZED ("), 4
        )  # 3 branches + claimed

    def test_param_count_matches_claim_types(self) -> None:
        """N claim_types → LIMIT $2..$(1+N) appears in SQL."""
        sql = feed_queries.build_acquire_feeds_batch_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertIn("LIMIT $2", sql)
        self.assertIn("LIMIT $3", sql)
        self.assertIn("LIMIT $4", sql)
        self.assertNotIn("LIMIT $5", sql)

    def test_source_type_literals_inlined(self) -> None:
        sql = feed_queries.build_acquire_feeds_batch_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertIn("source_type = 'bcfy_feeds'", sql)
        self.assertIn("source_type = 'bcfy_calls'", sql)
        self.assertIn("source_type = 'openmhz'", sql)

    def test_deterministic(self) -> None:
        types = [SourceType.BCFY_FEEDS, SourceType.OPENMHZ]
        self.assertEqual(
            feed_queries.build_acquire_feeds_batch_sql(types),
            feed_queries.build_acquire_feeds_batch_sql(types),
        )

    def test_empty_claim_types_raises(self) -> None:
        with self.assertRaises(ValueError):
            feed_queries.build_acquire_feeds_batch_sql([])

    def test_byte_identical_to_golden_for_production_set(self) -> None:
        """Production claim_types order produces a known-good SQL string.

        Guards against accidental SQL formatting drift during future
        refactors. The SQL shape (whitespace, indentation, branch
        ordering, parameter numbering) is load-bearing — the planner
        chooses the (source_type, id) WHERE status='unclaimed' partial
        composite index based on the literal source_type per branch, and
        the DB-side prepared-statement cache keys on the SQL string.
        """
        expected = (
            "WITH\n"
            "    bcfy_feeds_claim AS MATERIALIZED (\n"
            "        SELECT id FROM feeds\n"
            "        WHERE source_type = 'bcfy_feeds' AND status = 'unclaimed'::feed_status\n"
            "        ORDER BY id\n"
            "        LIMIT $2\n"
            "        FOR NO KEY UPDATE SKIP LOCKED\n"
            "    ),\n"
            "    bcfy_calls_claim AS MATERIALIZED (\n"
            "        SELECT id FROM feeds\n"
            "        WHERE source_type = 'bcfy_calls' AND status = 'unclaimed'::feed_status\n"
            "        ORDER BY id\n"
            "        LIMIT $3\n"
            "        FOR NO KEY UPDATE SKIP LOCKED\n"
            "    ),\n"
            "    openmhz_claim AS MATERIALIZED (\n"
            "        SELECT id FROM feeds\n"
            "        WHERE source_type = 'openmhz' AND status = 'unclaimed'::feed_status\n"
            "        ORDER BY id\n"
            "        LIMIT $4\n"
            "        FOR NO KEY UPDATE SKIP LOCKED\n"
            "    ),\n"
            "    claimed AS MATERIALIZED (\n"
            "        SELECT id FROM bcfy_feeds_claim\n"
            "        UNION ALL\n"
            "        SELECT id FROM bcfy_calls_claim\n"
            "        UNION ALL\n"
            "        SELECT id FROM openmhz_claim\n"
            "    ),\n"
            "leased AS (\n"
            "    UPDATE feeds\n"
            "    SET status = 'active'::feed_status,\n"
            "        worker_id = $1,\n"
            "        fencing_token = fencing_token + 1,\n"
            "        last_heartbeat = NOW(),\n"
            "        retry_after = NULL\n"
            "    FROM claimed\n"
            "    WHERE feeds.id = claimed.id\n"
            "    RETURNING feeds.id, feeds.name, feeds.source_type,\n"
            "              feeds.last_processed_filename, feeds.last_bookmark_time,\n"
            "              feeds.fencing_token, feeds.failure_count,\n"
            "              feeds.status_reason\n"
            ")\n"
            "SELECT leased.id, leased.name, leased.source_type,\n"
            "       leased.last_processed_filename, leased.last_bookmark_time,\n"
            "       leased.fencing_token, leased.failure_count,\n"
            "       leased.status_reason,\n"
            "       fpi.source_feed_id\n"
            "FROM leased\n"
            "JOIN feed_properties fpi ON fpi.feed_id = leased.id\n"
        )
        actual = feed_queries.build_acquire_feeds_batch_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertEqual(actual, expected)


class TestBuildAcquireFeedsRecoverySql(unittest.TestCase):
    """Tests for build_acquire_feeds_recovery_sql pure helper."""

    def test_one_branch_per_claim_type(self) -> None:
        sql = feed_queries.build_acquire_feeds_recovery_sql(
            [SourceType.BCFY_FEEDS]
        )
        # 1 _recovery branch + 1 recovered = 2 MATERIALIZED.
        self.assertEqual(sql.count("AS MATERIALIZED ("), 2)
        self.assertIn("bcfy_feeds_recovery AS MATERIALIZED", sql)

    def test_three_branches_for_production_set(self) -> None:
        sql = feed_queries.build_acquire_feeds_recovery_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertEqual(sql.count("AS MATERIALIZED ("), 4)

    def test_param_count_matches_claim_types(self) -> None:
        """N claim_types → LIMIT $3..$(2+N) appears in SQL."""
        sql = feed_queries.build_acquire_feeds_recovery_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertIn("LIMIT $3", sql)
        self.assertIn("LIMIT $4", sql)
        self.assertIn("LIMIT $5", sql)
        self.assertNotIn("LIMIT $6", sql)
        # $2 is the abandonment interval.
        self.assertIn("$2::interval", sql)

    def test_each_branch_filters_failing_or_active_abandoned(self) -> None:
        sql = feed_queries.build_acquire_feeds_recovery_sql(
            [SourceType.BCFY_FEEDS]
        )
        self.assertIn("status = 'failing'::feed_status", sql)
        self.assertIn("status = 'active'::feed_status", sql)
        self.assertIn("retry_after IS NULL OR retry_after <= NOW()", sql)
        self.assertIn("last_heartbeat < NOW() - $2::interval", sql)

    def test_source_type_literals_inlined(self) -> None:
        sql = feed_queries.build_acquire_feeds_recovery_sql(
            [SourceType.BCFY_FEEDS, SourceType.OPENMHZ]
        )
        self.assertIn("source_type = 'bcfy_feeds'", sql)
        self.assertIn("source_type = 'openmhz'", sql)
        # bcfy_calls is absent — no branch generated, so no filter.
        self.assertNotIn("source_type = 'bcfy_calls'", sql)

    def test_empty_claim_types_raises(self) -> None:
        with self.assertRaises(ValueError):
            feed_queries.build_acquire_feeds_recovery_sql([])


class TestAsyncSyncFailureSqlContracts(unittest.TestCase):
    """Cross-check async and sync lifecycle SQL invariants."""

    def test_budgeted_failures_increment_failure_count(self) -> None:
        self.assertIn(
            "failure_count + 1",
            _sql_without_comments(feed_queries.REPORT_FAILURE_SQL),
        )
        self.assertIn(
            "failure_count + 1",
            _sql_without_comments(sync_feed_queries.RECORD_FAILURE_SQL),
        )

    def test_non_budgeted_failures_reset_failure_count(self) -> None:
        self.assertIn(
            "failure_count = 0",
            _sql_without_comments(
                feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL
            ),
        )
        self.assertIn(
            "failure_count = 0",
            _sql_without_comments(
                sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL
            ),
        )

    def test_async_failure_sql_writes_status_reason_detail(self) -> None:
        self.assertIn(
            "status_reason_detail = $8",
            _sql_without_comments(feed_queries.REPORT_FAILURE_SQL),
        )
        self.assertIn(
            "status_reason_detail = $6",
            _sql_without_comments(
                feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL
            ),
        )

    def test_sync_failure_sql_writes_status_reason_detail(self) -> None:
        self.assertIn(
            "status_reason_detail = %s",
            _sql_without_comments(sync_feed_queries.RECORD_FAILURE_SQL),
        )
        self.assertIn(
            "status_reason_detail = %s",
            _sql_without_comments(
                sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL
            ),
        )

    def test_sync_heartbeat_sql_clears_status_reason_detail(self) -> None:
        sql = _sql_without_comments(sync_feed_queries.HEARTBEAT_SQL)

        self.assertIn("status_reason_detail = NULL", sql)
        self.assertIn(
            "feeds.status_reason IS NOT NULL OR feeds.status_reason_detail IS NOT NULL",
            sql,
        )

    def test_status_reason_timestamp_updates_are_conditional(self) -> None:
        for sql in (
            feed_queries.REPORT_FAILURE_SQL,
            feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL,
            sync_feed_queries.RECORD_FAILURE_SQL,
            sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL,
        ):
            stripped = _sql_without_comments(sql)
            self.assertIn("status_reason_updated_at = CASE", stripped)
            self.assertIn("ELSE feeds.status_reason_updated_at", stripped)

    def test_async_sql_uses_fencing_and_sync_sql_uses_terminal_guards(
        self,
    ) -> None:
        self.assertIn(
            "fencing_token",
            _sql_without_comments(feed_queries.REPORT_FAILURE_SQL),
        )
        self.assertIn(
            "fencing_token",
            _sql_without_comments(
                feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL
            ),
        )
        for sql in (
            sync_feed_queries.HEARTBEAT_SQL,
            sync_feed_queries.RECORD_FAILURE_SQL,
            sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL,
        ):
            stripped = _sql_without_comments(sql)
            self.assertIn("quarantined", stripped)
            self.assertIn("deactivated", stripped)
            self.assertNotIn("fencing_token", stripped)


class TestRuntimeAuditOwnership(unittest.TestCase):
    """Static guards for storage-owned runtime audit insertion."""

    def test_runtime_and_echo_sources_do_not_reference_audit_table(
        self,
    ) -> None:
        paths = (
            pathlib.Path("backend/pipeline/ingestion/collector_runtime.py"),
            pathlib.Path("backend/pipeline/ingestion/collectors/echo/main.py"),
        )

        for path in paths:
            with self.subTest(path=str(path)):
                text = path.read_text()
                self.assertNotIn("feed_audit_events", text)
                self.assertNotIn("INSERT INTO feed_audit_events", text)
