"""Static contracts for the controlled Broadcastify Calls SID handoff."""

from __future__ import annotations

import pathlib
import re
import unittest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_ALLOYDB_ROOT = _REPO_ROOT / "terraform/modules/alloydb"
_INGESTION_SQL = _ALLOYDB_ROOT / "sql/ingestion"
_OPERATIONS_SQL = _ALLOYDB_ROOT / "sql/operations/bcfy_calls_sid"
_RUNTIME_MIGRATION = _INGESTION_SQL / (
    "038_ingestion_lease_runtime_columns.sql"
)
_RUNTIME_CHECK = _ALLOYDB_ROOT / (
    "sql/ci/ingestion_lease_runtime_columns_check.sql"
)


def _sql_without_comments(sql: str) -> str:
    """Return normalized SQL with line comments removed."""
    uncommented = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    return " ".join(uncommented.split()).lower()


class TestLeaseRuntimeMigrationContract(unittest.TestCase):
    """Pins the forward-only repair required by the generic Lease runtime."""

    def test_runtime_migration_is_unique_and_historical_031_is_unchanged(
        self,
    ) -> None:
        names = {path.name for path in _INGESTION_SQL.glob("*.sql")}

        self.assertIn(_RUNTIME_MIGRATION.name, names)
        self.assertEqual(
            sum(name.startswith("038_") for name in names),
            1,
        )
        historical = (_INGESTION_SQL / "031_ingestion_leases.sql").read_text()
        self.assertNotIn("unclaimed_since", historical)
        self.assertNotIn("status_reason_updated_at", historical)
        self.assertNotIn("audit_revision", historical)

    def test_runtime_migration_is_replay_safe_and_fail_closed(self) -> None:
        sql = _sql_without_comments(_RUNTIME_MIGRATION.read_text())

        for column_contract in (
            "add column if not exists unclaimed_since timestamptz",
            "add column if not exists status_reason_updated_at timestamptz",
            "add column if not exists audit_revision bigint not null default 0",
        ):
            self.assertIn(column_contract, sql)
        self.assertIn("ingestion_leases_audit_revision_nonnegative", sql)
        self.assertIn("validate constraint", sql)
        self.assertIn("pg_catalog.pg_attribute", sql)
        self.assertIn("pg_catalog.pg_constraint", sql)
        self.assertNotRegex(sql, r"\b(delete|truncate)\b")
        self.assertNotIn("set fencing_token", sql)
        self.assertNotIn("set status", sql)

    def test_catalog_check_pins_exact_runtime_shape(self) -> None:
        sql = _sql_without_comments(_RUNTIME_CHECK.read_text())

        for token in (
            "unclaimed_since",
            "status_reason_updated_at",
            "audit_revision",
            "ingestion_leases_audit_revision_nonnegative",
            "pg_catalog.pg_get_expr",
            "convalidated",
        ):
            self.assertIn(token, sql)


class TestSidCutoverOperationPlacement(unittest.TestCase):
    """Keeps authority mutations outside automatic schema application."""

    def test_exact_operation_set_is_outside_ingestion_glob(self) -> None:
        operations = sorted(path.name for path in _OPERATIONS_SQL.glob("*.sql"))

        self.assertEqual(
            operations,
            [
                "001_preseed.sql",
                "002_activate.sql",
                "003_rollback_children.sql",
                "004_verify.sql",
            ],
        )
        main_tf = (_ALLOYDB_ROOT / "main.tf").read_text()
        self.assertIn(
            'fileset("${path.module}/sql/ingestion", "*.sql")',
            main_tf,
        )
        self.assertNotIn(
            'fileset("${path.module}/sql/operations",',
            main_tf,
        )

    def test_mutations_are_transactional_and_do_not_infer_membership(
        self,
    ) -> None:
        forbidden = (
            "split_part",
            "substring(source_feed_id",
            "regexp_match",
            "delete from public.ingestion_leases",
            "truncate public.ingestion_leases",
        )

        for name in (
            "001_preseed.sql",
            "002_activate.sql",
            "003_rollback_children.sql",
        ):
            with self.subTest(operation=name):
                sql = _sql_without_comments(
                    (_OPERATIONS_SQL / name).read_text()
                )
                self.assertIn("begin;", sql)
                self.assertIn("commit;", sql)
                self.assertIn("set local lock_timeout", sql)
                self.assertIn("set local statement_timeout", sql)
                self.assertIn("bcfy_calls_sid", sql)
                self.assertIn("bcfy_calls_group_id", sql)
                self.assertIn("bcfy_calls_is_trunked", sql)
                for token in forbidden:
                    self.assertNotIn(token, sql)

    def test_activation_is_review_bound_and_monotonic(self) -> None:
        sql = _sql_without_comments(
            (_OPERATIONS_SQL / "002_activate.sql").read_text()
        )

        self.assertIn("reviewed_sid_count", sql)
        self.assertIn("reviewed_manifest_digest", sql)
        self.assertIn("membership_revision", sql)
        self.assertIn("greatest", sql)
        self.assertIn("max", sql)
        self.assertIn("fencing_token", sql)
        self.assertIn("9223372036854775807", sql)
        self.assertIn("'unclaimed'::public.feed_status", sql)
        self.assertRegex(
            sql,
            re.compile(
                r"lock table public\.ingestion_leases.*"
                r"lock table public\.feeds.*"
                r"lock table public\.feed_properties",
            ),
        )

    def test_rollback_never_mutates_lease_rows_or_durable_progress(
        self,
    ) -> None:
        sql = _sql_without_comments(
            (_OPERATIONS_SQL / "003_rollback_children.sql").read_text()
        )

        self.assertNotIn("update public.ingestion_leases", sql)
        self.assertIn("'unclaimed'::public.feed_status", sql)
        for field in (
            "fencing_token =",
            "last_bookmark_time =",
            "last_processed_filename =",
            "failure_count =",
            "retry_after =",
            "status_reason =",
            "audit_revision =",
        ):
            self.assertNotIn(field, sql)


class TestManualOperationJobContract(unittest.TestCase):
    """Pins the private, explicit, zero-retry execution surface."""

    def test_manual_job_is_whitelisted_and_never_auto_executed(self) -> None:
        main_tf = (_ALLOYDB_ROOT / "main.tf").read_text()

        self.assertIn(
            'resource "google_cloud_run_v2_job" "bcfy_calls_sid_operation"',
            main_tf,
        )
        self.assertIn("max_retries     = 0", main_tf)
        self.assertIn('value = "5432"', main_tf)
        for operation in (
            "verify",
            "preseed",
            "activate",
            "rollback_children",
        ):
            self.assertIn(operation, main_tf)
        self.assertNotRegex(
            main_tf,
            re.compile(
                r'resource "null_resource"[^}]*bcfy_calls_sid_operation',
                re.DOTALL,
            ),
        )
        self.assertIn("ON_ERROR_STOP=1", main_tf)
        self.assertIn("read_only = true", main_tf)


if __name__ == "__main__":
    unittest.main()
