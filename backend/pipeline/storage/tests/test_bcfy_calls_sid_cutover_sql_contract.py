"""Static contracts for the controlled Broadcastify Calls SID handoff."""

from __future__ import annotations

import pathlib
import re
import unittest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_ALLOYDB_ROOT = _REPO_ROOT / "terraform/modules/alloydb"
_OPERATIONS_SQL = _ALLOYDB_ROOT / "sql/operations/bcfy_calls_sid"


def _named_block(text: str, marker: str) -> str:
    """Return one HCL block selected by its exact opening marker.

    Args:
        text: Complete HCL source text.
        marker: Exact opening text for the requested block.

    Returns:
        The complete balanced HCL block.

    Raises:
        AssertionError: If the marker is absent or the block is unbalanced.
    """
    start = text.find(marker)
    assert start >= 0, f"missing {marker}"
    depth = 0
    for index in range(start, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    raise AssertionError  # pragma: no cover - malformed fixture diagnostic


def _assert_active_endpoint_fanout(main_tf: str, outputs_tf: str) -> None:
    """Pin the source fallback and both administrative endpoint consumers.

    Args:
        main_tf: AlloyDB module implementation text.
        outputs_tf: AlloyDB module output declarations.

    Raises:
        AssertionError: If either job bypasses the selected endpoint or source
            and active output identities are conflated.
    """
    assert main_tf.count("google_alloydb_instance.primary.ip_address") == 1
    assert main_tf.count("value = local.active_primary_instance_ip") == 2

    schema_job = _named_block(
        main_tf,
        'resource "google_cloud_run_v2_job" "schema_migration" {',
    )
    sid_job = _named_block(
        main_tf,
        'resource "google_cloud_run_v2_job" "bcfy_calls_sid_operation" {',
    )
    for job in (schema_job, sid_job):
        assert 'name  = "DB_HOST"' in job
        assert "value = local.active_primary_instance_ip" in job

    source_output = _named_block(
        outputs_tf,
        'output "primary_instance_ip" {',
    )
    active_output = _named_block(
        outputs_tf,
        'output "active_primary_instance_ip" {',
    )
    assert "google_alloydb_instance.primary.ip_address" in source_output
    assert "local.active_primary_instance_ip" in active_output


def _sql_without_comments(sql: str) -> str:
    """Return normalized SQL with line comments removed."""
    uncommented = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    return " ".join(uncommented.split()).lower()


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
        self.assertIn("last_heartbeat is null", sql)
        self.assertIn("last_heartbeat >= now() - interval '60 seconds'", sql)
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

    def test_both_sql_jobs_share_fail_closed_artifact_inputs(self) -> None:
        main_tf = (_ALLOYDB_ROOT / "main.tf").read_text()
        variables_tf = (_ALLOYDB_ROOT / "variables.tf").read_text()
        schema_job = _named_block(
            main_tf,
            'resource "google_cloud_run_v2_job" "schema_migration" {',
        )
        sid_job = _named_block(
            main_tf,
            'resource "google_cloud_run_v2_job" "bcfy_calls_sid_operation" {',
        )

        for job in (schema_job, sid_job):
            with self.subTest(job=job.splitlines()[0]):
                self.assertRegex(job, r"image\s*=\s*var\.sql_job_image")
                self.assertRegex(
                    job,
                    r"version\s*=\s*var\.password_secret_version",
                )
                self.assertNotIn('version = "latest"', job)
                self.assertIn("var.sql_job_image != null", job)
                self.assertIn("var.password_secret_version != null", job)

        image_variable = _named_block(
            variables_tf,
            'variable "sql_job_image" {',
        )
        secret_version_variable = _named_block(
            variables_tf,
            'variable "password_secret_version" {',
        )
        self.assertIn(
            "^[^[:space:]@]+@sha256:[0-9a-f]{64}$",
            image_variable,
        )
        self.assertIn("^[1-9][0-9]*$", secret_version_variable)
        self.assertNotIn('default     = "latest"', secret_version_variable)


class TestActiveAlloyDBEndpointContract(unittest.TestCase):
    """Pins the two public administrative consumers to one selected endpoint."""

    def test_both_jobs_follow_active_endpoint_and_source_remains_distinct(
        self,
    ) -> None:
        main_tf = (_ALLOYDB_ROOT / "main.tf").read_text()
        outputs_tf = (_ALLOYDB_ROOT / "outputs.tf").read_text()

        _assert_active_endpoint_fanout(main_tf, outputs_tf)
        self.assertIn(
            "active_primary_instance_ip = coalesce(",
            main_tf,
        )
        schema_executor = _named_block(
            main_tf,
            'resource "null_resource" "execute_schema_migration" {',
        )
        self.assertNotIn("active_primary_instance_ip", schema_executor)

    def test_deliberate_job_bypass_fails_the_search_contract(self) -> None:
        main_tf = (_ALLOYDB_ROOT / "main.tf").read_text()
        outputs_tf = (_ALLOYDB_ROOT / "outputs.tf").read_text()
        bypass = main_tf.replace(
            "value = local.active_primary_instance_ip",
            "value = google_alloydb_instance.primary.ip_address",
            1,
        )
        self.assertNotEqual(bypass, main_tf)

        with self.assertRaises(AssertionError):
            _assert_active_endpoint_fanout(bypass, outputs_tf)


if __name__ == "__main__":
    unittest.main()
