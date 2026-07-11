"""Static regression contracts for the Phase 1 Lease migrations."""

from __future__ import annotations

import pathlib
import re

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


def _read(path: str) -> str:
    return (_REPO_ROOT / path).read_text(encoding="utf-8")


def _normalized_sql(path: str) -> str:
    return " ".join(_read(path).split())


def _sql_without_comments(path: str) -> str:
    return re.sub(r"--.*$", "", _read(path), flags=re.MULTILINE).strip()


def test_lease_table_contract_requires_permanent_standalone_table() -> None:
    for path in (
        "terraform/modules/alloydb/sql/ingestion/031_ingestion_leases.sql",
        "terraform/modules/alloydb/sql/ingestion/032_ingestion_lease_guards.sql",
        "terraform/modules/alloydb/sql/ci/phase_1_schema_contract.sql",
    ):
        sql = _normalized_sql(path)

        assert "c.relpersistence" in sql
        assert "c.relispartition" in sql
        assert "FROM pg_catalog.pg_inherits AS i" in sql
        assert "i.inhrelid = lease_table_oid" in sql
        assert "i.inhparent = lease_table_oid" in sql


def test_lease_guard_recovery_is_one_atomic_statement() -> None:
    sql = " ".join(
        _sql_without_comments(
            "terraform/modules/alloydb/sql/ingestion/032_ingestion_lease_guards.sql"
        ).split()
    )

    assert sql.startswith("DO $migration$")
    assert sql.endswith("$migration$;")
    assert "DO $install$" not in sql
    assert "DO $postcondition$" not in sql

    table_lock = sql.index(
        "LOCK TABLE ONLY public.ingestion_leases IN SHARE ROW EXCLUSIVE MODE"
    )
    preflight = sql.index(
        "Existing Lease guard trigger %I has the wrong definition"
    )
    create_trigger = sql.index("CREATE TRIGGER")
    enable_always = sql.index("ENABLE ALWAYS TRIGGER")
    final_verification = sql.index(
        "Lease guard trigger %I has the wrong final definition"
    )

    assert (
        table_lock
        < preflight
        < create_trigger
        < enable_always
        < final_verification
    )


def test_ci_exercises_lease_guard_partial_state_recovery() -> None:
    workflow = _read(".github/workflows/ci.yml")

    for token in (
        "Starting unlogged Lease-table rejection fixture",
        "Starting missing Lease-guard trigger recovery fixture",
        "Starting malformed Lease-guard trigger atomicity fixture",
        "missing_trigger_count_before",
        "missing_trigger_count_after",
        "malformed_trigger_state_before",
        "malformed_trigger_state_after",
        'if psql -v ON_ERROR_STOP=1 -f "$lease_guard_sql"; then',
        'if [ "$malformed_trigger_state_after" != '
        '"$malformed_trigger_state_before" ]; then',
    ):
        assert token in workflow


def test_ci_requires_the_expected_membership_preflight_rejection() -> None:
    workflow = _read(".github/workflows/ci.yml")

    for token in (
        "preflight_rejection_output=",
        "preflight_rejection_status=$?",
        'if [ "$preflight_rejection_status" -ne 3 ]; then',
        "Unexpected or unhealthy membership-index relation in public",
    ):
        assert token in workflow


def test_membership_index_catalog_name_arrays_use_text_elements() -> None:
    for path in (
        "terraform/modules/alloydb/sql/ingestion/"
        "035_feed_properties_bcfy_calls_membership_index_preflight.sql",
        "terraform/modules/alloydb/sql/ingestion/"
        "037_feed_properties_bcfy_calls_membership_index_postflight.sql",
        "terraform/modules/alloydb/sql/ci/phase_1_schema_contract.sql",
    ):
        sql = _normalized_sql(path)

        for projection in (
            "key_attribute.attname::TEXT AS key_name",
            "opclass.opcname::TEXT AS opclass_name",
            "opclass_namespace.nspname::TEXT AS opclass_schema",
        ):
            assert projection in sql
