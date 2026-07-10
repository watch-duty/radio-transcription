"""Static regression contracts for the Phase 1 Lease migrations."""

from __future__ import annotations

import pathlib


_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


def _read(path: str) -> str:
    return (_REPO_ROOT / path).read_text(encoding="utf-8")


def _normalized_sql(path: str) -> str:
    return " ".join(_read(path).split())


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
