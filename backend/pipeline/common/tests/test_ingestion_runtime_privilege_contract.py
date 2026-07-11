"""Static contracts for the dedicated ingestion runtime database role."""

from __future__ import annotations

import pathlib
import re

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_BOOTSTRAP_PATH = (
    "terraform/modules/alloydb/sql/privileges/"
    "000_ingestion_runtime_bootstrap.sql"
)
_RECONCILE_PATH = (
    "terraform/modules/alloydb/sql/privileges/"
    "999_ingestion_runtime_reconcile.sql"
)
_CONTRACT_PATH = (
    "terraform/modules/alloydb/sql/ci/ingestion_runtime_privilege_contract.sql"
)
_ROLE = "app_ingestion_runtime"


def _read(path: str) -> str:
    return (_REPO_ROOT / path).read_text(encoding="utf-8")


def _normalized_sql(path: str) -> str:
    sql = re.sub(r"--.*$", "", _read(path), flags=re.MULTILINE)
    return " ".join(sql.split())


def test_schema_job_orders_privilege_boundaries_around_migrations() -> None:
    terraform = _read("terraform/modules/alloydb/main.tf")

    bootstrap = "/sql/privileges/000_ingestion_runtime_bootstrap.sql"
    migrations = "for f in /sql/ingestion/*.sql"
    reconcile = "/sql/privileges/999_ingestion_runtime_reconcile.sql"
    assert terraform.index(bootstrap) < terraform.index(migrations)
    assert terraform.index(migrations) < terraform.index(reconcile)

    assert 'name   = "ingestion/${each.value}"' in terraform
    assert 'name   = "privileges/${each.value}"' in terraform
    assert "google_storage_bucket_object.privilege_sql" in terraform
    assert terraform.count("psql -X -v ON_ERROR_STOP=1 -f") >= 3


def test_bootstrap_normalizes_a_non_login_role_without_touching_worker() -> (
    None
):
    sql = _normalized_sql(_BOOTSTRAP_PATH)

    assert sql.startswith("BEGIN;")
    assert sql.endswith("COMMIT;")
    assert f"CREATE ROLE {_ROLE}" in sql
    assert (
        f"ALTER ROLE {_ROLE} NOLOGIN NOSUPERUSER NOCREATEDB "
        "NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS"
    ) in sql
    assert f"ALTER ROLE {_ROLE} PASSWORD NULL" in sql
    assert "FROM pg_catalog.pg_auth_members" in sql
    assert f"REVOKE %I FROM {_ROLE}" in sql
    assert "rolcanlogin" in sql
    assert "rolsuper" in sql
    assert "rolcreatedb" in sql
    assert "rolcreaterole" in sql
    assert "rolreplication" in sql
    assert "rolbypassrls" in sql
    assert "worker" not in sql.replace(_ROLE, "")


def test_reconciliation_revokes_before_the_four_exact_table_grants() -> None:
    sql = _normalized_sql(_RECONCILE_PATH)

    assert sql.startswith("BEGIN;")
    assert sql.endswith("COMMIT;")
    first_grant = sql.index("GRANT SELECT, UPDATE ON TABLE")
    for revocation in (
        f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {_ROLE}",
        f"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {_ROLE}",
        f"REVOKE ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA public FROM {_ROLE}",
        "REVOKE CREATE, TEMPORARY ON DATABASE",
        "REVOKE CREATE ON SCHEMA public FROM PUBLIC",
    ):
        assert sql.index(revocation) < first_grant

    expected_grants = {
        "public.ingestion_leases": frozenset({"SELECT", "UPDATE"}),
        "public.feeds": frozenset({"SELECT", "UPDATE"}),
        "public.feed_properties": frozenset({"SELECT"}),
        "public.feed_audit_events": frozenset({"SELECT", "INSERT"}),
    }
    matches = re.findall(
        rf"GRANT ([A-Z, ]+) ON TABLE (public\.[a-z_]+) TO {_ROLE};",
        sql,
    )
    actual_grants = {
        table: frozenset(
            privilege.strip() for privilege in privileges.split(",")
        )
        for privileges, table in matches
    }
    assert actual_grants == expected_grants

    assert f"GRANT CONNECT ON DATABASE %I TO {_ROLE}" in sql
    assert f"GRANT USAGE ON SCHEMA public TO {_ROLE}" in sql
    assert f"GRANT USAGE ON TYPE public.feed_status TO {_ROLE}" in sql


def test_scripts_forbid_future_dml_schema_create_sequences_and_functions() -> (
    None
):
    combined = " ".join(
        (
            _normalized_sql(_BOOTSTRAP_PATH),
            _normalized_sql(_RECONCILE_PATH),
        )
    )

    assert not re.search(
        rf"ALTER DEFAULT PRIVILEGES[^;]*GRANT [^;]*ON TABLES TO {_ROLE}",
        combined,
    )
    assert not re.search(
        rf"GRANT [^;]*ON (?:ALL )?SEQUENCES? [^;]*TO {_ROLE}",
        combined,
    )
    assert not re.search(
        rf"GRANT [^;]*ON (?:ALL )?(?:FUNCTIONS?|ROUTINES?) [^;]*TO {_ROLE}",
        combined,
    )
    assert not re.search(
        rf"GRANT (?:ALL|CREATE)[^;]*ON SCHEMA [^;]*TO {_ROLE}",
        combined,
    )
    assert "ALTER DEFAULT PRIVILEGES FOR ROLE postgres" in combined
    assert f"REVOKE ALL PRIVILEGES ON TABLES FROM {_ROLE}" in combined
    assert (
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres "
        "REVOKE EXECUTE ON ROUTINES FROM PUBLIC;"
    ) in combined
    assert (
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres "
        "REVOKE USAGE ON TYPES FROM PUBLIC;"
    ) in combined
    assert "REVOKE EXECUTE ON ROUTINES FROM PUBLIC" in combined
    assert "REVOKE USAGE ON TYPES FROM PUBLIC" in combined


def test_reconciliation_fails_closed_and_keeps_postgres_ownership() -> None:
    sql = _normalized_sql(_RECONCILE_PATH)

    for relation in (
        "public.ingestion_leases",
        "public.feeds",
        "public.feed_properties",
        "public.feed_audit_events",
    ):
        assert relation in sql
    assert "public.feed_status" in sql
    assert "missing expected ingestion privilege object" in sql
    assert "must remain owned by postgres" in sql
    assert "must not own database objects" in sql
    assert "ALTER OWNER" not in sql
    assert "REASSIGN OWNED" not in sql
    assert "DROP OWNED" not in sql
    assert "ALTER TABLE" not in sql
    assert "pg_catalog.current_user" not in sql
    assert "CURRENT_USER <> 'postgres'" in sql
    assert "pg_catalog.pg_attribute" in sql
    assert "pg_catalog.has_column_privilege" in sql
    assert "type.typrelid = 0" in sql
    assert "type_relation.relkind = 'c'" in sql


def test_database_contract_checks_effective_rights_and_every_object_class() -> (
    None
):
    sql = _normalized_sql(_CONTRACT_PATH)

    for function in (
        "has_database_privilege",
        "has_schema_privilege",
        "has_type_privilege",
        "has_table_privilege",
        "has_any_column_privilege",
        "has_column_privilege",
        "has_sequence_privilege",
        "has_function_privilege",
        "pg_has_role",
    ):
        assert f"pg_catalog.{function}" in sql
    for catalog in (
        "pg_catalog.pg_auth_members",
        "pg_catalog.pg_attribute",
        "pg_catalog.pg_class",
        "pg_catalog.pg_proc",
        "pg_catalog.pg_type",
        "pg_catalog.pg_default_acl",
    ):
        assert catalog in sql
    assert "PUBLIC/inherited effective privilege" in sql
    assert "pg_catalog.aclexplode(attribute.attacl)" in sql
    assert "WITH GRANT OPTION" in sql
    assert "'CONNECT WITH GRANT OPTION'" in sql
    assert "'USAGE WITH GRANT OPTION'" in sql
    assert "MAINTAIN" in sql
    assert "server_version_num')::INTEGER >= 160000" in sql
    assert "type.typrelid = 0" in sql
    assert "type_relation.relkind = 'c'" in sql
