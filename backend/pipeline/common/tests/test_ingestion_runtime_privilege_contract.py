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
    assert "DO $creator_default_acl_revoke$" in combined
    assert "ALTER DEFAULT PRIVILEGES FOR ROLE %I" in combined
    for object_class in ("TABLES", "SEQUENCES", "ROUTINES", "TYPES"):
        assert f"REVOKE ALL PRIVILEGES ON {object_class} FROM %I" in combined
        assert (
            f"REVOKE ALL PRIVILEGES ON {object_class} FROM PUBLIC" in combined
        )


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


def test_parameter_and_ownership_boundaries_use_complete_catalogs() -> None:
    bootstrap = _normalized_sql(_BOOTSTRAP_PATH)
    reconcile = _normalized_sql(_RECONCILE_PATH)
    contract = _normalized_sql(_CONTRACT_PATH)

    for sql in (bootstrap, reconcile):
        assert "pg_catalog.pg_parameter_acl" in sql
        assert "pg_catalog.pg_db_role_setting" in sql
        assert "REVOKE SET, ALTER SYSTEM ON PARAMETER" in sql
        assert "ALTER ROLE ALL RESET %I" in sql
        assert "ALTER ROLE %I RESET %I" in sql
        assert "ALTER ROLE %I IN DATABASE %I RESET %I" in sql
        assert "ALTER DATABASE %I RESET %I" in sql
        assert re.search(
            r"role_setting\.setrole = 0::OID\s+"
            r"AND role_setting\.setdatabase IN \(\s*0::OID,",
            sql,
        )
        assert "session_replication_role" in sql
        assert "lo_compat_privileges" in sql
        assert "pg_catalog.has_parameter_privilege" in sql

    for sql in (bootstrap, reconcile, contract):
        assert "pg_catalog.pg_shdepend" in sql
        assert "'pg_catalog.pg_authid'::pg_catalog.regclass" in sql
        assert "dependency.deptype = 'o'" in sql
        assert "pg_catalog.has_parameter_privilege" in sql
        assert "pg_catalog.pg_db_role_setting" in sql
        assert "session_replication_role" in sql
        assert "lo_compat_privileges" in sql

    assert "pg_catalog.pg_parameter_acl" in contract
    assert "pg_catalog.aclexplode(parameter_acl.paracl)" in contract


def test_large_object_creation_and_existing_acl_surfaces_are_closed() -> None:
    bootstrap = _normalized_sql(_BOOTSTRAP_PATH)
    reconcile = _normalized_sql(_RECONCILE_PATH)
    contract = _normalized_sql(_CONTRACT_PATH)
    combined = f"{bootstrap} {reconcile} {contract}"

    creation_signatures = (
        "pg_catalog.lo_create(oid)",
        "pg_catalog.lo_creat(integer)",
        "pg_catalog.lo_from_bytea(oid, bytea)",
    )
    exact_public_revoke = (
        "REVOKE EXECUTE ON FUNCTION "
        f"{', '.join(creation_signatures)} FROM PUBLIC CASCADE;"
    )
    for sql in (bootstrap, reconcile):
        assert exact_public_revoke in sql
        assert "REVOKE SELECT, UPDATE ON LARGE OBJECT" in sql
        for signature in creation_signatures:
            assert signature in sql

    for sql in (bootstrap, reconcile, contract):
        assert "pg_catalog.pg_largeobject_metadata" in sql
        assert "large_object.lomowner" in sql
        assert "large_object.lomacl" in sql
        assert re.search(r"pg_catalog\.acldefault\(\s*'L'", sql)
        assert "pg_catalog.aclexplode" in sql
        assert "acl.is_grantable" in sql

        creator_start = sql.index(
            "FROM pg_catalog.pg_proc AS procedure CROSS JOIN runtime_roles"
        )
        creator_end = sql.index(
            "large-object creator'",
            creator_start,
        )
        creator_assertion = sql[creator_start:creator_end]
        assert "acl.grantee = runtime_role.oid" in creator_assertion
        assert "acl.grantee = 0::OID" in creator_assertion
        assert "'USAGE'" in creator_assertion
        assert "set_role_membership_mode" in creator_assertion

        acl_start = sql.index(
            "FROM pg_catalog.pg_largeobject_metadata AS large_object",
            creator_end,
        )
        acl_end = sql.index("large-object capability'", acl_start)
        acl_assertion = sql[acl_start:acl_end]
        for reachability_contract in (
            "large_object.lomowner = runtime_role.oid",
            "pg_catalog.pg_has_role( runtime_role.oid, "
            "large_object.lomowner, 'USAGE' )",
            "pg_catalog.pg_has_role( runtime_role.oid, "
            "large_object.lomowner, set_role_membership_mode )",
            "acl.grantee = runtime_role.oid",
            "pg_catalog.pg_has_role( runtime_role.oid, acl.grantee, 'USAGE' )",
            "pg_catalog.pg_has_role( runtime_role.oid, acl.grantee, "
            "set_role_membership_mode )",
        ):
            assert reachability_contract in acl_assertion

    assert not re.search(
        r"REVOKE\s+(?:EXECUTE|ALL(?:\s+PRIVILEGES)?)\s+ON\s+ALL\s+"
        r"(?:FUNCTIONS|PROCEDURES|ROUTINES)\s+"
        r"IN\s+SCHEMA\s+pg_catalog",
        combined,
        flags=re.IGNORECASE,
    )


def test_every_public_schema_creator_has_all_default_acl_classes_normalized() -> (
    None
):
    bootstrap = _normalized_sql(_BOOTSTRAP_PATH)
    reconcile = _normalized_sql(_RECONCILE_PATH)
    contract = _normalized_sql(_CONTRACT_PATH)

    for sql in (bootstrap, reconcile):
        assert "pg_catalog.has_schema_privilege" in sql
        assert "SELECT role.oid, role.rolname" in sql
        assert "'public', 'CREATE'" in sql
        assert "ALTER DEFAULT PRIVILEGES FOR ROLE %I" in sql
        for object_class in ("TABLES", "SEQUENCES", "ROUTINES", "TYPES"):
            assert f"ON {object_class} FROM PUBLIC" in sql
            assert f"ON {object_class} FROM %I" in sql
        assert "set_role_membership_mode" in sql
        assert "'SET'" in sql
        assert "'MEMBER'" in sql

    assert "pg_catalog.pg_default_acl" in contract
    assert "pg_catalog.acldefault" in contract
    assert "defaults.defaclobjtype IN ('r', 'S', 'f', 'T')" in contract
    assert "pg_catalog.has_schema_privilege" in contract
    assert "set_role_membership_mode" in contract


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
        "pg_catalog.pg_parameter_acl",
        "pg_catalog.pg_shdepend",
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


def _assert_workflow_large_object_contract(workflow: str) -> None:
    for token in (
        "INGESTION_LARGE_OBJECT_FIXTURE",
        "GRANT SELECT, UPDATE ON LARGE OBJECT",
        "pg_catalog.lo_unlink",
        "ALTER ROLE ALL RESET lo_compat_privileges",
    ):
        assert token in workflow
    for creation_routine in (
        "pg_catalog.lo_create(oid)",
        "pg_catalog.lo_creat(integer)",
        "pg_catalog.lo_from_bytea(oid, bytea)",
    ):
        assert creation_routine in workflow
    for global_setting_drift in (
        "ALTER ROLE ALL\n              SET session_replication_role = replica;",
        "ALTER ROLE ALL\n              SET lo_compat_privileges = on;",
    ):
        assert global_setting_drift in workflow

    cleanup_start = workflow.index("cleanup_ephemeral_roles()")
    cleanup_end = workflow.index("trap cleanup_ephemeral_roles EXIT")
    cleanup = workflow[cleanup_start:cleanup_end]
    guarded_psql_blocks = [
        match.group(1)
        for match in re.finditer(
            r"if ! psql -X -v ON_ERROR_STOP=1 <<'SQL'\n"
            r"(.*?)\n\s*SQL\n\s*then",
            cleanup,
            flags=re.DOTALL,
        )
    ]
    cleanup_operations = (
        "ALTER ROLE ALL RESET session_replication_role;",
        "ALTER ROLE ALL RESET lo_compat_privileges;",
        "SELECT pg_catalog.lo_unlink(large_object.oid)",
    )
    operation_blocks = [
        next(
            index
            for index, block in enumerate(guarded_psql_blocks)
            if operation in block
        )
        for operation in cleanup_operations
    ]
    assert len(set(operation_blocks)) == len(cleanup_operations)

    fixture_start = workflow.index("large_object_fixture=$(psql", cleanup_end)
    fixture_end = workflow.index(
        'echo "Starting parameter/default-ACL drift recovery fixture..."',
        fixture_start,
    )
    fixture_setup = workflow[fixture_start:fixture_end]
    assignment = (
        "large_object_fixture=$(psql -X -v ON_ERROR_STOP=1 -t -A \\\n"
        '            -c "SELECT pg_catalog.lo_from_bytea(0, '
        "decode('00', 'hex'))\")"
    )
    assert assignment in fixture_setup
    validation = 'if [[ ! "$large_object_fixture" =~ ^[0-9]+$ ]]; then'
    export = 'export INGESTION_LARGE_OBJECT_FIXTURE="$large_object_fixture"'
    assert fixture_setup.index(assignment) < fixture_setup.index(validation)
    assert fixture_setup.index(validation) < fixture_setup.index("exit 1")
    assert fixture_setup.index("exit 1") < fixture_setup.index(export)
    assert fixture_setup.count(export) == 1


def _assert_integration_large_object_contract(integration_test: str) -> None:
    assert "SET lo_compat_privileges = on" in integration_test
    for creation_probe in (
        "SELECT pg_catalog.lo_create(0)",
        "SELECT pg_catalog.lo_creat(0)",
        "SELECT pg_catalog.lo_from_bytea(0, decode('00', 'hex'))",
    ):
        assert creation_probe in integration_test
    for token in (
        "pg_catalog.pg_largeobject_metadata",
        "large_object.lomowner",
        "large_object.lomacl",
        "acl.is_grantable",
        "transaction.rollback()",
        "pg_catalog.lo_unlink",
    ):
        assert token in integration_test


def test_ci_uses_isolated_admin_runtime_legacy_and_creator_identities() -> None:
    workflow = _read(".github/workflows/ci.yml")
    integration_test = _read(
        "integration_tests/storage/test_ingestion_runtime_privileges.py"
    )

    for token in (
        "INGESTION_RUNTIME_ADMIN_DSN",
        "INGESTION_RUNTIME_TEST_DSN",
        "INGESTION_LEGACY_TEST_DSN",
        "INGESTION_RUNTIME_EXTERNAL_POSTGRES_REQUIRED",
        "INGESTION_RUNTIME_ROLE",
        "INGESTION_LEGACY_ROLE",
        "INGESTION_RUNTIME_CREATOR_ROLE",
        "::add-mask::",
        "app_ingestion_runtime",
        "ingestion_runtime_privilege_contract.sql",
        "Starting column-ACL drift recovery fixture",
        "Starting parameter/default-ACL drift recovery fixture",
        "GRANT SELECT (slug)",
        "DROP OWNED BY",
        "Starting SET-ROLE-only default-ACL fixture",
        "ALTER DATABASE postgres",
        "ALTER ROLE ALL",
    ):
        assert token in workflow
    _assert_workflow_large_object_contract(workflow)
    trap_index = workflow.index("trap cleanup_ephemeral_roles EXIT")
    setup_begin = workflow.index("BEGIN;", trap_index)
    runtime_create = workflow.index('CREATE ROLE :"runtime_role"', setup_begin)
    setup_commit = workflow.index("COMMIT;", runtime_create)
    assert trap_index < setup_begin < runtime_create < setup_commit
    assert (
        workflow.index('CREATE ROLE :"legacy_role"', runtime_create)
        < setup_commit
    )
    assert (
        workflow.index('CREATE ROLE :"creator_role"', runtime_create)
        < setup_commit
    )
    for exact_grant in (
        'GRANT CONNECT ON DATABASE postgres TO :"legacy_role";',
        'GRANT USAGE ON SCHEMA public TO :"legacy_role";',
        'GRANT USAGE ON TYPE public.feed_status TO :"legacy_role";',
        'GRANT SELECT, UPDATE ON TABLE public.feeds TO :"legacy_role";',
        'GRANT SELECT, UPDATE ON TABLE public.feed_properties TO :"legacy_role";',
        'GRANT SELECT, INSERT ON TABLE public.feed_audit_events TO :"legacy_role";',
    ):
        assert setup_begin < workflow.index(exact_grant) < setup_commit
    cleanup_start = workflow.index("cleanup_ephemeral_roles()")
    drop_owned = workflow.index(
        'DROP OWNED BY :"cleanup_role" CASCADE;', cleanup_start
    )
    drop_role = workflow.index('DROP ROLE :"cleanup_role";', cleanup_start)
    assert cleanup_start < drop_owned < drop_role < trap_index
    assert (
        cleanup_start
        < workflow.index(
            "ALTER ROLE ALL RESET session_replication_role;",
            cleanup_start,
        )
        < trap_index
    )
    assert (
        cleanup_start
        < workflow.index(
            "ALTER ROLE ALL RESET lo_compat_privileges;",
            cleanup_start,
        )
        < trap_index
    )
    assert (
        cleanup_start
        < workflow.index("pg_catalog.lo_unlink", cleanup_start)
        < drop_owned
    )
    assert "DROP ROLE IF EXISTS" not in workflow
    assert (
        ">/dev/null"
        not in workflow[workflow.index("cleanup_ephemeral_roles") :]
    )
    assert "statement_cache_size=0" in integration_test
    assert "admin_pool" in integration_test
    assert "runtime_pool" in integration_test
    assert "legacy_pool" in integration_test
    assert "INGESTION_RUNTIME_ROLE" in integration_test
    assert "INGESTION_LEGACY_ROLE" in integration_test
    assert "INGESTION_RUNTIME_CREATOR_ROLE" in integration_test
    assert "has_column_privilege" in integration_test
    assert "if server_version >= 160000" in integration_test
    assert "future_type" in integration_test
    assert "SET session_replication_role = replica" in integration_test
    assert "current_setting('session_replication_role')" in integration_test
    _assert_integration_large_object_contract(integration_test)
    assert (
        "claim_unclaimed(\n        _SOURCE_TYPE,\n        privilege_fixtures.lease_owner,\n        1,"
        in integration_test
    )
    assert "1000" not in integration_test
    assert (
        "Lease table must be empty before the privilege suite"
        in integration_test
    )
    assert "permanent Lease tombstone" in integration_test
    fixture_signature = integration_test[
        integration_test.index(
            "async def privilege_fixtures("
        ) : integration_test.index(
            ") -> collections.abc.AsyncIterator[_PrivilegeFixtures]:",
            integration_test.index("async def privilege_fixtures("),
        )
    ]
    assert "admin_pool: asyncpg.Pool" in fixture_signature
    assert "runtime_pool" not in fixture_signature
    assert "legacy_pool" not in fixture_signature
    assert "fixture construction must use the admin pool" in integration_test
    guard_start = integration_test.index("async def _privilege_fixture_guard(")
    guard_armed = integration_test.index(
        "try:\n        yield fixtures", guard_start
    )
    setup_start = integration_test.index("async def privilege_fixtures(")
    assert guard_start < guard_armed < setup_start
    assert (
        "legacy_store = feed_store.FeedStore(\n        legacy_pool"
        in integration_test
    )
    for legacy_operation in (
        "legacy_store.acquire_feeds_batch(",
        "legacy_store.update_feed_progress(",
        "legacy_store.list_feed_history_records(",
        "legacy_store.release_feed(",
    ):
        assert legacy_operation in integration_test
