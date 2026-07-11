"""Static contracts for the dedicated ingestion runtime database role."""

from __future__ import annotations

import ast
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
_HARDENING_PATH = (
    "terraform/modules/alloydb/sql/privileges/"
    "100_ingestion_runtime_hardening.sql"
)
_CONTRACT_PATH = (
    "terraform/modules/alloydb/sql/ci/ingestion_runtime_privilege_contract.sql"
)
_LEGACY_SNAPSHOT_PATH = (
    "terraform/modules/alloydb/sql/ci/ingestion_legacy_privilege_snapshot.sql"
)
_GATE_SCRIPT_PATH = "scripts/ci/ingestion_runtime_privilege_gate.sh"
_ROLE = "app_ingestion_runtime"


def _read(path: str) -> str:
    return (_REPO_ROOT / path).read_text(encoding="utf-8")


def _normalized_sql(path: str) -> str:
    sql = re.sub(r"--.*$", "", _read(path), flags=re.MULTILINE)
    return " ".join(sql.split())


def _shared_entrypoint_sql(path: str) -> str:
    """Model psql's relative include at the wrapper's exact call position.

    Args:
        path: Repository-relative path to a privilege wrapper.

    Returns:
        Normalized wrapper SQL with the shared fragment expanded in place.
    """
    wrapper = _normalized_sql(path)
    include = r"\ir 100_ingestion_runtime_hardening.sql"
    assert wrapper.count(include) == 1
    return wrapper.replace(include, _normalized_sql(_HARDENING_PATH))


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
    assert terraform.count("psql -X -v ON_ERROR_STOP=1") >= 3
    assert terraform.count("-f /sql/privileges/") >= 2


def test_bootstrap_normalizes_a_non_login_role_without_touching_worker() -> (
    None
):
    wrapper = _normalized_sql(_BOOTSTRAP_PATH)
    sql = _shared_entrypoint_sql(_BOOTSTRAP_PATH)

    assert wrapper.startswith("BEGIN;")
    assert wrapper.endswith("COMMIT;")
    assert f"CREATE ROLE {_ROLE}" in sql
    assert (
        f"ALTER ROLE {_ROLE} NOLOGIN NOSUPERUSER NOCREATEDB "
        "NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS"
    ) in sql
    assert f"ALTER ROLE {_ROLE} PASSWORD NULL" in sql
    assert "FROM pg_catalog.pg_auth_members" in sql
    assert "REVOKE %I FROM %I" in sql
    assert "pg_temp.ingestion_runtime_reachable_roles" in sql
    assert "rolcanlogin" in sql
    assert "rolsuper" in sql
    assert "rolcreatedb" in sql
    assert "rolcreaterole" in sql
    assert "rolreplication" in sql
    assert "rolbypassrls" in sql
    assert "worker" not in sql.replace(_ROLE, "")


def test_reconciliation_revokes_before_the_four_exact_table_grants() -> None:
    sql = _shared_entrypoint_sql(_RECONCILE_PATH)

    assert sql.startswith("BEGIN;")
    assert sql.endswith("COMMIT;")
    first_grant = sql.index("GRANT SELECT, UPDATE ON TABLE")
    for revocation in (
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM %I",
        "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM %I",
        "REVOKE ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA public FROM %I",
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
            _normalized_sql(_HARDENING_PATH),
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
        assert f"('{object_class}')" in combined
    assert "REVOKE ALL PRIVILEGES ON %s FROM %I" in combined
    assert "REVOKE ALL PRIVILEGES ON %s FROM PUBLIC" in combined


def test_reconciliation_fails_closed_and_keeps_postgres_ownership() -> None:
    sql = _shared_entrypoint_sql(_RECONCILE_PATH)

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
    hardening = _normalized_sql(_HARDENING_PATH)
    contract = _normalized_sql(_CONTRACT_PATH)

    for sql in (hardening,):
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

    for sql in (hardening, contract):
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
    hardening = _normalized_sql(_HARDENING_PATH)
    contract = _normalized_sql(_CONTRACT_PATH)
    combined = f"{hardening} {contract}"

    creation_signatures = (
        "pg_catalog.lo_create(oid)",
        "pg_catalog.lo_creat(integer)",
        "pg_catalog.lo_from_bytea(oid, bytea)",
        "pg_catalog.lo_import(text)",
        "pg_catalog.lo_import(text, oid)",
    )
    exact_public_revoke = (
        "REVOKE EXECUTE ON FUNCTION "
        f"{', '.join(creation_signatures)} FROM PUBLIC CASCADE;"
    )
    assert exact_public_revoke in hardening
    assert "REVOKE SELECT, UPDATE ON LARGE OBJECT" in hardening

    for sql in (hardening, contract):
        for signature in creation_signatures:
            assert signature in sql
        assert "pg_catalog.pg_largeobject_metadata" in sql
        assert "large_object.lomowner" in sql
        assert "large_object.lomacl" in sql
        assert re.search(r"pg_catalog\.acldefault\(\s*'L'", sql)
        assert "pg_catalog.aclexplode" in sql
        assert "acl.is_grantable" in sql

        assert "large_object.lomowner = runtime_role.oid" in sql
        assert "acl.grantee = 0::OID" in sql
        assert "acl.grantee = runtime_role.oid" in sql
        assert "'USAGE'" in sql
        assert "set_role_membership_mode" in sql

    assert "pg_temp.ingestion_runtime_reachable_roles" in hardening
    assert "runtime_role.rolsuper" in hardening
    for reachability_contract in (
        "pg_catalog.pg_has_role( runtime_role.oid, "
        "large_object.lomowner, 'USAGE' )",
        "pg_catalog.pg_has_role( runtime_role.oid, "
        "large_object.lomowner, set_role_membership_mode )",
        "pg_catalog.pg_has_role( runtime_role.oid, acl.grantee, 'USAGE' )",
        "pg_catalog.pg_has_role( runtime_role.oid, acl.grantee, "
        "set_role_membership_mode )",
    ):
        assert reachability_contract in contract

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
    hardening = _normalized_sql(_HARDENING_PATH)
    contract = _normalized_sql(_CONTRACT_PATH)

    assert "pg_catalog.has_schema_privilege" in hardening
    assert "SELECT role.oid, role.rolname" in hardening
    assert "'public', 'CREATE'" in hardening
    assert "ALTER DEFAULT PRIVILEGES FOR ROLE %I" in hardening
    assert "REVOKE ALL PRIVILEGES ON %s FROM PUBLIC" in hardening
    assert "REVOKE ALL PRIVILEGES ON %s FROM %I" in hardening
    for object_class in ("TABLES", "SEQUENCES", "ROUTINES", "TYPES"):
        assert f"('{object_class}')" in hardening
    assert "set_role_membership_mode" in hardening
    assert "'SET'" in hardening
    assert "'MEMBER'" in hardening

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


def _assert_gate_script_large_object_contract(gate_script: str) -> None:
    for token in (
        "INGESTION_LARGE_OBJECT_FIXTURE",
        "GRANT SELECT, UPDATE ON LARGE OBJECT",
        "pg_catalog.lo_unlink",
        "ALTER ROLE ALL RESET lo_compat_privileges",
    ):
        assert token in gate_script
    for creation_routine in (
        "pg_catalog.lo_create(oid)",
        "pg_catalog.lo_creat(integer)",
        "pg_catalog.lo_from_bytea(oid, bytea)",
    ):
        assert creation_routine in gate_script
    for global_setting_drift in (
        "ALTER ROLE ALL SET session_replication_role = replica;",
        "ALTER ROLE ALL SET lo_compat_privileges = on;",
    ):
        assert global_setting_drift in gate_script

    schema_cleanup = gate_script[
        gate_script.index(
            "cleanup_schema_large_object_fixture()"
        ) : gate_script.index("apply_schema_gate()")
    ]
    assert "REVOKE SELECT (slug) ON TABLE public.source_types" in schema_cleanup
    assert "REVOKE UPDATE (slug) ON TABLE public.source_types" in schema_cleanup
    assert "Failed to clean apply-schema column ACL drift" in schema_cleanup

    cleanup_start = gate_script.index("cleanup_ephemeral_roles()")
    cleanup_end = gate_script.index("create_ephemeral_roles()")
    cleanup = gate_script[cleanup_start:cleanup_end]
    guarded_psql_blocks = [
        match.group(1)
        for match in re.finditer(
            r"if ! psql_gate <<'SQL'\n"
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
    assert all(
        any(operation in block for block in guarded_psql_blocks)
        for operation in cleanup_operations
    )
    settings_block = next(
        index
        for index, block in enumerate(guarded_psql_blocks)
        if cleanup_operations[0] in block
    )
    unlink_block = next(
        index
        for index, block in enumerate(guarded_psql_blocks)
        if cleanup_operations[2] in block
    )
    assert settings_block != unlink_block

    fixture_start = gate_script.index(
        "large_object_fixture=$(psql_gate",
        cleanup_end,
    )
    fixture_end = gate_script.index(
        'echo "Starting parameter/default-ACL drift recovery fixture..."',
        fixture_start,
    )
    fixture_setup = gate_script[fixture_start:fixture_end]
    assignment = (
        "large_object_fixture=$(psql_gate -t -A \\\n"
        '    -c "SELECT pg_catalog.lo_from_bytea(0, '
        "decode('00', 'hex'))\")"
    )
    assert assignment in fixture_setup
    validation = 'if [[ ! "$large_object_fixture" =~ ^[0-9]+$ ]]; then'
    export = 'export INGESTION_LARGE_OBJECT_FIXTURE="$large_object_fixture"'
    assert fixture_setup.index(assignment) < fixture_setup.index(validation)
    assert fixture_setup.index(validation) < fixture_setup.index("return 1")
    assert fixture_setup.index("return 1") < fixture_setup.index(export)
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


def _assert_gate_fixture_lifecycle(gate_script: str) -> None:
    """Check trap ordering, setup identities, grants, and durable cleanup.

    Args:
        gate_script: Complete ingestion privilege gate source.
    """
    setup_function = gate_script.index("create_ephemeral_roles()")
    setup_begin = gate_script.index("BEGIN;", setup_function)
    runtime_create = gate_script.index(
        'CREATE ROLE :"runtime_role"',
        setup_begin,
    )
    setup_commit = gate_script.index("COMMIT;", runtime_create)
    assert setup_function < setup_begin < runtime_create < setup_commit

    probe = gate_script[
        gate_script.index("run_early_exit_cleanup_probe()") : gate_script.index(
            "reconcile_with_legacy()"
        )
    ]
    assert probe.index("trap cleanup_ephemeral_roles EXIT") < probe.index(
        "create_ephemeral_roles"
    )
    dedicated = gate_script[gate_script.index("dedicated_proof_gate()") :]
    assert dedicated.index(
        "trap cleanup_ephemeral_roles EXIT"
    ) < dedicated.index("create_ephemeral_roles")

    for identity in (
        "legacy_role",
        "legacy_parent_role",
        "creator_role",
    ):
        assert (
            gate_script.index(f'CREATE ROLE :"{identity}"', runtime_create)
            < setup_commit
        )
    for exact_grant in (
        'GRANT CONNECT ON DATABASE postgres TO :"legacy_role";',
        'GRANT USAGE ON SCHEMA public TO :"legacy_role";',
        'GRANT USAGE ON TYPE public.feed_status TO :"legacy_role";',
        'GRANT SELECT, UPDATE ON TABLE public.feeds TO :"legacy_role";',
        'GRANT SELECT ON TABLE public.feed_properties TO :"legacy_role";',
        (
            "GRANT UPDATE ON TABLE public.feed_properties "
            'TO :"legacy_parent_role";'
        ),
        (
            "GRANT SELECT, INSERT ON TABLE public.feed_audit_events "
            'TO :"legacy_role";'
        ),
    ):
        assert setup_begin < gate_script.index(exact_grant) < setup_commit

    cleanup_start = gate_script.index("cleanup_ephemeral_roles()")
    drop_owned = gate_script.index(
        'DROP OWNED BY :"cleanup_role" CASCADE;',
        cleanup_start,
    )
    drop_role = gate_script.index('DROP ROLE :"cleanup_role";', cleanup_start)
    assert cleanup_start < drop_owned < drop_role < setup_function
    for cleanup_operation in (
        "ALTER ROLE ALL RESET session_replication_role;",
        "ALTER ROLE ALL RESET lo_compat_privileges;",
    ):
        assert (
            cleanup_start
            < gate_script.index(cleanup_operation, cleanup_start)
            < setup_function
        )
    assert (
        cleanup_start
        < gate_script.index("pg_catalog.lo_unlink", cleanup_start)
        < drop_owned
    )
    assert "DROP ROLE IF EXISTS" not in gate_script
    assert (
        ">/dev/null"
        not in gate_script[gate_script.index("cleanup_ephemeral_roles") :]
    )


def _assert_integration_privilege_module_contract(
    integration_test: str,
) -> None:
    """Check integration identity and fixture safety contracts.

    Args:
        integration_test: Complete privilege integration module source.
    """
    for token in (
        "statement_cache_size=0",
        "admin_pool",
        "runtime_pool",
        "legacy_pool",
        "INGESTION_RUNTIME_ROLE",
        "INGESTION_LEGACY_ROLE",
        "INGESTION_LEGACY_PARENT_ROLE",
        "INGESTION_RUNTIME_CREATOR_ROLE",
        "has_column_privilege",
        "if server_version >= 160000",
        "future_type",
        "SET session_replication_role = replica",
        "current_setting('session_replication_role')",
    ):
        assert token in integration_test
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

    fixture_start = integration_test.index("async def privilege_fixtures(")
    fixture_signature = integration_test[
        fixture_start : integration_test.index(
            ") -> collections.abc.AsyncIterator[_PrivilegeFixtures]:",
            fixture_start,
        )
    ]
    assert "admin_pool: asyncpg.Pool" in fixture_signature
    assert "runtime_pool" not in fixture_signature
    assert "legacy_pool" not in fixture_signature
    assert "fixture construction must use the admin pool" in integration_test
    guard_start = integration_test.index("async def _privilege_fixture_guard(")
    guard_armed = integration_test.index(
        "try:\n        yield fixtures",
        guard_start,
    )
    assert guard_start < guard_armed < fixture_start
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


def test_ci_uses_isolated_admin_runtime_legacy_and_creator_identities() -> None:
    workflow = _read(".github/workflows/ci.yml")
    gate_script = _read(_GATE_SCRIPT_PATH)
    integration_test = _read(
        "integration_tests/storage/test_ingestion_runtime_privileges.py"
    )

    assert workflow.count(_GATE_SCRIPT_PATH) == 2
    assert f"run: {_GATE_SCRIPT_PATH} apply-schema" in workflow
    assert f"run: {_GATE_SCRIPT_PATH} prove" in workflow
    assert "EXPECTED_POSTGRES_MAJOR: ${{ matrix.postgres-version }}" in workflow

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
        assert token in gate_script
    _assert_gate_script_large_object_contract(gate_script)
    _assert_gate_fixture_lifecycle(gate_script)
    _assert_integration_privilege_module_contract(integration_test)


def test_r3_shared_hardening_is_uploaded_hashed_and_included_once() -> None:
    hardening_path = _REPO_ROOT / _HARDENING_PATH
    assert hardening_path.is_file(), "the shared privilege fragment is missing"
    hardening = _normalized_sql(_HARDENING_PATH)
    bootstrap = _read(_BOOTSTRAP_PATH)
    reconcile = _read(_RECONCILE_PATH)
    contract = _normalized_sql(_CONTRACT_PATH)
    terraform = _read("terraform/modules/alloydb/main.tf")

    include = r"\ir 100_ingestion_runtime_hardening.sql"
    assert bootstrap.count(include) == 1
    assert reconcile.count(include) == 1
    assert "100_ingestion_runtime_hardening.sql" in terraform
    assert "local.privilege_sql_files" in terraform
    assert "schema_sql_combined" in terraform
    assert "google_storage_bucket_object.privilege_sql" in terraform

    assert hardening.count("WITH RECURSIVE runtime_roles AS") == 1
    assert "CREATE TEMP TABLE ingestion_runtime_roles" in hardening
    assert "ON COMMIT DROP" in hardening
    assert "WITH RECURSIVE runtime_roles AS" not in bootstrap
    assert "WITH RECURSIVE runtime_roles AS" not in reconcile
    assert contract.count("WITH RECURSIVE runtime_roles AS") == 1
    assert "CREATE TEMP TABLE ingestion_contract_runtime_roles" in contract

    cloud_command = terraform[
        terraform.index('image   = "postgres:16-alpine"') :
    ].replace(r"\"", '"')
    assert '-v legacy_role="$DB_LEGACY_ROLE"' in cloud_command
    assert cloud_command.count("-f /sql/privileges/") >= 2
    assert "DB_LEGACY_ROLE" in cloud_command
    assert 'var.create_worker_user ? var.worker_user_id : ""' in terraform
    assert "google_alloydb_user.worker" in terraform


def test_r3_models_all_five_server_side_large_object_creators() -> None:
    hardening = (
        _normalized_sql(_HARDENING_PATH)
        if (_REPO_ROOT / _HARDENING_PATH).is_file()
        else ""
    )
    contract = _normalized_sql(_CONTRACT_PATH)
    gate_script = _read(_GATE_SCRIPT_PATH)
    integration = _read(
        "integration_tests/storage/test_ingestion_runtime_privileges.py"
    )
    signatures = (
        "pg_catalog.lo_create(oid)",
        "pg_catalog.lo_creat(integer)",
        "pg_catalog.lo_from_bytea(oid, bytea)",
        "pg_catalog.lo_import(text)",
        "pg_catalog.lo_import(text, oid)",
    )
    exact_revoke = (
        "REVOKE EXECUTE ON FUNCTION "
        f"{', '.join(signatures)} FROM PUBLIC CASCADE;"
    )
    assert exact_revoke in hardening
    for source in (hardening, contract, gate_script):
        for signature in signatures:
            assert signature in source
    for probe in (
        "SELECT pg_catalog.lo_import('/etc/hosts')",
        "pg_catalog.lo_import('/etc/hosts',",
    ):
        assert probe in integration
    assert integration.count("transaction.rollback()") >= 1
    assert "unexpected_oid" in integration


def test_r3_legacy_identity_preserves_only_preexisting_effective_rights() -> (
    None
):
    hardening_source = (
        _read(_HARDENING_PATH)
        if (_REPO_ROOT / _HARDENING_PATH).is_file()
        else ""
    )
    hardening = " ".join(hardening_source.split())
    terraform = _read("terraform/modules/alloydb/main.tf")
    gate_script = _read(_GATE_SCRIPT_PATH)
    snapshot_oracle = _read(_LEGACY_SNAPSHOT_PATH)

    for contract in (
        r"\if :{?legacy_role}",
        r"\set ON_ERROR_STOP on",
        "pg_temp.ingestion_legacy_capability_snapshot",
        "legacy preservation role must be empty or a simple identifier",
        "legacy preservation role does not exist",
        (
            "legacy preservation role must be a distinct non-admin login "
            "outside the app ingestion closure"
        ),
        "legacy capability changed during ingestion hardening",
    ):
        assert contract in hardening_source
    assert r"\quit" not in hardening_source
    for surface in (
        "database",
        "schema",
        "relation",
        "column",
        "sequence",
        "routine",
        "type",
        "parameter",
        "startup_setting",
        "large_object_creator",
        "large_object",
    ):
        assert f"'{surface}'" in hardening
    snapshot = hardening.index(
        "INSERT INTO pg_temp.ingestion_legacy_capability_snapshot"
    )
    first_revoke = hardening.index("REVOKE", snapshot)
    restore = hardening.index("legacy_capability_restore", first_revoke)
    equality = hardening.index(
        "legacy capability changed during ingestion hardening",
        restore,
    )
    assert snapshot < first_revoke < restore < equality

    terraform_command = terraform.replace(r"\"", '"')
    assert terraform_command.count('-v legacy_role="$DB_LEGACY_ROLE"') == 2
    assert "google_alloydb_user.worker" in terraform
    assert (
        'value = var.create_worker_user ? var.worker_user_id : ""' in terraform
    )
    assert "jsonencode(sort(var.worker_database_roles))" in terraform
    assert '-v legacy_role="$legacy_role"' in gate_script
    assert "legacy_capabilities_before" in gate_script
    assert '"$legacy_capabilities_before" != "$after"' in gate_script
    assert "-v legacy_role=" in gate_script
    assert "INGESTION_LEGACY_PARENT_ROLE" in gate_script
    assert "assert_legacy_parent_path_preserved" in gate_script
    assert "pg_temp.ingestion_legacy_effective_roles" in hardening
    assert "pg_temp.ci_legacy_effective_roles" in snapshot_oracle
    assert "FROM pg_catalog.pg_settings AS setting" in snapshot_oracle
    assert r"\set ON_ERROR_STOP on" in snapshot_oracle
    assert r"\quit" not in snapshot_oracle


def test_r3_legacy_lo_compat_startup_capabilities_are_preserved() -> None:
    hardening = _normalized_sql(_HARDENING_PATH)
    snapshot_oracle = _normalized_sql(_LEGACY_SNAPSHOT_PATH)
    contract = _normalized_sql(_CONTRACT_PATH)
    gate_script = _read(_GATE_SCRIPT_PATH)

    for sql in (hardening, snapshot_oracle):
        assert "legacy_startup_setting" in sql
        assert "pg_catalog.pg_db_role_setting" in sql
        assert "role_setting.setrole = legacy.oid" in sql
        assert "role_setting.setdatabase = database.oid" in sql
        assert "role_setting.setdatabase = 0::OID" in sql
        assert "pg_catalog.pg_settings" in sql
        assert "'configuration file'" in sql
        assert "'environment variable'" in sql
        assert "'command line'" in sql
        assert "legacy lo_compat_privileges default is ambiguous" in sql
        assert "legacy session_replication_role default is ambiguous" in sql
        assert "privilege.name IN ('SELECT', 'UPDATE')" in sql
        assert "'startup_setting'" in sql

    assert "legacy_lo_compat_and_replication_preserve" in hardening
    assert "ALTER ROLE %I IN DATABASE %I SET %I = %L" in hardening
    assert "runtime_safe_role_settings" in hardening
    assert "ALTER DATABASE %I SET lo_compat_privileges = off" in hardening
    assert (
        "ALTER DATABASE %I SET session_replication_role = origin" in hardening
    )
    assert "SET lo_compat_privileges = off" in hardening
    assert "SET session_replication_role = origin" in hardening

    for sql in (hardening, contract):
        assert "configuration_value" in sql
        assert "lo_compat_privileges" in sql
        assert "session_replication_role" in sql
        assert "safe runtime database parameter defaults are missing" in sql
        assert "safe runtime role parameter defaults are missing" in sql

    for token in (
        "INGESTION_LEGACY_LO_COMPAT_FIXTURE",
        "Starting lower-source parameter override fixture",
        "ALTER SYSTEM SET lo_compat_privileges = on",
        "ALTER DATABASE postgres SET lo_compat_privileges = off",
        "Starting legacy lo_compat_privileges preservation fixture",
        "ALTER ROLE ALL SET lo_compat_privileges = on",
        "ALTER ROLE ALL SET session_replication_role = replica",
        "INGESTION_RUNTIME_FUTURE_ROLE",
    ):
        assert token in gate_script


def test_r3_exit_trap_independently_reverses_every_persistent_drift() -> None:
    gate_script = _read(_GATE_SCRIPT_PATH)
    cleanup_start = gate_script.index("cleanup_ephemeral_roles()")
    cleanup_end = gate_script.index("create_ephemeral_roles()")
    cleanup = gate_script[cleanup_start:cleanup_end]
    guarded_psql_blocks = [
        match.group(1)
        for match in re.finditer(
            r"if ! psql_gate <<'SQL'\n"
            r"(.*?)\n\s*SQL\n\s*then",
            cleanup,
            flags=re.DOTALL,
        )
    ]
    cleanup_tags = (
        "ALTER SYSTEM RESET session_replication_role",
        "ALTER ROLE ALL RESET session_replication_role",
        "REVOKE SET, ALTER SYSTEM ON PARAMETER session_replication_role",
        "REVOKE EXECUTE ON FUNCTION",
        "REVOKE SELECT, UPDATE ON LARGE OBJECT %s",
        "ALTER DEFAULT PRIVILEGES FOR ROLE %I",
        "WITH parent(rolname) AS",
        "DROP COLLATION IF EXISTS",
        "SELECT pg_catalog.lo_unlink",
    )
    tag_blocks = [
        next(
            index
            for index, block in enumerate(guarded_psql_blocks)
            if cleanup_tag in block
        )
        for cleanup_tag in cleanup_tags
    ]
    assert len(set(tag_blocks)) == len(cleanup_tags)
    creator_cleanup = guarded_psql_blocks[
        tag_blocks[cleanup_tags.index("REVOKE EXECUTE ON FUNCTION")]
    ]
    for signature in (
        "pg_catalog.lo_create(oid)",
        "pg_catalog.lo_creat(integer)",
        "pg_catalog.lo_from_bytea(oid, bytea)",
        "pg_catalog.lo_import(text)",
        "pg_catalog.lo_import(text, oid)",
    ):
        assert signature in creator_cleanup
    assert "run_early_exit_cleanup_probe" in gate_script
    assert "early_exit_status" in gate_script
    assert "exit 86" in gate_script
    assert "assert_privilege_drift_absent" in gate_script


def test_r3_adversarial_ci_covers_public_direct_inherited_and_set_import() -> (
    None
):
    gate_script = _read(_GATE_SCRIPT_PATH)
    for token in (
        "Starting legacy capability preservation fixture",
        "Starting inherited large-object creator fixture",
        "Starting SET-ROLE-only large-object creator fixture",
        "pg_catalog.lo_import(text)",
        "pg_catalog.lo_import(text, oid)",
        'TO PUBLIC, app_ingestion_runtime, :"runtime_role"',
        'TO :"inherited_role" WITH GRANT OPTION',
        'TO :"set_role" WITH GRANT OPTION',
        "legacy_capabilities_before",
        "assert_legacy_capabilities_unchanged",
        "assert_legacy_parent_path_preserved",
    ):
        assert token in gate_script
    assert 'if [ "$version" -ge 160000 ]' in gate_script


def test_r3_python_privilege_helpers_follow_repository_standards() -> None:
    source = _read(
        "integration_tests/storage/test_ingestion_runtime_privileges.py"
    )
    module = ast.parse(source)
    assert "import typing" in source
    assert "from typing import TYPE_CHECKING" not in source
    assert "if typing.TYPE_CHECKING:" in source
    assert "async def _effective_role_privileges(" in source
    assert source.count("await _effective_role_privileges(") >= 2

    required_docstrings = {
        "_configured_value",
        "_dsn",
        "_configured_identifier",
        "_configured_oid",
        "_role_name",
        "_large_object_creation_privileges",
        "_large_object_capabilities",
        "_assert_no_large_object_capabilities",
        "_effective_role_privileges",
        "_fixture_identifier",
        "admin_pool",
        "runtime_pool",
        "legacy_pool",
        "_privilege_fixture_guard",
        "privilege_fixtures",
    }
    functions = {
        node.name: node
        for node in ast.walk(module)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert required_docstrings <= functions.keys()
    for name in required_docstrings:
        assert ast.get_docstring(functions[name]), f"{name} needs a docstring"
    for fixture in (
        "admin_pool",
        "runtime_pool",
        "legacy_pool",
        "_privilege_fixture_guard",
        "privilege_fixtures",
    ):
        fixture_docstring = ast.get_docstring(functions[fixture])
        assert fixture_docstring is not None
        assert "Yields:" in fixture_docstring
