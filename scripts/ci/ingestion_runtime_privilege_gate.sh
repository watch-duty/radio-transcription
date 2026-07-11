#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../.." && pwd)
privilege_dir="$repo_root/terraform/modules/alloydb/sql/privileges"
ci_sql_dir="$repo_root/terraform/modules/alloydb/sql/ci"
ingestion_dir="$repo_root/terraform/modules/alloydb/sql/ingestion"
bootstrap_sql="$privilege_dir/000_ingestion_runtime_bootstrap.sql"
reconcile_sql="$privilege_dir/999_ingestion_runtime_reconcile.sql"
contract_sql="$ci_sql_dir/ingestion_runtime_privilege_contract.sql"
legacy_snapshot_sql="$ci_sql_dir/ingestion_legacy_privilege_snapshot.sql"

psql_gate() {
  psql -X -v ON_ERROR_STOP=1 "$@"
}

server_version_num() {
  psql_gate -t -A -c "SHOW server_version_num"
}

assert_server_major() {
  local version major
  : "${EXPECTED_POSTGRES_MAJOR:?EXPECTED_POSTGRES_MAJOR must be set}"
  version=$(server_version_num)
  if [[ ! "$version" =~ ^[0-9]+$ ]]; then
    echo "::error::Unexpected server_version_num: $version"
    return 1
  fi
  major=$((version / 10000))
  if [ "$major" -ne "$EXPECTED_POSTGRES_MAJOR" ]; then
    echo "::error::PostgreSQL server major $major does not match $EXPECTED_POSTGRES_MAJOR"
    return 1
  fi
}

cleanup_schema_large_object_fixture() {
  local original_status=$?
  local cleanup_status=0
  trap - EXIT
  set +e
  if [ -n "${schema_large_object_fixture:-}" ]; then
    if ! psql_gate \
      -v large_object_fixture="$schema_large_object_fixture" <<'SQL'
REVOKE EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    FROM PUBLIC, app_ingestion_runtime CASCADE;
REVOKE SELECT, UPDATE ON LARGE OBJECT :large_object_fixture
    FROM PUBLIC, app_ingestion_runtime CASCADE;
SELECT pg_catalog.lo_unlink(large_object.oid)
FROM pg_catalog.pg_largeobject_metadata AS large_object
WHERE large_object.oid = :large_object_fixture::OID;
SQL
    then
      echo "::error::Failed to clean the apply-schema large-object fixture"
      cleanup_status=1
    fi
  fi
  if ! psql_gate <<'SQL'
SELECT 'REVOKE SELECT (slug) ON TABLE public.source_types '
       'FROM app_ingestion_runtime CASCADE'
WHERE EXISTS (
    SELECT 1
    FROM pg_catalog.pg_attribute AS attribute
    WHERE attribute.attrelid =
          pg_catalog.to_regclass('public.source_types')
      AND attribute.attname = 'slug'
      AND NOT attribute.attisdropped
)
  AND EXISTS (
      SELECT 1 FROM pg_catalog.pg_roles
      WHERE rolname = 'app_ingestion_runtime'
  ) \gexec
SELECT 'REVOKE UPDATE (slug) ON TABLE public.source_types '
       'FROM PUBLIC CASCADE'
WHERE EXISTS (
    SELECT 1
    FROM pg_catalog.pg_attribute AS attribute
    WHERE attribute.attrelid =
          pg_catalog.to_regclass('public.source_types')
      AND attribute.attname = 'slug'
      AND NOT attribute.attisdropped
) \gexec
SQL
  then
    echo "::error::Failed to clean apply-schema column ACL drift"
    cleanup_status=1
  fi
  if [ "$original_status" -ne 0 ]; then
    exit "$original_status"
  fi
  exit "$cleanup_status"
}

apply_schema_gate() (
  assert_server_major
  schema_large_object_fixture=""
  trap cleanup_schema_large_object_fixture EXIT

  psql_gate -v legacy_role= -f "$bootstrap_sql"
  for migration in "$ingestion_dir"/*.sql; do
    case "$migration" in
      *pg_cron*)
        echo "Skipping $migration (pg_cron extension unavailable)"
        continue
        ;;
    esac
    echo "Applying $migration..."
    psql_gate -f "$migration"
  done
  psql_gate -v legacy_role= -f "$reconcile_sql"

  echo "Starting PUBLIC/direct large-object creator drift fixture..."
  schema_large_object_fixture=$(psql_gate -t -A \
    -c "SELECT pg_catalog.lo_from_bytea(0, decode('00', 'hex'))")
  if [[ ! "$schema_large_object_fixture" =~ ^[0-9]+$ ]]; then
    echo "::error::Unexpected large-object fixture identity: $schema_large_object_fixture"
    return 1
  fi
  psql_gate -v large_object_fixture="$schema_large_object_fixture" <<'SQL'
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    TO PUBLIC;
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    TO app_ingestion_runtime WITH GRANT OPTION;
GRANT SELECT, UPDATE ON LARGE OBJECT :large_object_fixture TO PUBLIC;
GRANT SELECT, UPDATE ON LARGE OBJECT :large_object_fixture
    TO app_ingestion_runtime WITH GRANT OPTION;
SQL
  psql_gate -v legacy_role= -f "$reconcile_sql"
  psql_gate -v large_object_fixture="$schema_large_object_fixture" <<'SQL'
DO $public_creator_drift_closed$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc AS procedure
        WHERE procedure.oid IN (
            'pg_catalog.lo_create(oid)'::pg_catalog.regprocedure,
            'pg_catalog.lo_creat(integer)'::pg_catalog.regprocedure,
            'pg_catalog.lo_from_bytea(oid, bytea)'::pg_catalog.regprocedure,
            'pg_catalog.lo_import(text)'::pg_catalog.regprocedure,
            'pg_catalog.lo_import(text, oid)'::pg_catalog.regprocedure
        )
          AND (
              pg_catalog.has_function_privilege(
                  'app_ingestion_runtime', procedure.oid, 'EXECUTE'
              )
              OR EXISTS (
                  SELECT 1
                  FROM pg_catalog.aclexplode(COALESCE(
                      procedure.proacl,
                      pg_catalog.acldefault('f'::"char", procedure.proowner)
                  )) AS acl
                  WHERE acl.grantee = 0::OID
                    AND acl.privilege_type = 'EXECUTE'
              )
          )
    ) THEN
        RAISE EXCEPTION 'PUBLIC/direct creator drift survived reconciliation';
    END IF;
END
$public_creator_drift_closed$;
SELECT pg_catalog.lo_unlink(:large_object_fixture::OID);
SQL
  schema_large_object_fixture=""

  echo "Starting column-ACL drift recovery fixture..."
  psql_gate \
    -c "GRANT SELECT (slug) ON TABLE public.source_types TO app_ingestion_runtime WITH GRANT OPTION" \
    -c "GRANT UPDATE (slug) ON TABLE public.source_types TO PUBLIC"
  psql_gate -v legacy_role= -f "$reconcile_sql"
  trap - EXIT
)

runtime_role=""
legacy_role=""
legacy_parent_role=""
creator_role=""
inherited_role=""
set_role=""
future_role=""
ownership_fixture=""
large_object_fixture=""
legacy_lo_compat_fixture=""
runtime_password=""
legacy_password=""

export_fixture_environment() {
  export INGESTION_RUNTIME_ROLE="$runtime_role"
  export INGESTION_LEGACY_ROLE="$legacy_role"
  export INGESTION_LEGACY_PARENT_ROLE="$legacy_parent_role"
  export INGESTION_RUNTIME_CREATOR_ROLE="$creator_role"
  export INGESTION_RUNTIME_INHERITED_ROLE="$inherited_role"
  export INGESTION_RUNTIME_SET_ROLE="$set_role"
  export INGESTION_RUNTIME_FUTURE_ROLE="$future_role"
  export INGESTION_OWNERSHIP_FIXTURE="$ownership_fixture"
  export INGESTION_LARGE_OBJECT_FIXTURE="$large_object_fixture"
  export INGESTION_LEGACY_LO_COMPAT_FIXTURE="$legacy_lo_compat_fixture"
  export INGESTION_RUNTIME_PASSWORD="$runtime_password"
  export INGESTION_LEGACY_PASSWORD="$legacy_password"
}

cleanup_ephemeral_roles() {
  local original_status=$?
  local cleanup_status=0
  local role_name role_exists
  trap - EXIT
  set +e

  if ! psql_gate <<'SQL'
ALTER SYSTEM RESET session_replication_role;
ALTER SYSTEM RESET lo_compat_privileges;
SELECT pg_catalog.pg_reload_conf();
SQL
  then
    echo "::error::Failed to remove server-parameter drift"
    cleanup_status=1
  fi

  if ! psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv legacy_role INGESTION_LEGACY_ROLE
ALTER ROLE ALL RESET session_replication_role;
ALTER ROLE ALL RESET lo_compat_privileges;
ALTER DATABASE postgres SET session_replication_role = origin;
ALTER DATABASE postgres SET lo_compat_privileges = off;
SELECT pg_catalog.format('ALTER ROLE %I RESET session_replication_role', rolname)
FROM pg_catalog.pg_roles WHERE rolname = :'runtime_role' \gexec
SELECT pg_catalog.format('ALTER ROLE %I RESET lo_compat_privileges', rolname)
FROM pg_catalog.pg_roles WHERE rolname = :'runtime_role' \gexec
SELECT pg_catalog.format(
    'ALTER ROLE %I IN DATABASE postgres RESET session_replication_role', rolname
)
FROM pg_catalog.pg_roles WHERE rolname = :'runtime_role' \gexec
SELECT pg_catalog.format(
    'ALTER ROLE %I IN DATABASE postgres RESET lo_compat_privileges', rolname
)
FROM pg_catalog.pg_roles WHERE rolname = :'runtime_role' \gexec
SELECT pg_catalog.format(
    'ALTER ROLE %I IN DATABASE postgres RESET session_replication_role',
    rolname
)
FROM pg_catalog.pg_roles WHERE rolname = :'legacy_role' \gexec
SELECT pg_catalog.format(
    'ALTER ROLE %I IN DATABASE postgres RESET lo_compat_privileges', rolname
)
FROM pg_catalog.pg_roles WHERE rolname = :'legacy_role' \gexec
SQL
  then
    echo "::error::Failed to remove role/default parameter drift"
    cleanup_status=1
  fi

  if ! psql_gate <<'SQL'
\getenv large_object_fixture INGESTION_LARGE_OBJECT_FIXTURE
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv legacy_role INGESTION_LEGACY_ROLE
\getenv legacy_parent_role INGESTION_LEGACY_PARENT_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
\getenv inherited_role INGESTION_RUNTIME_INHERITED_ROLE
\getenv set_role INGESTION_RUNTIME_SET_ROLE
WITH fixture AS (
    SELECT :'large_object_fixture' AS oid_text
    WHERE :'large_object_fixture' ~ '^[0-9]+$'
), command AS (
    SELECT pg_catalog.format(
        'REVOKE SELECT, UPDATE ON LARGE OBJECT %s '
        'FROM PUBLIC, app_ingestion_runtime CASCADE', oid_text
    ) AS sql
    FROM fixture
    WHERE EXISTS (
        SELECT 1 FROM pg_catalog.pg_largeobject_metadata
        WHERE oid = oid_text::OID
    )
)
SELECT sql FROM command \gexec
WITH fixture AS (
    SELECT :'large_object_fixture' AS oid_text
    WHERE :'large_object_fixture' ~ '^[0-9]+$'
), grantee(rolname) AS (
    VALUES
        (:'runtime_role'), (:'legacy_role'), (:'legacy_parent_role'),
        (:'creator_role'),
        (:'inherited_role'), (:'set_role')
)
SELECT pg_catalog.format(
    'REVOKE SELECT, UPDATE ON LARGE OBJECT %s FROM %I CASCADE',
    fixture.oid_text,
    grantee.rolname
)
FROM fixture
CROSS JOIN grantee
WHERE EXISTS (
    SELECT 1 FROM pg_catalog.pg_largeobject_metadata
    WHERE oid = fixture.oid_text::OID
)
  AND EXISTS (
      SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = grantee.rolname
  ) \gexec
SQL
  then
    echo "::error::Failed to remove large-object ACL drift"
    cleanup_status=1
  fi

  if ! psql_gate <<'SQL'
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
\getenv runtime_role INGESTION_RUNTIME_ROLE
WITH object_class(name) AS (
    VALUES ('TABLES'), ('SEQUENCES'), ('ROUTINES'), ('TYPES')
), grantee(name, is_public) AS (
    VALUES
        ('PUBLIC', TRUE),
        ('app_ingestion_runtime', FALSE),
        (:'runtime_role', FALSE)
)
SELECT pg_catalog.format(
    'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
    'REVOKE ALL PRIVILEGES ON %s FROM %s',
    :'creator_role',
    object_class.name,
    CASE WHEN grantee.is_public THEN 'PUBLIC'
         ELSE pg_catalog.quote_ident(grantee.name) END
)
FROM object_class
CROSS JOIN grantee
WHERE EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'creator_role'
)
  AND (
      grantee.is_public
      OR EXISTS (
          SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = grantee.name
      )
  )
UNION ALL
SELECT pg_catalog.format(
    'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
    'REVOKE ALL PRIVILEGES ON %s FROM %s',
    :'creator_role',
    object_class.name,
    CASE WHEN grantee.is_public THEN 'PUBLIC'
         ELSE pg_catalog.quote_ident(grantee.name) END
)
FROM object_class
CROSS JOIN grantee
WHERE EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'creator_role'
)
  AND (
      grantee.is_public
      OR EXISTS (
          SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = grantee.name
      )
  ) \gexec
SQL
  then
    echo "::error::Failed to remove default ACL drift"
    cleanup_status=1
  fi

  if ! psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv legacy_role INGESTION_LEGACY_ROLE
\getenv legacy_parent_role INGESTION_LEGACY_PARENT_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
\getenv inherited_role INGESTION_RUNTIME_INHERITED_ROLE
\getenv set_role INGESTION_RUNTIME_SET_ROLE
\getenv future_role INGESTION_RUNTIME_FUTURE_ROLE
WITH parent(rolname) AS (
    VALUES (:'creator_role'), (:'inherited_role'), (:'set_role')
)
SELECT pg_catalog.format(
    'REVOKE %I FROM %I', parent.rolname, :'runtime_role'
)
FROM parent
WHERE EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'runtime_role'
)
  AND EXISTS (
      SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = parent.rolname
  ) \gexec
SELECT pg_catalog.format(
    'ALTER ROLE %I NOCREATEDB NOCREATEROLE INHERIT', rolname
)
FROM pg_catalog.pg_roles WHERE rolname = :'runtime_role' \gexec
SELECT pg_catalog.format(
    'REVOKE %I FROM %I', :'legacy_parent_role', :'legacy_role'
)
WHERE EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'legacy_role'
)
  AND EXISTS (
      SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'legacy_parent_role'
  ) \gexec
SELECT pg_catalog.format(
    'REVOKE app_ingestion_runtime FROM %I', :'future_role'
)
WHERE EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'future_role'
) \gexec
SQL
  then
    echo "::error::Failed to remove membership drift"
    cleanup_status=1
  fi

  if ! psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv legacy_role INGESTION_LEGACY_ROLE
\getenv legacy_parent_role INGESTION_LEGACY_PARENT_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
\getenv inherited_role INGESTION_RUNTIME_INHERITED_ROLE
\getenv set_role INGESTION_RUNTIME_SET_ROLE
REVOKE SET, ALTER SYSTEM ON PARAMETER session_replication_role
    FROM PUBLIC, app_ingestion_runtime CASCADE;
REVOKE SET, ALTER SYSTEM ON PARAMETER lo_compat_privileges
    FROM PUBLIC, app_ingestion_runtime CASCADE;
WITH parameter(name) AS (
    VALUES ('session_replication_role'), ('lo_compat_privileges')
), grantee(rolname) AS (
    VALUES
        (:'runtime_role'), (:'legacy_role'), (:'legacy_parent_role'),
        (:'creator_role'),
        (:'inherited_role'), (:'set_role')
)
SELECT pg_catalog.format(
    'REVOKE SET, ALTER SYSTEM ON PARAMETER %I FROM %I CASCADE',
    parameter.name,
    grantee.rolname
)
FROM parameter
CROSS JOIN grantee
WHERE EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = grantee.rolname
) \gexec
SQL
  then
    echo "::error::Failed to remove parameter ACL drift"
    cleanup_status=1
  fi

  if ! psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv legacy_role INGESTION_LEGACY_ROLE
\getenv legacy_parent_role INGESTION_LEGACY_PARENT_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
\getenv inherited_role INGESTION_RUNTIME_INHERITED_ROLE
\getenv set_role INGESTION_RUNTIME_SET_ROLE
REVOKE EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    FROM PUBLIC, app_ingestion_runtime CASCADE;
WITH grantee(rolname) AS (
    VALUES
        (:'runtime_role'), (:'legacy_role'), (:'legacy_parent_role'),
        (:'creator_role'),
        (:'inherited_role'), (:'set_role')
)
SELECT pg_catalog.format(
    'REVOKE EXECUTE ON FUNCTION '
    'pg_catalog.lo_create(oid), pg_catalog.lo_creat(integer), '
    'pg_catalog.lo_from_bytea(oid, bytea), pg_catalog.lo_import(text), '
    'pg_catalog.lo_import(text, oid) FROM %I CASCADE',
    grantee.rolname
)
FROM grantee
WHERE EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = grantee.rolname
) \gexec
SQL
  then
    echo "::error::Failed to remove creator ACL drift"
    cleanup_status=1
  fi

  if ! psql_gate <<'SQL'
\getenv ownership_fixture INGESTION_OWNERSHIP_FIXTURE
DROP COLLATION IF EXISTS public.:"ownership_fixture";
REVOKE CREATE ON SCHEMA public FROM app_ingestion_runtime;
SQL
  then
    echo "::error::Failed to remove ownership drift"
    cleanup_status=1
  fi

  if ! psql_gate <<'SQL'
\getenv large_object_fixture INGESTION_LARGE_OBJECT_FIXTURE
\getenv legacy_lo_compat_fixture INGESTION_LEGACY_LO_COMPAT_FIXTURE
SELECT pg_catalog.lo_unlink(large_object.oid)
FROM pg_catalog.pg_largeobject_metadata AS large_object
WHERE large_object.oid IN (
    SELECT fixture.oid_text::OID
    FROM (VALUES
        (:'large_object_fixture'),
        (:'legacy_lo_compat_fixture')
    ) AS fixture(oid_text)
    WHERE fixture.oid_text ~ '^[0-9]+$'
);
SQL
  then
    echo "::error::Failed to unlink the large-object fixture"
    cleanup_status=1
  fi

  for role_name in \
    "$runtime_role" "$legacy_role" "$legacy_parent_role" "$creator_role" \
    "$inherited_role" "$set_role" "$future_role"; do
    export CLEANUP_ROLE="$role_name"
    role_exists=$(psql_gate -t -A <<'SQL'
\getenv cleanup_role CLEANUP_ROLE
SELECT pg_catalog.count(*) FROM pg_catalog.pg_roles
WHERE rolname = :'cleanup_role';
SQL
    )
    if [ "$role_exists" = "0" ]; then
      continue
    fi
    if [ "$role_exists" != "1" ]; then
      echo "::error::Unexpected role lookup result for $role_name: $role_exists"
      cleanup_status=1
      continue
    fi
    if ! psql_gate <<'SQL'
\getenv cleanup_role CLEANUP_ROLE
DROP OWNED BY :"cleanup_role" CASCADE;
SQL
    then
      echo "::error::DROP OWNED failed for $role_name"
      cleanup_status=1
    fi
    if ! psql_gate <<'SQL'
\getenv cleanup_role CLEANUP_ROLE
DROP ROLE :"cleanup_role";
SQL
    then
      echo "::error::DROP ROLE failed for $role_name"
      cleanup_status=1
    fi
  done
  unset CLEANUP_ROLE

  if [ "$cleanup_status" -ne 0 ]; then
    exit "$cleanup_status"
  fi
  exit "$original_status"
}

create_ephemeral_roles() {
  psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv legacy_role INGESTION_LEGACY_ROLE
\getenv legacy_parent_role INGESTION_LEGACY_PARENT_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
\getenv inherited_role INGESTION_RUNTIME_INHERITED_ROLE
\getenv set_role INGESTION_RUNTIME_SET_ROLE
\getenv runtime_password INGESTION_RUNTIME_PASSWORD
\getenv legacy_password INGESTION_LEGACY_PASSWORD
BEGIN;
CREATE ROLE :"runtime_role"
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT
    NOREPLICATION NOBYPASSRLS PASSWORD :'runtime_password';
CREATE ROLE :"legacy_role"
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT
    NOREPLICATION NOBYPASSRLS PASSWORD :'legacy_password';
CREATE ROLE :"legacy_parent_role"
    NOLOGIN NOSUPERUSER CREATEDB CREATEROLE INHERIT
    NOREPLICATION NOBYPASSRLS;
CREATE ROLE :"creator_role"
    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT
    NOREPLICATION NOBYPASSRLS;
CREATE ROLE :"inherited_role"
    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT
    NOREPLICATION NOBYPASSRLS;
CREATE ROLE :"set_role"
    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
    NOREPLICATION NOBYPASSRLS;
GRANT app_ingestion_runtime TO :"runtime_role";
GRANT :"legacy_parent_role" TO :"legacy_role";
GRANT CONNECT ON DATABASE postgres TO :"legacy_role";
GRANT CREATE, TEMPORARY ON DATABASE postgres TO :"legacy_parent_role";
GRANT USAGE ON SCHEMA public TO :"legacy_role";
GRANT CREATE ON SCHEMA public TO :"legacy_parent_role";
GRANT USAGE ON TYPE public.feed_status TO :"legacy_role";
GRANT SELECT, UPDATE ON TABLE public.feeds TO :"legacy_role";
GRANT SELECT ON TABLE public.feed_properties TO :"legacy_role";
GRANT UPDATE ON TABLE public.feed_properties TO :"legacy_parent_role";
GRANT SELECT, INSERT ON TABLE public.feed_audit_events TO :"legacy_role";
GRANT SET ON PARAMETER zero_damaged_pages TO :"legacy_parent_role";
GRANT EXECUTE ON FUNCTION pg_catalog.lo_import(text)
    TO :"legacy_parent_role";
GRANT CREATE ON SCHEMA public TO :"creator_role";
COMMIT;
SQL

  large_object_fixture=$(psql_gate -t -A \
    -c "SELECT pg_catalog.lo_from_bytea(0, decode('00', 'hex'))")
  if [[ ! "$large_object_fixture" =~ ^[0-9]+$ ]]; then
    echo "::error::Unexpected large-object fixture identity: $large_object_fixture"
    return 1
  fi
  export INGESTION_LARGE_OBJECT_FIXTURE="$large_object_fixture"
  legacy_lo_compat_fixture=$(psql_gate -t -A \
    -c "SELECT pg_catalog.lo_from_bytea(0, decode('01', 'hex'))")
  if [[ ! "$legacy_lo_compat_fixture" =~ ^[0-9]+$ ]]; then
    echo "::error::Unexpected legacy lo_compat fixture identity: $legacy_lo_compat_fixture"
    return 1
  fi
  export INGESTION_LEGACY_LO_COMPAT_FIXTURE="$legacy_lo_compat_fixture"
  psql_gate -v large_object_fixture="$large_object_fixture" <<'SQL'
\getenv legacy_parent_role INGESTION_LEGACY_PARENT_ROLE
GRANT SELECT ON LARGE OBJECT :large_object_fixture TO :"legacy_parent_role";
SQL
}

inject_persistent_privilege_drift() {
  echo "Starting parameter/default-ACL drift recovery fixture..."
  psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
\getenv large_object_fixture INGESTION_LARGE_OBJECT_FIXTURE
GRANT SET, ALTER SYSTEM ON PARAMETER session_replication_role
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
GRANT SET, ALTER SYSTEM ON PARAMETER lo_compat_privileges
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER ROLE ALL SET session_replication_role = replica;
ALTER ROLE ALL SET lo_compat_privileges = on;
ALTER ROLE :"runtime_role" SET session_replication_role = replica;
ALTER ROLE :"runtime_role" IN DATABASE postgres
    SET session_replication_role = replica;
ALTER DATABASE postgres SET session_replication_role = replica;
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role"
    GRANT ALL PRIVILEGES ON TABLES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role"
    GRANT ALL PRIVILEGES ON SEQUENCES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role"
    GRANT ALL PRIVILEGES ON ROUTINES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role"
    GRANT ALL PRIVILEGES ON TYPES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
    GRANT ALL PRIVILEGES ON TABLES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
    GRANT ALL PRIVILEGES ON SEQUENCES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
    GRANT ALL PRIVILEGES ON ROUTINES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
    GRANT ALL PRIVILEGES ON TYPES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    TO PUBLIC;
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    TO app_ingestion_runtime, :"runtime_role" WITH GRANT OPTION;
GRANT SELECT, UPDATE ON LARGE OBJECT :large_object_fixture TO PUBLIC;
GRANT SELECT, UPDATE ON LARGE OBJECT :large_object_fixture
    TO app_ingestion_runtime, :"runtime_role" WITH GRANT OPTION;
GRANT SELECT (slug) ON TABLE public.source_types
    TO :"runtime_role" WITH GRANT OPTION;
SQL
}

seed_legacy_public_capabilities() {
  echo "Starting legacy capability preservation fixture..."
  echo "Starting legacy lo_compat_privileges preservation fixture..."
  psql_gate <<'SQL'
\getenv large_object_fixture INGESTION_LARGE_OBJECT_FIXTURE
ALTER DATABASE postgres RESET lo_compat_privileges;
ALTER DATABASE postgres RESET session_replication_role;
ALTER ROLE ALL SET lo_compat_privileges = on;
ALTER ROLE ALL SET session_replication_role = replica;
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea)
    TO PUBLIC;
GRANT UPDATE ON LARGE OBJECT :large_object_fixture TO PUBLIC;
SQL
}

run_lower_source_parameter_override_fixture() {
  local base_settings future_settings probe_output probe_status

  echo "Starting lower-source parameter override fixture..."
  psql_gate <<'SQL'
ALTER SYSTEM SET session_replication_role = replica;
ALTER SYSTEM SET lo_compat_privileges = on;
SELECT pg_catalog.pg_reload_conf();
SQL
  base_settings=$(psql_gate -d template1 -t -A <<'SQL'
SELECT pg_catalog.current_setting('session_replication_role') || '|' ||
       pg_catalog.current_setting('lo_compat_privileges');
SQL
  )
  if [ "$base_settings" != "replica|on" ]; then
    echo "::error::Unsafe lower-source fixture did not become active"
    return 1
  fi

  psql_gate <<'SQL'
ALTER DATABASE postgres SET session_replication_role = origin;
ALTER DATABASE postgres SET lo_compat_privileges = off;
SQL
  reconcile_with_legacy

  psql_gate -v future_password="$runtime_password" <<'SQL'
\getenv future_role INGESTION_RUNTIME_FUTURE_ROLE
CREATE ROLE :"future_role"
    LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT
    NOREPLICATION NOBYPASSRLS PASSWORD :'future_password';
GRANT app_ingestion_runtime TO :"future_role";
SQL
  future_settings=$(PGPASSWORD="$runtime_password" \
    psql_gate -U "$future_role" -t -A <<'SQL'
SELECT pg_catalog.current_setting('session_replication_role') || '|' ||
       pg_catalog.current_setting('lo_compat_privileges');
SQL
  )
  if [ "$future_settings" != "origin|off" ]; then
    echo "::error::A post-reconcile app member inherited unsafe settings"
    return 1
  fi

  set +e
  probe_output=$(PGPASSWORD="$runtime_password" \
    psql_gate -U "$future_role" \
      -v large_object_fixture="$legacy_lo_compat_fixture" 2>&1 <<'SQL'
BEGIN;
SELECT pg_catalog.lo_get(:large_object_fixture::OID);
SELECT pg_catalog.lo_put(:large_object_fixture::OID, 0, decode('02', 'hex'));
ROLLBACK;
SQL
  )
  probe_status=$?
  set -e
  if [ "$probe_status" -eq 0 ]; then
    echo "::error::A post-reconcile app member retained large-object access"
    echo "$probe_output"
    return 1
  fi

  psql_gate <<'SQL'
\getenv future_role INGESTION_RUNTIME_FUTURE_ROLE
REVOKE app_ingestion_runtime FROM :"future_role";
DROP ROLE :"future_role";
ALTER SYSTEM RESET session_replication_role;
ALTER SYSTEM RESET lo_compat_privileges;
SELECT pg_catalog.pg_reload_conf();
SQL
}

inject_runtime_privilege_drift() {
  echo "Starting direct parameter/default/large-object drift fixture..."
  psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
\getenv large_object_fixture INGESTION_LARGE_OBJECT_FIXTURE
GRANT SET, ALTER SYSTEM ON PARAMETER session_replication_role
    TO app_ingestion_runtime, :"runtime_role";
GRANT SET, ALTER SYSTEM ON PARAMETER lo_compat_privileges
    TO app_ingestion_runtime, :"runtime_role";
ALTER ROLE ALL SET session_replication_role = replica;
ALTER ROLE ALL SET lo_compat_privileges = on;
ALTER ROLE :"runtime_role" SET session_replication_role = replica;
ALTER ROLE :"runtime_role" IN DATABASE postgres
    SET session_replication_role = replica;
ALTER DATABASE postgres SET session_replication_role = replica;
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role"
    GRANT ALL PRIVILEGES ON TABLES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role"
    GRANT ALL PRIVILEGES ON SEQUENCES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role"
    GRANT ALL PRIVILEGES ON ROUTINES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role"
    GRANT ALL PRIVILEGES ON TYPES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
    GRANT ALL PRIVILEGES ON TABLES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
    GRANT ALL PRIVILEGES ON SEQUENCES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
    GRANT ALL PRIVILEGES ON ROUTINES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"creator_role" IN SCHEMA public
    GRANT ALL PRIVILEGES ON TYPES
    TO PUBLIC, app_ingestion_runtime, :"runtime_role";
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    TO app_ingestion_runtime, :"runtime_role" WITH GRANT OPTION;
GRANT SELECT, UPDATE ON LARGE OBJECT :large_object_fixture
    TO app_ingestion_runtime, :"runtime_role" WITH GRANT OPTION;
GRANT SELECT (slug) ON TABLE public.source_types
    TO :"runtime_role" WITH GRANT OPTION;
SQL
}

legacy_capability_snapshot() {
  psql_gate -t -A -v legacy_role="$legacy_role" -f "$legacy_snapshot_sql"
}

assert_legacy_capabilities_unchanged() {
  local phase=$1
  local after
  after=$(legacy_capability_snapshot)
  if [ "$legacy_capabilities_before" != "$after" ]; then
    echo "::error::Legacy capabilities changed $phase"
    return 1
  fi
}

assert_legacy_parent_path_preserved() {
  psql_gate <<'SQL'
\getenv legacy_role INGESTION_LEGACY_ROLE
\getenv legacy_parent_role INGESTION_LEGACY_PARENT_ROLE
\getenv large_object_fixture INGESTION_LARGE_OBJECT_FIXTURE
SELECT pg_catalog.set_config('ci.legacy_role', :'legacy_role', FALSE);
SELECT pg_catalog.set_config(
    'ci.legacy_parent_role', :'legacy_parent_role', FALSE
);
SELECT pg_catalog.set_config(
    'ci.large_object_fixture', :'large_object_fixture', FALSE
);
DO $legacy_parent_path_preserved$
DECLARE
    legacy_oid OID;
    parent_oid OID;
    fixture_oid OID := pg_catalog.current_setting(
        'ci.large_object_fixture'
    )::OID;
BEGIN
    SELECT oid INTO STRICT legacy_oid
    FROM pg_catalog.pg_roles
    WHERE rolname = pg_catalog.current_setting('ci.legacy_role');
    SELECT oid INTO STRICT parent_oid
    FROM pg_catalog.pg_roles
    WHERE rolname = pg_catalog.current_setting('ci.legacy_parent_role');

    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members
        WHERE roleid = parent_oid AND member = legacy_oid
    )
       OR NOT pg_catalog.has_database_privilege(
           parent_oid, pg_catalog.current_database(), 'CREATE'
       )
       OR NOT pg_catalog.has_database_privilege(
           parent_oid, pg_catalog.current_database(), 'TEMPORARY'
       )
       OR NOT pg_catalog.has_schema_privilege(
           parent_oid, 'public', 'CREATE'
       )
       OR NOT pg_catalog.has_table_privilege(
           parent_oid, 'public.feed_properties', 'UPDATE'
       )
       OR NOT pg_catalog.has_parameter_privilege(
           parent_oid, 'zero_damaged_pages', 'SET'
       )
       OR NOT pg_catalog.has_function_privilege(
           parent_oid, 'pg_catalog.lo_import(text)', 'EXECUTE'
       ) THEN
        RAISE EXCEPTION 'legacy parent capability path was not preserved';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_database AS database
        CROSS JOIN LATERAL pg_catalog.aclexplode(database.datacl) AS acl
        WHERE database.datname = pg_catalog.current_database()
          AND acl.grantee = legacy_oid
          AND acl.privilege_type IN ('CREATE', 'TEMPORARY')
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_namespace AS namespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(namespace.nspacl) AS acl
        WHERE namespace.nspname = 'public'
          AND acl.grantee = legacy_oid
          AND acl.privilege_type = 'CREATE'
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(relation.relacl) AS acl
        WHERE namespace.nspname = 'public'
          AND relation.relname = 'feed_properties'
          AND acl.grantee = legacy_oid
          AND acl.privilege_type = 'UPDATE'
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_parameter_acl AS parameter
        CROSS JOIN LATERAL pg_catalog.aclexplode(parameter.paracl) AS acl
        WHERE parameter.parname = 'zero_damaged_pages'
          AND acl.grantee = legacy_oid
          AND acl.privilege_type = 'SET'
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_proc AS procedure
        CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
            procedure.proacl,
            pg_catalog.acldefault('f'::"char", procedure.proowner)
        )) AS acl
        WHERE procedure.oid =
              'pg_catalog.lo_import(text)'::pg_catalog.regprocedure
          AND acl.grantee = legacy_oid
          AND acl.privilege_type = 'EXECUTE'
        UNION ALL
        SELECT 1
        FROM pg_catalog.pg_largeobject_metadata AS large_object
        CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
            large_object.lomacl,
            pg_catalog.acldefault('L'::"char", large_object.lomowner)
        )) AS acl
        WHERE large_object.oid = fixture_oid
          AND acl.grantee = legacy_oid
          AND acl.privilege_type = 'SELECT'
    ) THEN
        RAISE EXCEPTION 'legacy hardening expanded a parent right directly';
    END IF;
END
$legacy_parent_path_preserved$;
SQL
}

assert_creator_fixture_closed() {
  local fixture_role=$1
  local remaining
  remaining=$(psql_gate -t -A \
    -v runtime_role="$runtime_role" \
    -v fixture_role="$fixture_role" <<'SQL'
SELECT pg_catalog.count(*)
FROM pg_catalog.pg_proc AS procedure
WHERE procedure.oid IN (
    'pg_catalog.lo_create(oid)'::pg_catalog.regprocedure,
    'pg_catalog.lo_creat(integer)'::pg_catalog.regprocedure,
    'pg_catalog.lo_from_bytea(oid, bytea)'::pg_catalog.regprocedure,
    'pg_catalog.lo_import(text)'::pg_catalog.regprocedure,
    'pg_catalog.lo_import(text, oid)'::pg_catalog.regprocedure
)
  AND (
      pg_catalog.has_function_privilege(
          :'runtime_role', procedure.oid, 'EXECUTE'
      )
      OR pg_catalog.has_function_privilege(
          :'fixture_role', procedure.oid, 'EXECUTE'
      )
  );
SQL
  )
  if [ "$remaining" != "0" ]; then
    echo "::error::Large-object creator fixture survived reconciliation"
    return 1
  fi
}

assert_privilege_drift_absent() {
  psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv legacy_role INGESTION_LEGACY_ROLE
\getenv legacy_parent_role INGESTION_LEGACY_PARENT_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
\getenv inherited_role INGESTION_RUNTIME_INHERITED_ROLE
\getenv set_role INGESTION_RUNTIME_SET_ROLE
\getenv ownership_fixture INGESTION_OWNERSHIP_FIXTURE
SELECT pg_catalog.set_config('ci.runtime_role', :'runtime_role', FALSE);
SELECT pg_catalog.set_config('ci.legacy_role', :'legacy_role', FALSE);
SELECT pg_catalog.set_config(
    'ci.legacy_parent_role', :'legacy_parent_role', FALSE
);
SELECT pg_catalog.set_config('ci.creator_role', :'creator_role', FALSE);
SELECT pg_catalog.set_config('ci.inherited_role', :'inherited_role', FALSE);
SELECT pg_catalog.set_config('ci.set_role', :'set_role', FALSE);
SELECT pg_catalog.set_config(
    'ci.ownership_fixture', :'ownership_fixture', FALSE
);
DO $assert_privilege_drift_absent$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles
        WHERE rolname IN (
            pg_catalog.current_setting('ci.runtime_role'),
            pg_catalog.current_setting('ci.legacy_role'),
            pg_catalog.current_setting('ci.legacy_parent_role'),
            pg_catalog.current_setting('ci.creator_role'),
            pg_catalog.current_setting('ci.inherited_role'),
            pg_catalog.current_setting('ci.set_role')
        )
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_proc AS procedure
        WHERE procedure.oid IN (
            'pg_catalog.lo_create(oid)'::pg_catalog.regprocedure,
            'pg_catalog.lo_creat(integer)'::pg_catalog.regprocedure,
            'pg_catalog.lo_from_bytea(oid, bytea)'::pg_catalog.regprocedure,
            'pg_catalog.lo_import(text)'::pg_catalog.regprocedure,
            'pg_catalog.lo_import(text, oid)'::pg_catalog.regprocedure
        )
          AND (
              pg_catalog.has_function_privilege(
                  'app_ingestion_runtime', procedure.oid, 'EXECUTE'
              )
              OR EXISTS (
                  SELECT 1
                  FROM pg_catalog.aclexplode(COALESCE(
                      procedure.proacl,
                      pg_catalog.acldefault('f'::"char", procedure.proowner)
                  )) AS acl
                  WHERE acl.grantee = 0::OID
                    AND acl.privilege_type = 'EXECUTE'
              )
          )
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_parameter_acl AS parameter
        CROSS JOIN LATERAL pg_catalog.aclexplode(parameter.paracl) AS acl
        WHERE parameter.parname IN (
            'session_replication_role', 'lo_compat_privileges'
        )
          AND (
              acl.grantee = 0::OID
              OR pg_catalog.has_parameter_privilege(
                  'app_ingestion_runtime', parameter.parname, 'SET'
              )
          )
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_db_role_setting AS role_setting
        CROSS JOIN LATERAL unnest(role_setting.setconfig)
          AS configuration(setting)
        WHERE role_setting.setrole = 0::OID
          AND pg_catalog.split_part(configuration.setting, '=', 1)
              IN ('session_replication_role', 'lo_compat_privileges')
          AND NOT (
                  pg_catalog.split_part(configuration.setting, '=', 1) =
                      'lo_compat_privileges'
              AND substring(
                      configuration.setting
                      FROM pg_catalog.strpos(configuration.setting, '=') + 1
                  )::BOOLEAN IS FALSE
              OR pg_catalog.split_part(configuration.setting, '=', 1) =
                      'session_replication_role'
              AND substring(
                      configuration.setting
                      FROM pg_catalog.strpos(configuration.setting, '=') + 1
                  ) = 'origin'
          )
    ) OR EXISTS (
        WITH expected_setting(parameter_name, configuration_value) AS (
            VALUES
                ('session_replication_role', 'origin'),
                ('lo_compat_privileges', 'off')
        )
        SELECT 1
        FROM expected_setting
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_db_role_setting AS role_setting
            CROSS JOIN LATERAL unnest(role_setting.setconfig)
              AS configuration(setting)
            WHERE role_setting.setrole = 0::OID
              AND role_setting.setdatabase = (
                  SELECT database.oid
                  FROM pg_catalog.pg_database AS database
                  WHERE database.datname = pg_catalog.current_database()
              )
              AND configuration.setting =
                  expected_setting.parameter_name || '=' ||
                  expected_setting.configuration_value
        )
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_collation AS object_collation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = object_collation.collnamespace
        WHERE namespace.nspname = 'public'
          AND object_collation.collname =
              pg_catalog.current_setting('ci.ownership_fixture')
    ) OR pg_catalog.has_schema_privilege(
        'app_ingestion_runtime', 'public', 'CREATE'
    ) THEN
        RAISE EXCEPTION 'early-exit cleanup left persistent drift';
    END IF;
END
$assert_privilege_drift_absent$;
SQL
}

inject_early_exit_blockers() {
  local version
  version=$(server_version_num)
  psql_gate <<'SQL'
\getenv ownership_fixture INGESTION_OWNERSHIP_FIXTURE
BEGIN;
GRANT CREATE ON SCHEMA public TO app_ingestion_runtime;
SET ROLE app_ingestion_runtime;
CREATE COLLATION public.:"ownership_fixture" FROM "C";
RESET ROLE;
COMMIT;
SQL
  if [ "$version" -ge 160000 ]; then
    psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv set_role INGESTION_RUNTIME_SET_ROLE
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    TO :"set_role" WITH GRANT OPTION;
GRANT :"set_role" TO :"runtime_role" WITH INHERIT FALSE, SET TRUE;
SQL
  else
    psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv inherited_role INGESTION_RUNTIME_INHERITED_ROLE
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    TO :"inherited_role" WITH GRANT OPTION;
GRANT :"inherited_role" TO :"runtime_role";
SQL
  fi
}

run_early_exit_cleanup_probe() (
  trap cleanup_ephemeral_roles EXIT
  create_ephemeral_roles
  inject_persistent_privilege_drift
  inject_early_exit_blockers
  exit 86
)

reconcile_with_legacy() {
  psql_gate -v legacy_role="$legacy_role" -f "$reconcile_sql"
}

expect_reconcile_rejection() {
  local fixture_name=$1 expected_message=$2 output status
  set +e
  output=$(reconcile_with_legacy 2>&1)
  status=$?
  set -e
  if [ "$status" -eq 0 ]; then
    echo "::error::Reconciliation accepted $fixture_name"
    return 1
  fi
  if [ "$status" -ne 3 ]; then
    echo "::error::$fixture_name failed outside the SQL contract"
    echo "$output"
    return 1
  fi
  if [[ "$output" != *"$expected_message"* ]]; then
    echo "::error::$fixture_name produced the wrong diagnostic"
    echo "$output"
    return 1
  fi
}

run_member_attribute_rejection_fixture() {
  local direct_acl_after_failure direct_acl_after_reconcile
  echo "Starting fail-closed CREATEDB member fixture..."
  psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
ALTER ROLE :"runtime_role" CREATEDB;
GRANT SELECT ON TABLE public.source_types TO :"runtime_role";
SQL
  expect_reconcile_rejection \
    "a CREATEDB app ingestion member" \
    "app ingestion closure has unsafe role attributes"

  direct_acl_after_failure=$(psql_gate -t -A <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
SELECT pg_catalog.count(*)
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS namespace
  ON namespace.oid = relation.relnamespace
CROSS JOIN LATERAL pg_catalog.aclexplode(relation.relacl) AS acl
JOIN pg_catalog.pg_roles AS role ON role.oid = acl.grantee
WHERE namespace.nspname = 'public'
  AND relation.relname = 'source_types'
  AND role.rolname = :'runtime_role'
  AND acl.privilege_type = 'SELECT';
SQL
  )
  if [ "$direct_acl_after_failure" != "1" ]; then
    echo "::error::A failed reconciliation did not roll back direct member ACL removal"
    return 1
  fi

  psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
ALTER ROLE :"runtime_role" NOCREATEDB;
SQL
  reconcile_with_legacy
  direct_acl_after_reconcile=$(psql_gate -t -A <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
SELECT pg_catalog.count(*)
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS namespace
  ON namespace.oid = relation.relnamespace
CROSS JOIN LATERAL pg_catalog.aclexplode(relation.relacl) AS acl
JOIN pg_catalog.pg_roles AS role ON role.oid = acl.grantee
WHERE namespace.nspname = 'public'
  AND relation.relname = 'source_types'
  AND role.rolname = :'runtime_role'
  AND acl.privilege_type = 'SELECT';
SQL
  )
  if [ "$direct_acl_after_reconcile" != "0" ]; then
    echo "::error::A successful reconciliation left direct member ACL drift"
    return 1
  fi
}

run_inherited_creator_fixture() {
  local version=$1
  echo "Starting inherited large-object creator fixture..."
  if [ "$version" -ge 160000 ]; then
    psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv inherited_role INGESTION_RUNTIME_INHERITED_ROLE
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    TO :"inherited_role" WITH GRANT OPTION;
GRANT :"inherited_role" TO :"runtime_role" WITH INHERIT TRUE, SET FALSE;
SQL
  else
    psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv inherited_role INGESTION_RUNTIME_INHERITED_ROLE
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    TO :"inherited_role" WITH GRANT OPTION;
GRANT :"inherited_role" TO :"runtime_role";
SQL
  fi
  reconcile_with_legacy
  assert_creator_fixture_closed "$inherited_role"
  psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv inherited_role INGESTION_RUNTIME_INHERITED_ROLE
REVOKE :"inherited_role" FROM :"runtime_role";
SQL
  assert_legacy_capabilities_unchanged "while closing inherited drift"
}

run_set_creator_fixture() {
  echo "Starting SET-ROLE-only large-object creator fixture..."
  psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv set_role INGESTION_RUNTIME_SET_ROLE
GRANT EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    TO :"set_role" WITH GRANT OPTION;
GRANT :"set_role" TO :"runtime_role" WITH INHERIT FALSE, SET TRUE;
SQL
  reconcile_with_legacy
  assert_creator_fixture_closed "$set_role"
  psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv set_role INGESTION_RUNTIME_SET_ROLE
REVOKE :"set_role" FROM :"runtime_role";
SQL
  assert_legacy_capabilities_unchanged "while closing SET-only drift"
}

run_default_acl_rejection_fixture() {
  local version=$1 expected_diagnostic
  expected_diagnostic="future public objects expose PUBLIC or an app ingestion member"
  echo "Starting SET-ROLE-only default-ACL fixture..."
  if [ "$version" -ge 160000 ]; then
    psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
GRANT :"creator_role" TO :"runtime_role" WITH INHERIT FALSE, SET TRUE;
SQL
  else
    psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
ALTER ROLE :"runtime_role" NOINHERIT;
GRANT :"creator_role" TO :"runtime_role";
SQL
    expected_diagnostic="app ingestion closure has unsafe role attributes"
  fi
  expect_reconcile_rejection \
    "a SET-ROLE-only default ACL" \
    "$expected_diagnostic"
  psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
\getenv creator_role INGESTION_RUNTIME_CREATOR_ROLE
REVOKE :"creator_role" FROM :"runtime_role";
SQL
  if [ "$version" -lt 160000 ]; then
    psql_gate <<'SQL'
\getenv runtime_role INGESTION_RUNTIME_ROLE
ALTER ROLE :"runtime_role" INHERIT;
SQL
  fi
}

run_ownership_rejection_fixture() {
  echo "Starting fail-closed ownership dependency fixture..."
  psql_gate <<'SQL'
\getenv ownership_fixture INGESTION_OWNERSHIP_FIXTURE
BEGIN;
GRANT CREATE ON SCHEMA public TO app_ingestion_runtime;
SET ROLE app_ingestion_runtime;
CREATE COLLATION public.:"ownership_fixture" FROM "C";
RESET ROLE;
REVOKE CREATE ON SCHEMA public FROM app_ingestion_runtime;
COMMIT;
SQL
  expect_reconcile_rejection \
    "an app-owned collation" \
    "app_ingestion_runtime must not own database objects"
  psql_gate <<'SQL'
\getenv ownership_fixture INGESTION_OWNERSHIP_FIXTURE
DROP COLLATION public.:"ownership_fixture";
SQL
}

dedicated_proof_gate() {
  local version large_objects_before large_objects_after early_exit_status
  local admin_dsn runtime_dsn legacy_dsn
  assert_server_major
  version=$(server_version_num)

  runtime_role="ci_ingestion_runtime_${EXPECTED_POSTGRES_MAJOR}_${GITHUB_RUN_ID:-local}_${GITHUB_RUN_ATTEMPT:-1}"
  legacy_role="ci_ingestion_legacy_${EXPECTED_POSTGRES_MAJOR}_${GITHUB_RUN_ID:-local}_${GITHUB_RUN_ATTEMPT:-1}"
  legacy_parent_role="ci_ingestion_legacy_parent_${EXPECTED_POSTGRES_MAJOR}_${GITHUB_RUN_ID:-local}_${GITHUB_RUN_ATTEMPT:-1}"
  creator_role="ci_ingestion_creator_${EXPECTED_POSTGRES_MAJOR}_${GITHUB_RUN_ID:-local}_${GITHUB_RUN_ATTEMPT:-1}"
  inherited_role="ci_ingestion_inherited_${EXPECTED_POSTGRES_MAJOR}_${GITHUB_RUN_ID:-local}_${GITHUB_RUN_ATTEMPT:-1}"
  set_role="ci_ingestion_set_${EXPECTED_POSTGRES_MAJOR}_${GITHUB_RUN_ID:-local}_${GITHUB_RUN_ATTEMPT:-1}"
  future_role="ci_ingestion_future_${EXPECTED_POSTGRES_MAJOR}_${GITHUB_RUN_ID:-local}_${GITHUB_RUN_ATTEMPT:-1}"
  ownership_fixture="ci_runtime_owned_collation_${EXPECTED_POSTGRES_MAJOR}_${GITHUB_RUN_ID:-local}_${GITHUB_RUN_ATTEMPT:-1}"
  runtime_password=$(openssl rand -hex 32)
  legacy_password=$(openssl rand -hex 32)
  admin_dsn="postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${PGDATABASE}"
  runtime_dsn="postgresql://${runtime_role}:${runtime_password}@${PGHOST}:${PGPORT}/${PGDATABASE}"
  legacy_dsn="postgresql://${legacy_role}:${legacy_password}@${PGHOST}:${PGPORT}/${PGDATABASE}"
  for secret in \
    "$runtime_password" "$legacy_password" \
    "$admin_dsn" "$runtime_dsn" "$legacy_dsn"; do
    echo "::add-mask::$secret"
  done
  export_fixture_environment

  large_objects_before=$(psql_gate -t -A \
    -c "SELECT pg_catalog.count(*) FROM pg_catalog.pg_largeobject_metadata")
  set +e
  run_early_exit_cleanup_probe
  early_exit_status=$?
  set -e
  if [ "$early_exit_status" -ne 86 ]; then
    echo "::error::Early-exit cleanup probe returned $early_exit_status"
    return 1
  fi
  assert_privilege_drift_absent
  large_objects_after=$(psql_gate -t -A \
    -c "SELECT pg_catalog.count(*) FROM pg_catalog.pg_largeobject_metadata")
  if [ "$large_objects_before" != "$large_objects_after" ]; then
    echo "::error::Early-exit cleanup leaked a large object"
    return 1
  fi

  trap cleanup_ephemeral_roles EXIT
  create_ephemeral_roles
  run_lower_source_parameter_override_fixture
  unset INGESTION_RUNTIME_PASSWORD INGESTION_LEGACY_PASSWORD

  seed_legacy_public_capabilities
  legacy_capabilities_before=$(legacy_capability_snapshot)
  reconcile_with_legacy
  assert_legacy_capabilities_unchanged "during initial hardening"
  assert_legacy_parent_path_preserved

  run_member_attribute_rejection_fixture
  assert_legacy_capabilities_unchanged "across member attribute rejection"
  assert_legacy_parent_path_preserved

  inject_runtime_privilege_drift
  reconcile_with_legacy
  assert_legacy_capabilities_unchanged "while closing direct drift"

  run_inherited_creator_fixture "$version"
  if [ "$version" -ge 160000 ]; then
    run_set_creator_fixture
  fi
  run_default_acl_rejection_fixture "$version"
  run_ownership_rejection_fixture

  reconcile_with_legacy
  assert_legacy_capabilities_unchanged "across the full CI proof"
  assert_legacy_parent_path_preserved
  psql_gate -v runtime_role="$runtime_role" -f "$contract_sql"

  INGESTION_RUNTIME_ADMIN_DSN="$admin_dsn" \
  INGESTION_RUNTIME_TEST_DSN="$runtime_dsn" \
  INGESTION_LEGACY_TEST_DSN="$legacy_dsn" \
  INGESTION_RUNTIME_EXTERNAL_POSTGRES_REQUIRED=1 \
    uv run python -m pytest \
      integration_tests/storage/test_ingestion_runtime_privileges.py -q -n 0

  INGESTION_LEASE_TEST_DSN="$admin_dsn" \
  INGESTION_LEASE_EXTERNAL_POSTGRES_REQUIRED=1 \
  EXPECTED_POSTGRES_MAJOR="$EXPECTED_POSTGRES_MAJOR" \
    uv run python -m pytest \
      integration_tests/storage/test_ingestion_lease_store_integration.py \
      -q -n 0
}

case "${1:-}" in
  apply-schema)
    apply_schema_gate
    ;;
  prove)
    dedicated_proof_gate
    ;;
  *)
    echo "usage: $0 {apply-schema|prove}" >&2
    exit 2
    ;;
esac
