-- Shared transaction-local hardening for both privilege entry points.
-- Callers must start a transaction and define the psql variable legacy_role.
\set ON_ERROR_STOP on
\if :{?legacy_role}
\else
    DO $missing_legacy_role$
    BEGIN
        RAISE EXCEPTION
            'legacy_role must be explicitly defined (empty means no legacy preservation)';
    END
    $missing_legacy_role$;
\endif

CREATE TEMP TABLE ingestion_hardening_input (
    legacy_role TEXT NOT NULL
) ON COMMIT DROP;
INSERT INTO pg_temp.ingestion_hardening_input (legacy_role)
VALUES (:'legacy_role');

DO $hardening_preflight$
DECLARE
    configured_legacy_role TEXT;
BEGIN
    SELECT legacy_role
      INTO STRICT configured_legacy_role
      FROM pg_temp.ingestion_hardening_input;
    PERFORM pg_catalog.set_config('search_path', 'pg_catalog, pg_temp', TRUE);

    IF CURRENT_USER <> 'postgres' THEN
        RAISE EXCEPTION
            'ingestion privilege hardening must run as postgres';
    END IF;
    IF configured_legacy_role <> ''
       AND configured_legacy_role !~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
        RAISE EXCEPTION
            'legacy preservation role must be empty or a simple identifier';
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_roles
         WHERE rolname = 'app_ingestion_runtime'
    ) THEN
        RAISE EXCEPTION
            'missing expected ingestion privilege role app_ingestion_runtime';
    END IF;
END
$hardening_preflight$;

CREATE TEMP TABLE ingestion_runtime_roles (
    oid OID PRIMARY KEY,
    rolname NAME NOT NULL UNIQUE
) ON COMMIT DROP;

WITH RECURSIVE runtime_roles AS (
    SELECT role.oid, role.rolname
      FROM pg_catalog.pg_roles AS role
     WHERE role.rolname = 'app_ingestion_runtime'
    UNION
    SELECT member.oid, member.rolname
      FROM runtime_roles AS parent
      JOIN pg_catalog.pg_auth_members AS membership
        ON membership.roleid = parent.oid
      JOIN pg_catalog.pg_roles AS member
        ON member.oid = membership.member
)
INSERT INTO pg_temp.ingestion_runtime_roles (oid, rolname)
SELECT oid, rolname FROM runtime_roles;

CREATE TEMP TABLE ingestion_runtime_reachable_roles (
    oid OID PRIMARY KEY,
    rolname NAME NOT NULL UNIQUE,
    rolsuper BOOLEAN NOT NULL
) ON COMMIT DROP;

DO $runtime_reachable_roles$
DECLARE
    membership_mode TEXT := CASE
        WHEN pg_catalog.current_setting('server_version_num')::INTEGER >= 160000
            THEN 'SET'
        ELSE 'MEMBER'
    END;
BEGIN
    INSERT INTO pg_temp.ingestion_runtime_reachable_roles (
        oid,
        rolname,
        rolsuper
    )
    SELECT DISTINCT candidate.oid, candidate.rolname, candidate.rolsuper
      FROM pg_temp.ingestion_runtime_roles AS runtime_role
      CROSS JOIN pg_catalog.pg_roles AS candidate
     WHERE candidate.oid = runtime_role.oid
        OR pg_catalog.pg_has_role(runtime_role.oid, candidate.oid, 'USAGE')
        OR pg_catalog.pg_has_role(
               runtime_role.oid,
               candidate.oid,
               membership_mode
           );
END
$runtime_reachable_roles$;

CREATE TEMP TABLE ingestion_large_object_creators (
    oid OID PRIMARY KEY,
    signature TEXT NOT NULL UNIQUE
) ON COMMIT DROP;

INSERT INTO pg_temp.ingestion_large_object_creators (oid, signature)
VALUES
    (
        'pg_catalog.lo_create(oid)'::pg_catalog.regprocedure,
        'pg_catalog.lo_create(oid)'
    ),
    (
        'pg_catalog.lo_creat(integer)'::pg_catalog.regprocedure,
        'pg_catalog.lo_creat(integer)'
    ),
    (
        'pg_catalog.lo_from_bytea(oid, bytea)'::pg_catalog.regprocedure,
        'pg_catalog.lo_from_bytea(oid, bytea)'
    ),
    (
        'pg_catalog.lo_import(text)'::pg_catalog.regprocedure,
        'pg_catalog.lo_import(text)'
    ),
    (
        'pg_catalog.lo_import(text, oid)'::pg_catalog.regprocedure,
        'pg_catalog.lo_import(text, oid)'
    );

CREATE TEMP TABLE ingestion_legacy_identity (
    oid OID PRIMARY KEY,
    rolname NAME NOT NULL UNIQUE
) ON COMMIT DROP;

DO $legacy_identity_validation$
DECLARE
    configured_legacy_role TEXT;
    legacy_state RECORD;
BEGIN
    SELECT legacy_role
      INTO STRICT configured_legacy_role
      FROM pg_temp.ingestion_hardening_input;
    IF configured_legacy_role = '' THEN
        RETURN;
    END IF;

    SELECT
        role.oid,
        role.rolname,
        role.rolcanlogin,
        role.rolsuper,
        role.rolcreatedb,
        role.rolcreaterole,
        role.rolreplication,
        role.rolbypassrls
      INTO legacy_state
      FROM pg_catalog.pg_roles AS role
     WHERE role.rolname = configured_legacy_role;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'legacy preservation role does not exist';
    END IF;

    IF NOT legacy_state.rolcanlogin
       OR legacy_state.oid = CURRENT_USER::pg_catalog.regrole
       OR legacy_state.rolsuper
       OR legacy_state.rolcreatedb
       OR legacy_state.rolcreaterole
       OR legacy_state.rolreplication
       OR legacy_state.rolbypassrls
       OR EXISTS (
           SELECT 1
             FROM pg_temp.ingestion_runtime_roles AS runtime_role
            WHERE runtime_role.oid = legacy_state.oid
       )
       OR EXISTS (
           SELECT 1
             FROM pg_catalog.pg_auth_members AS membership
            WHERE membership.roleid = legacy_state.oid
       ) THEN
        RAISE EXCEPTION
            'legacy preservation role must be a distinct non-admin login outside the app ingestion closure';
    END IF;

    INSERT INTO pg_temp.ingestion_legacy_identity (oid, rolname)
    VALUES (legacy_state.oid, legacy_state.rolname);
END
$legacy_identity_validation$;

CREATE TEMP TABLE ingestion_parameter_names (
    parname TEXT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO pg_temp.ingestion_parameter_names (parname)
SELECT parameter_acl.parname
  FROM pg_catalog.pg_parameter_acl AS parameter_acl
UNION
SELECT 'session_replication_role'::TEXT
UNION
SELECT 'lo_compat_privileges'::TEXT;

CREATE TEMP TABLE ingestion_legacy_effective_roles (
    oid OID PRIMARY KEY,
    rolname NAME NOT NULL UNIQUE,
    rolsuper BOOLEAN NOT NULL
) ON COMMIT DROP;

DO $legacy_effective_roles$
DECLARE
    membership_mode TEXT := CASE
        WHEN pg_catalog.current_setting('server_version_num')::INTEGER >= 160000
            THEN 'SET'
        ELSE 'MEMBER'
    END;
BEGIN
    INSERT INTO pg_temp.ingestion_legacy_effective_roles (
        oid,
        rolname,
        rolsuper
    )
    SELECT role.oid, role.rolname, role.rolsuper
     FROM pg_temp.ingestion_legacy_identity AS legacy
      CROSS JOIN pg_catalog.pg_roles AS role
     WHERE role.oid = legacy.oid
        OR pg_catalog.pg_has_role(legacy.oid, role.oid, membership_mode);
END
$legacy_effective_roles$;

-- Resolve both security-sensitive startup values without mistaking an
-- admin-only role, client, or session override for a shared server default.
-- PostgreSQL applies defaults in this exact order: role-in-database, role,
-- database, all roles, then the shared server base.
CREATE TEMP TABLE ingestion_legacy_startup_parameter (
    parameter_name TEXT PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO pg_temp.ingestion_legacy_startup_parameter (parameter_name)
VALUES ('lo_compat_privileges'), ('session_replication_role');

CREATE TEMP TABLE ingestion_shared_startup_base (
    parameter_name TEXT PRIMARY KEY,
    configuration_value TEXT NOT NULL
) ON COMMIT DROP;

INSERT INTO pg_temp.ingestion_shared_startup_base (
    parameter_name,
    configuration_value
)
SELECT name, setting
  FROM pg_catalog.pg_settings
 WHERE name IN (
           SELECT parameter_name
             FROM pg_temp.ingestion_legacy_startup_parameter
       )
   AND source IN (
       'default',
       'configuration file',
       'environment variable',
       'command line',
       'override'
   );

CREATE TEMP VIEW ingestion_legacy_startup_setting_current AS
WITH database AS (
    SELECT oid
      FROM pg_catalog.pg_database
     WHERE datname = pg_catalog.current_database()
), catalog_candidate AS (
    SELECT
        pg_catalog.split_part(configuration.setting, '=', 1)
            AS parameter_name,
        substring(
            configuration.setting
            FROM pg_catalog.strpos(configuration.setting, '=') + 1
        ) AS configuration_value,
        CASE
            WHEN role_setting.setrole = legacy.oid
             AND role_setting.setdatabase = database.oid THEN 4
            WHEN role_setting.setrole = legacy.oid
             AND role_setting.setdatabase = 0::OID THEN 3
            WHEN role_setting.setrole = 0::OID
             AND role_setting.setdatabase = database.oid THEN 2
            ELSE 1
        END AS priority,
        CASE
            WHEN role_setting.setrole = legacy.oid
             AND role_setting.setdatabase = database.oid
                THEN 'role_database'
            WHEN role_setting.setrole = legacy.oid
                THEN 'role'
            WHEN role_setting.setdatabase = database.oid
                THEN 'database'
            ELSE 'global'
        END AS source_kind
      FROM pg_temp.ingestion_legacy_identity AS legacy
      CROSS JOIN database
      JOIN pg_catalog.pg_db_role_setting AS role_setting
        ON role_setting.setrole IN (0::OID, legacy.oid)
       AND role_setting.setdatabase IN (0::OID, database.oid)
      CROSS JOIN LATERAL
        unnest(role_setting.setconfig) AS configuration(setting)
     WHERE pg_catalog.split_part(configuration.setting, '=', 1) IN (
               SELECT parameter_name
                 FROM pg_temp.ingestion_legacy_startup_parameter
           )
), ranked_candidate AS (
    SELECT
        parameter_name,
        configuration_value,
        source_kind,
        pg_catalog.row_number() OVER (
            PARTITION BY parameter_name ORDER BY priority DESC
        ) AS precedence
      FROM catalog_candidate
), selected AS (
    SELECT parameter_name, configuration_value, source_kind
      FROM ranked_candidate
     WHERE precedence = 1
)
SELECT parameter_name, configuration_value, source_kind FROM selected
UNION ALL
SELECT
    server_base.parameter_name,
    server_base.configuration_value,
    'base'::TEXT
  FROM pg_temp.ingestion_legacy_identity AS legacy
  CROSS JOIN pg_temp.ingestion_shared_startup_base AS server_base
 WHERE NOT EXISTS (
           SELECT 1
             FROM selected
            WHERE selected.parameter_name = server_base.parameter_name
       );

DO $legacy_startup_setting_preflight$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_temp.ingestion_legacy_identity)
       AND NOT EXISTS (
           SELECT 1
             FROM pg_temp.ingestion_legacy_startup_setting_current
            WHERE parameter_name = 'lo_compat_privileges'
       ) THEN
        RAISE EXCEPTION
            'legacy lo_compat_privileges default is ambiguous';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_temp.ingestion_legacy_identity)
       AND NOT EXISTS (
           SELECT 1
             FROM pg_temp.ingestion_legacy_startup_setting_current
            WHERE parameter_name = 'session_replication_role'
       ) THEN
        RAISE EXCEPTION
            'legacy session_replication_role default is ambiguous';
    END IF;
END
$legacy_startup_setting_preflight$;

CREATE TEMP TABLE ingestion_legacy_startup_setting_snapshot
ON COMMIT DROP
AS SELECT * FROM pg_temp.ingestion_legacy_startup_setting_current;

CREATE TEMP VIEW ingestion_legacy_current_capabilities AS
WITH table_privileges AS (
    SELECT privilege
      FROM unnest(
          CASE
              WHEN pg_catalog.current_setting('server_version_num')::INTEGER
                       >= 170000
                  THEN ARRAY[
                      'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
                      'REFERENCES', 'TRIGGER', 'MAINTAIN'
                  ]::TEXT[]
              ELSE ARRAY[
                  'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
                  'REFERENCES', 'TRIGGER'
              ]::TEXT[]
          END
      ) AS privilege
),
column_privileges AS (
    SELECT privilege
      FROM unnest(
          ARRAY['SELECT', 'INSERT', 'UPDATE', 'REFERENCES']::TEXT[]
      ) AS privilege
),
large_object_acl AS (
    SELECT
        legacy.oid AS legacy_oid,
        large_object.oid AS large_object_oid,
        large_object.lomowner,
        acl.grantee,
        acl.privilege_type,
        acl.is_grantable
      FROM pg_temp.ingestion_legacy_identity AS legacy
      CROSS JOIN pg_catalog.pg_largeobject_metadata AS large_object
      CROSS JOIN LATERAL pg_catalog.aclexplode(
          COALESCE(
              large_object.lomacl,
              pg_catalog.acldefault('L'::"char", large_object.lomowner)
          )
      ) AS acl
)
SELECT
    'database'::TEXT AS surface,
    database.oid AS object_oid,
    NULL::INTEGER AS subid,
    NULL::TEXT AS schema_name,
    database.datname::TEXT AS object_name,
    privilege.name::TEXT AS privilege,
    CASE
        WHEN privilege.name = 'CONNECT' THEN
            pg_catalog.has_database_privilege(
                legacy.oid,
                database.oid,
                privilege.name
            )
        ELSE EXISTS (
            SELECT 1
              FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
             WHERE pg_catalog.has_database_privilege(
                       effective_role.oid,
                       database.oid,
                       privilege.name
                   )
        )
    END AS allowed,
    CASE
        WHEN privilege.name = 'CONNECT' THEN
            pg_catalog.has_database_privilege(
                legacy.oid,
                database.oid,
                privilege.name || ' WITH GRANT OPTION'
            )
        ELSE EXISTS (
            SELECT 1
              FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
             WHERE pg_catalog.has_database_privilege(
                       effective_role.oid,
                       database.oid,
                       privilege.name || ' WITH GRANT OPTION'
                   )
        )
    END AS grantable
  FROM pg_temp.ingestion_legacy_identity AS legacy
  JOIN pg_catalog.pg_database AS database
    ON database.datname = pg_catalog.current_database()
  CROSS JOIN (VALUES ('CONNECT'), ('CREATE'), ('TEMPORARY')) AS privilege(name)
UNION ALL
SELECT
    'schema',
    namespace.oid,
    NULL,
    NULL,
    namespace.nspname,
    privilege.name,
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_schema_privilege(
                   effective_role.oid,
                   namespace.oid,
                   privilege.name
               )
    ),
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_schema_privilege(
                   effective_role.oid,
                   namespace.oid,
                   privilege.name || ' WITH GRANT OPTION'
               )
    )
  FROM pg_temp.ingestion_legacy_identity AS legacy
  JOIN pg_catalog.pg_namespace AS namespace ON namespace.nspname = 'public'
  CROSS JOIN (VALUES ('USAGE'), ('CREATE')) AS privilege(name)
UNION ALL
SELECT
    'relation',
    relation.oid,
    NULL,
    namespace.nspname,
    relation.relname,
    privilege.privilege,
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_table_privilege(
                   effective_role.oid,
                   relation.oid,
                   privilege.privilege
               )
    ),
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_table_privilege(
                   effective_role.oid,
                   relation.oid,
                   privilege.privilege || ' WITH GRANT OPTION'
               )
    )
  FROM pg_temp.ingestion_legacy_identity AS legacy
  CROSS JOIN pg_catalog.pg_class AS relation
  JOIN pg_catalog.pg_namespace AS namespace
    ON namespace.oid = relation.relnamespace
  CROSS JOIN table_privileges AS privilege
 WHERE namespace.nspname = 'public'
   AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
UNION ALL
SELECT
    'column',
    relation.oid,
    attribute.attnum,
    namespace.nspname,
    relation.relname || '.' || attribute.attname,
    privilege.privilege,
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_column_privilege(
                   effective_role.oid,
                   relation.oid,
                   attribute.attnum,
                   privilege.privilege
               )
    ),
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_column_privilege(
                   effective_role.oid,
                   relation.oid,
                   attribute.attnum,
                   privilege.privilege || ' WITH GRANT OPTION'
               )
    )
  FROM pg_temp.ingestion_legacy_identity AS legacy
  CROSS JOIN pg_catalog.pg_attribute AS attribute
  JOIN pg_catalog.pg_class AS relation ON relation.oid = attribute.attrelid
  JOIN pg_catalog.pg_namespace AS namespace
    ON namespace.oid = relation.relnamespace
  CROSS JOIN column_privileges AS privilege
 WHERE namespace.nspname = 'public'
   AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
   AND attribute.attnum > 0
   AND NOT attribute.attisdropped
UNION ALL
SELECT
    'sequence',
    relation.oid,
    NULL,
    namespace.nspname,
    relation.relname,
    privilege.name,
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_sequence_privilege(
                   effective_role.oid,
                   relation.oid,
                   privilege.name
               )
    ),
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_sequence_privilege(
                   effective_role.oid,
                   relation.oid,
                   privilege.name || ' WITH GRANT OPTION'
               )
    )
  FROM pg_temp.ingestion_legacy_identity AS legacy
  CROSS JOIN pg_catalog.pg_class AS relation
  JOIN pg_catalog.pg_namespace AS namespace
    ON namespace.oid = relation.relnamespace
  CROSS JOIN (VALUES ('USAGE'), ('SELECT'), ('UPDATE')) AS privilege(name)
 WHERE namespace.nspname = 'public'
   AND relation.relkind = 'S'
UNION ALL
SELECT
    'routine',
    procedure.oid,
    NULL,
    namespace.nspname,
    procedure.oid::pg_catalog.regprocedure::TEXT,
    'EXECUTE',
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_function_privilege(
                   effective_role.oid,
                   procedure.oid,
                   'EXECUTE'
               )
    ),
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_function_privilege(
                   effective_role.oid,
                   procedure.oid,
                   'EXECUTE WITH GRANT OPTION'
               )
    )
  FROM pg_temp.ingestion_legacy_identity AS legacy
  CROSS JOIN pg_catalog.pg_proc AS procedure
  JOIN pg_catalog.pg_namespace AS namespace
    ON namespace.oid = procedure.pronamespace
 WHERE namespace.nspname = 'public'
UNION ALL
SELECT
    'type',
    type.oid,
    NULL,
    namespace.nspname,
    type.oid::pg_catalog.regtype::TEXT,
    'USAGE',
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_type_privilege(
                   effective_role.oid,
                   type.oid,
                   'USAGE'
               )
    ),
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_type_privilege(
                   effective_role.oid,
                   type.oid,
                   'USAGE WITH GRANT OPTION'
               )
    )
  FROM pg_temp.ingestion_legacy_identity AS legacy
  CROSS JOIN pg_catalog.pg_type AS type
  JOIN pg_catalog.pg_namespace AS namespace
    ON namespace.oid = type.typnamespace
  LEFT JOIN pg_catalog.pg_class AS type_relation
    ON type_relation.oid = type.typrelid
 WHERE namespace.nspname = 'public'
   AND type.typelem = 0
   AND (
       (type.typrelid = 0 AND type.typtype IN ('b', 'd', 'e', 'm', 'r'))
       OR (type.typtype = 'c' AND type_relation.relkind = 'c')
   )
UNION ALL
SELECT
    'parameter',
    NULL,
    NULL,
    NULL,
    parameter.parname,
    privilege.name,
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_parameter_privilege(
                   effective_role.oid,
                   parameter.parname,
                   privilege.name
               )
    ),
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_parameter_privilege(
                   effective_role.oid,
                   parameter.parname,
                   privilege.name || ' WITH GRANT OPTION'
               )
    )
  FROM pg_temp.ingestion_legacy_identity AS legacy
  CROSS JOIN pg_temp.ingestion_parameter_names AS parameter
  CROSS JOIN (VALUES ('SET'), ('ALTER SYSTEM')) AS privilege(name)
UNION ALL
SELECT
    'startup_setting',
    NULL,
    NULL,
    NULL,
    setting.parameter_name,
    setting.configuration_value,
    TRUE,
    FALSE
  FROM pg_temp.ingestion_legacy_identity AS legacy
  CROSS JOIN pg_temp.ingestion_legacy_startup_setting_current AS setting
UNION ALL
SELECT
    'large_object_creator',
    creator.oid,
    NULL,
    'pg_catalog',
    creator.signature,
    'EXECUTE',
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_function_privilege(
                   effective_role.oid,
                   creator.oid,
                   'EXECUTE'
               )
    ),
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE pg_catalog.has_function_privilege(
                   effective_role.oid,
                   creator.oid,
                   'EXECUTE WITH GRANT OPTION'
               )
    )
  FROM pg_temp.ingestion_legacy_identity AS legacy
  CROSS JOIN pg_temp.ingestion_large_object_creators AS creator
UNION ALL
SELECT
    'large_object',
    large_object.oid,
    NULL,
    'pg_catalog',
    large_object.oid::TEXT,
    privilege.name,
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE (
                   privilege.name IN ('SELECT', 'UPDATE')
               AND EXISTS (
                       SELECT 1
                         FROM pg_temp.ingestion_legacy_startup_setting_current
                        WHERE parameter_name = 'lo_compat_privileges'
                          AND configuration_value::BOOLEAN
                   )
               )
            OR effective_role.rolsuper
            OR large_object.lomowner = effective_role.oid
            OR pg_catalog.pg_has_role(
                   effective_role.oid,
                   large_object.lomowner,
                   'USAGE'
               )
            OR EXISTS (
                   SELECT 1
                     FROM large_object_acl AS acl
                    WHERE acl.legacy_oid = legacy.oid
                      AND acl.large_object_oid = large_object.oid
                      AND acl.privilege_type = privilege.name
                      AND (
                          acl.grantee = 0::OID
                          OR pg_catalog.pg_has_role(
                                 effective_role.oid,
                                 acl.grantee,
                                 'USAGE'
                             )
                      )
               )
    ),
    EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_legacy_effective_roles AS effective_role
         WHERE effective_role.rolsuper
            OR large_object.lomowner = effective_role.oid
            OR pg_catalog.pg_has_role(
                   effective_role.oid,
                   large_object.lomowner,
                   'USAGE'
               )
            OR EXISTS (
                   SELECT 1
                     FROM large_object_acl AS acl
                    WHERE acl.legacy_oid = legacy.oid
                      AND acl.large_object_oid = large_object.oid
                      AND acl.privilege_type = privilege.name
                      AND acl.is_grantable
                      AND (
                          acl.grantee = 0::OID
                          OR pg_catalog.pg_has_role(
                                 effective_role.oid,
                                 acl.grantee,
                                 'USAGE'
                             )
                      )
               )
    )
  FROM pg_temp.ingestion_legacy_identity AS legacy
  CROSS JOIN pg_catalog.pg_largeobject_metadata AS large_object
  CROSS JOIN (VALUES ('SELECT'), ('UPDATE')) AS privilege(name);

CREATE TEMP TABLE ingestion_legacy_capability_snapshot
ON COMMIT DROP
AS SELECT * FROM pg_temp.ingestion_legacy_current_capabilities WITH NO DATA;

INSERT INTO pg_temp.ingestion_legacy_capability_snapshot
SELECT * FROM pg_temp.ingestion_legacy_current_capabilities;

-- A broad or base setting is about to be replaced with safe database defaults.
-- Keep the legacy login's effective values exact with more-specific defaults;
-- lo_compat preservation also covers large objects created in the future.
DO $legacy_lo_compat_and_replication_preserve$
DECLARE
    legacy_setting RECORD;
BEGIN
    FOR legacy_setting IN
        SELECT
            identity.rolname,
            setting.parameter_name,
            setting.configuration_value
          FROM pg_temp.ingestion_legacy_identity AS identity
          CROSS JOIN pg_temp.ingestion_legacy_startup_setting_snapshot
               AS setting
         WHERE setting.source_kind IN ('database', 'global', 'base')
           AND (
                   setting.parameter_name = 'lo_compat_privileges'
               AND setting.configuration_value::BOOLEAN
               OR setting.parameter_name = 'session_replication_role'
               AND setting.configuration_value <> 'origin'
           )
         ORDER BY setting.parameter_name
    LOOP
        EXECUTE pg_catalog.format(
            'ALTER ROLE %I IN DATABASE %I SET %I = %L',
            legacy_setting.rolname,
            pg_catalog.current_database(),
            legacy_setting.parameter_name,
            legacy_setting.configuration_value
        );
    END LOOP;
END
$legacy_lo_compat_and_replication_preserve$;

-- Normalize attributes an AlloyDB administrator may change. Dangerous
-- SUPERUSER/REPLICATION/BYPASSRLS attributes are verified by the fail-closed
-- postcondition below; redundantly setting them is forbidden on AlloyDB,
-- whose built-in postgres login is not a PostgreSQL SUPERUSER.
ALTER ROLE app_ingestion_runtime
    NOLOGIN
    INHERIT
    CONNECTION LIMIT -1;
ALTER ROLE app_ingestion_runtime PASSWORD NULL;
ALTER ROLE app_ingestion_runtime RESET ALL;

DO $runtime_parent_memberships$
DECLARE
    membership_state RECORD;
BEGIN
    FOR membership_state IN
        SELECT parent.rolname AS parent_name, member.rolname AS member_name
          FROM pg_catalog.pg_auth_members AS membership
          JOIN pg_catalog.pg_roles AS parent
            ON parent.oid = membership.roleid
          JOIN pg_catalog.pg_roles AS member
            ON member.oid = membership.member
         WHERE member.oid IN (
                   SELECT oid FROM pg_temp.ingestion_runtime_roles
               )
           AND parent.oid NOT IN (
                   SELECT oid FROM pg_temp.ingestion_runtime_roles
               )
         ORDER BY member.oid, parent.oid
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE %I FROM %I',
            membership_state.parent_name,
            membership_state.member_name
        );
    END LOOP;
END
$runtime_parent_memberships$;

-- Parameter ACLs and unsafe login defaults are cluster-wide capabilities.
DO $parameter_acl_revoke$
DECLARE
    parameter_state RECORD;
    runtime_grantee RECORD;
BEGIN
    FOR parameter_state IN
        SELECT parname
          FROM pg_temp.ingestion_parameter_names
         ORDER BY parname
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE SET, ALTER SYSTEM ON PARAMETER %I FROM PUBLIC CASCADE',
            parameter_state.parname
        );
        FOR runtime_grantee IN
            SELECT DISTINCT grantee.rolname
              FROM pg_catalog.pg_parameter_acl AS parameter_acl
              CROSS JOIN LATERAL
                pg_catalog.aclexplode(parameter_acl.paracl) AS acl
              JOIN pg_catalog.pg_roles AS grantee ON grantee.oid = acl.grantee
             WHERE parameter_acl.parname = parameter_state.parname
               AND EXISTS (
                   SELECT 1
                     FROM pg_temp.ingestion_runtime_reachable_roles
                          AS reachable_role
                    WHERE reachable_role.oid = grantee.oid
               )
             ORDER BY grantee.rolname
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE SET, ALTER SYSTEM ON PARAMETER %I FROM %I CASCADE',
                parameter_state.parname,
                runtime_grantee.rolname
            );
        END LOOP;
    END LOOP;
END
$parameter_acl_revoke$;

DO $unsafe_role_settings$
DECLARE
    unsafe_setting RECORD;
BEGIN
    FOR unsafe_setting IN
        SELECT configured_setting.*
          FROM (
              SELECT
                  runtime_role.rolname,
                  role_setting.setdatabase,
                  pg_catalog.split_part(configuration.setting, '=', 1)
                      AS parameter_name,
                  substring(
                      configuration.setting
                      FROM pg_catalog.strpos(configuration.setting, '=') + 1
                  ) AS configuration_value,
                  FALSE AS database_wide
                FROM pg_temp.ingestion_runtime_roles AS runtime_role
                JOIN pg_catalog.pg_db_role_setting AS role_setting
                  ON role_setting.setrole = runtime_role.oid
                CROSS JOIN LATERAL
                  unnest(role_setting.setconfig) AS configuration(setting)
                JOIN pg_catalog.pg_settings AS parameter
                  ON parameter.name =
                     pg_catalog.split_part(configuration.setting, '=', 1)
               WHERE role_setting.setdatabase IN (
                         0::OID,
                         (
                             SELECT database.oid
                               FROM pg_catalog.pg_database AS database
                              WHERE database.datname =
                                    pg_catalog.current_database()
                         )
                     )
                 AND parameter.context IN (
                         'superuser', 'superuser-backend'
                     )
              UNION ALL
              SELECT
                  NULL::NAME,
                  role_setting.setdatabase,
                  pg_catalog.split_part(configuration.setting, '=', 1),
                  substring(
                      configuration.setting
                      FROM pg_catalog.strpos(configuration.setting, '=') + 1
                  ),
                  TRUE
                FROM pg_catalog.pg_db_role_setting AS role_setting
                CROSS JOIN LATERAL
                  unnest(role_setting.setconfig) AS configuration(setting)
                JOIN pg_catalog.pg_settings AS parameter
                  ON parameter.name =
                     pg_catalog.split_part(configuration.setting, '=', 1)
               WHERE role_setting.setrole = 0::OID
                 AND role_setting.setdatabase IN (
                         0::OID,
                         (
                             SELECT database.oid
                               FROM pg_catalog.pg_database AS database
                              WHERE database.datname =
                                    pg_catalog.current_database()
                         )
                     )
                 AND parameter.context IN (
                         'superuser', 'superuser-backend'
                     )
          ) AS configured_setting
         WHERE NOT (
                   configured_setting.parameter_name =
                       'lo_compat_privileges'
               AND configured_setting.configuration_value::BOOLEAN IS FALSE
               OR configured_setting.parameter_name =
                       'session_replication_role'
               AND configured_setting.configuration_value = 'origin'
         )
         ORDER BY setdatabase, rolname NULLS FIRST, parameter_name
    LOOP
        IF unsafe_setting.database_wide
           AND unsafe_setting.setdatabase = 0::OID THEN
            EXECUTE pg_catalog.format(
                'ALTER ROLE ALL RESET %I',
                unsafe_setting.parameter_name
            );
        ELSIF unsafe_setting.database_wide THEN
            EXECUTE pg_catalog.format(
                'ALTER DATABASE %I RESET %I',
                pg_catalog.current_database(),
                unsafe_setting.parameter_name
            );
        ELSIF unsafe_setting.setdatabase = 0::OID THEN
            EXECUTE pg_catalog.format(
                'ALTER ROLE %I RESET %I',
                unsafe_setting.rolname,
                unsafe_setting.parameter_name
            );
        ELSE
            EXECUTE pg_catalog.format(
                'ALTER ROLE %I IN DATABASE %I RESET %I',
                unsafe_setting.rolname,
                pg_catalog.current_database(),
                unsafe_setting.parameter_name
            );
        END IF;
    END LOOP;
END
$unsafe_role_settings$;

-- The database defaults protect clean login roles added to the group after this
-- run.  The role-in-database defaults additionally override an unsafe setting
-- already attached to any materialized runtime identity.
DO $runtime_safe_role_settings$
DECLARE
    runtime_role RECORD;
BEGIN
    EXECUTE pg_catalog.format(
        'ALTER DATABASE %I SET session_replication_role = origin',
        pg_catalog.current_database()
    );
    EXECUTE pg_catalog.format(
        'ALTER DATABASE %I SET lo_compat_privileges = off',
        pg_catalog.current_database()
    );

    FOR runtime_role IN
        SELECT rolname
          FROM pg_temp.ingestion_runtime_roles
         ORDER BY oid
    LOOP
        EXECUTE pg_catalog.format(
            'ALTER ROLE %I IN DATABASE %I '
            'SET session_replication_role = origin',
            runtime_role.rolname,
            pg_catalog.current_database()
        );
        EXECUTE pg_catalog.format(
            'ALTER ROLE %I IN DATABASE %I '
            'SET lo_compat_privileges = off',
            runtime_role.rolname,
            pg_catalog.current_database()
        );
    END LOOP;
END
$runtime_safe_role_settings$;

-- Close only the exact five server-side large-object creator functions.
REVOKE EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea),
    pg_catalog.lo_import(text),
    pg_catalog.lo_import(text, oid)
    FROM PUBLIC CASCADE;

DO $large_object_creator_acl_revoke$
DECLARE
    creator RECORD;
    grantee RECORD;
BEGIN
    FOR creator IN
        SELECT oid, signature
          FROM pg_temp.ingestion_large_object_creators
         ORDER BY oid
    LOOP
        FOR grantee IN
            SELECT DISTINCT role.rolname
              FROM pg_catalog.pg_proc AS procedure
              CROSS JOIN LATERAL pg_catalog.aclexplode(
                  COALESCE(
                      procedure.proacl,
                      pg_catalog.acldefault('f'::"char", procedure.proowner)
                  )
              ) AS acl
              JOIN pg_catalog.pg_roles AS role ON role.oid = acl.grantee
             WHERE procedure.oid = creator.oid
               AND acl.grantee <> procedure.proowner
               AND EXISTS (
                   SELECT 1
                     FROM pg_temp.ingestion_runtime_reachable_roles
                          AS reachable_role
                    WHERE reachable_role.oid = role.oid
               )
             ORDER BY role.rolname
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE EXECUTE ON FUNCTION %s FROM %I CASCADE',
                creator.signature,
                grantee.rolname
            );
        END LOOP;
    END LOOP;
END
$large_object_creator_acl_revoke$;

DO $large_object_acl_revoke$
DECLARE
    large_object RECORD;
    grantee RECORD;
BEGIN
    FOR large_object IN
        SELECT oid, lomowner
          FROM pg_catalog.pg_largeobject_metadata
         ORDER BY oid
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE SELECT, UPDATE ON LARGE OBJECT %s FROM PUBLIC CASCADE',
            large_object.oid
        );
        FOR grantee IN
            SELECT DISTINCT role.rolname
              FROM pg_catalog.pg_largeobject_metadata AS object
              CROSS JOIN LATERAL pg_catalog.aclexplode(
                  COALESCE(
                      object.lomacl,
                      pg_catalog.acldefault('L'::"char", object.lomowner)
                  )
              ) AS acl
              JOIN pg_catalog.pg_roles AS role ON role.oid = acl.grantee
             WHERE object.oid = large_object.oid
               AND acl.grantee <> object.lomowner
               AND EXISTS (
                   SELECT 1
                     FROM pg_temp.ingestion_runtime_reachable_roles
                          AS reachable_role
                    WHERE reachable_role.oid = role.oid
               )
             ORDER BY role.rolname
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE SELECT, UPDATE ON LARGE OBJECT %s FROM %I CASCADE',
                large_object.oid,
                grantee.rolname
            );
        END LOOP;
    END LOOP;
END
$large_object_acl_revoke$;

-- Remove PUBLIC and runtime capabilities on existing application objects.
DO $database_acl_revoke$
DECLARE
    runtime_grantee RECORD;
BEGIN
    EXECUTE pg_catalog.format(
        'REVOKE CREATE, TEMPORARY ON DATABASE %I FROM PUBLIC',
        pg_catalog.current_database()
    );
    FOR runtime_grantee IN
        SELECT rolname
          FROM pg_temp.ingestion_runtime_roles
         ORDER BY oid
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON DATABASE %I FROM %I',
            pg_catalog.current_database(),
            runtime_grantee.rolname
        );
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON SCHEMA public FROM %I',
            runtime_grantee.rolname
        );
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM %I',
            runtime_grantee.rolname
        );
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM %I',
            runtime_grantee.rolname
        );
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA public FROM %I',
            runtime_grantee.rolname
        );
    END LOOP;
END
$database_acl_revoke$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA public FROM PUBLIC;

DO $column_acl_revoke$
DECLARE
    application_column RECORD;
    runtime_grantee RECORD;
BEGIN
    FOR application_column IN
        SELECT
            namespace.nspname,
            relation.relname,
            attribute.attname
          FROM pg_catalog.pg_attribute AS attribute
          JOIN pg_catalog.pg_class AS relation
            ON relation.oid = attribute.attrelid
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = 'public'
           AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped
         ORDER BY relation.oid, attribute.attnum
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES (%I) ON TABLE %I.%I FROM PUBLIC',
            application_column.attname,
            application_column.nspname,
            application_column.relname
        );
        FOR runtime_grantee IN
            SELECT rolname
              FROM pg_temp.ingestion_runtime_roles
             ORDER BY oid
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE ALL PRIVILEGES (%I) ON TABLE %I.%I FROM %I',
                application_column.attname,
                application_column.nspname,
                application_column.relname,
                runtime_grantee.rolname
            );
        END LOOP;
    END LOOP;
END
$column_acl_revoke$;

DO $type_acl_revoke$
DECLARE
    application_type RECORD;
    runtime_grantee RECORD;
BEGIN
    FOR application_type IN
        SELECT namespace.nspname, type.typname
          FROM pg_catalog.pg_type AS type
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = type.typnamespace
          LEFT JOIN pg_catalog.pg_class AS type_relation
            ON type_relation.oid = type.typrelid
         WHERE namespace.nspname = 'public'
           AND type.typelem = 0
           AND (
               (type.typrelid = 0 AND type.typtype IN ('b', 'd', 'e', 'm', 'r'))
               OR (type.typtype = 'c' AND type_relation.relkind = 'c')
           )
    LOOP
        FOR runtime_grantee IN
            SELECT rolname
              FROM pg_temp.ingestion_runtime_roles
             ORDER BY oid
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE ALL PRIVILEGES ON TYPE %I.%I FROM %I',
                application_type.nspname,
                application_type.typname,
                runtime_grantee.rolname
            );
        END LOOP;
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON TYPE %I.%I FROM PUBLIC',
            application_type.nspname,
            application_type.typname
        );
    END LOOP;
END
$type_acl_revoke$;

-- Normalize global and public-scoped future ACLs for every public creator.
DO $creator_default_acl_revoke$
DECLARE
    creator_role RECORD;
    runtime_grantee RECORD;
    object_class RECORD;
BEGIN
    FOR creator_role IN
        SELECT role.oid, role.rolname
          FROM pg_catalog.pg_roles AS role
         WHERE pg_catalog.has_schema_privilege(role.oid, 'public', 'CREATE')
        UNION
        SELECT legacy.oid, legacy.rolname
          FROM pg_temp.ingestion_legacy_identity AS legacy
         WHERE EXISTS (
                   SELECT 1
                     FROM pg_temp.ingestion_legacy_capability_snapshot
                          AS capability
                    WHERE capability.surface = 'schema'
                      AND capability.object_name = 'public'
                      AND capability.privilege = 'CREATE'
                      AND capability.allowed
               )
         ORDER BY oid
    LOOP
        FOR object_class IN
            SELECT name
              FROM (VALUES
                  ('TABLES'),
                  ('SEQUENCES'),
                  ('ROUTINES'),
                  ('TYPES')
              ) AS classes(name)
        LOOP
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
                'REVOKE ALL PRIVILEGES ON %s FROM PUBLIC',
                creator_role.rolname,
                object_class.name
            );
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
                'REVOKE ALL PRIVILEGES ON %s FROM PUBLIC',
                creator_role.rolname,
                object_class.name
            );
            FOR runtime_grantee IN
                SELECT rolname
                  FROM pg_temp.ingestion_runtime_roles
                 ORDER BY oid
            LOOP
                EXECUTE pg_catalog.format(
                    'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
                    'REVOKE ALL PRIVILEGES ON %s FROM %I',
                    creator_role.rolname,
                    object_class.name,
                    runtime_grantee.rolname
                );
                EXECUTE pg_catalog.format(
                    'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
                    'REVOKE ALL PRIVILEGES ON %s FROM %I',
                    creator_role.rolname,
                    object_class.name,
                    runtime_grantee.rolname
                );
            END LOOP;
        END LOOP;
    END LOOP;
END
$creator_default_acl_revoke$;

-- Restore only capabilities that the approved legacy login had at entry.
DO $legacy_capability_restore$
DECLARE
    legacy_role NAME;
    capability RECORD;
    relation_name TEXT;
    attribute_name NAME;
    grant_option TEXT;
BEGIN
    SELECT rolname
      INTO legacy_role
      FROM pg_temp.ingestion_legacy_identity;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    FOR capability IN
        SELECT snapshot.*
          FROM pg_temp.ingestion_legacy_capability_snapshot AS snapshot
          JOIN pg_temp.ingestion_legacy_current_capabilities
               AS current_capability
            ON current_capability.surface = snapshot.surface
           AND current_capability.object_oid IS NOT DISTINCT FROM
               snapshot.object_oid
           AND current_capability.subid IS NOT DISTINCT FROM snapshot.subid
           AND current_capability.schema_name IS NOT DISTINCT FROM
               snapshot.schema_name
           AND current_capability.object_name IS NOT DISTINCT FROM
               snapshot.object_name
           AND current_capability.privilege = snapshot.privilege
         WHERE (snapshot.allowed AND NOT current_capability.allowed)
            OR (snapshot.grantable AND NOT current_capability.grantable)
         ORDER BY
               snapshot.surface,
               snapshot.object_oid,
               snapshot.subid,
               snapshot.privilege
    LOOP
        grant_option := CASE
            WHEN capability.grantable THEN ' WITH GRANT OPTION'
            ELSE ''
        END;
        CASE capability.surface
            WHEN 'database' THEN
                EXECUTE pg_catalog.format(
                    'GRANT %s ON DATABASE %I TO %I%s',
                    capability.privilege,
                    capability.object_name,
                    legacy_role,
                    grant_option
                );
            WHEN 'schema' THEN
                EXECUTE pg_catalog.format(
                    'GRANT %s ON SCHEMA %I TO %I%s',
                    capability.privilege,
                    capability.object_name,
                    legacy_role,
                    grant_option
                );
            WHEN 'relation' THEN
                SELECT relation.oid::pg_catalog.regclass::TEXT
                  INTO STRICT relation_name
                  FROM pg_catalog.pg_class AS relation
                 WHERE relation.oid = capability.object_oid;
                EXECUTE pg_catalog.format(
                    'GRANT %s ON TABLE %s TO %I%s',
                    capability.privilege,
                    relation_name,
                    legacy_role,
                    grant_option
                );
            WHEN 'column' THEN
                SELECT
                    relation.oid::pg_catalog.regclass::TEXT,
                    attribute.attname
                  INTO STRICT relation_name, attribute_name
                  FROM pg_catalog.pg_class AS relation
                  JOIN pg_catalog.pg_attribute AS attribute
                    ON attribute.attrelid = relation.oid
                   AND attribute.attnum = capability.subid
                 WHERE relation.oid = capability.object_oid;
                EXECUTE pg_catalog.format(
                    'GRANT %s (%I) ON TABLE %s TO %I%s',
                    capability.privilege,
                    attribute_name,
                    relation_name,
                    legacy_role,
                    grant_option
                );
            WHEN 'sequence' THEN
                SELECT relation.oid::pg_catalog.regclass::TEXT
                  INTO STRICT relation_name
                  FROM pg_catalog.pg_class AS relation
                 WHERE relation.oid = capability.object_oid;
                EXECUTE pg_catalog.format(
                    'GRANT %s ON SEQUENCE %s TO %I%s',
                    capability.privilege,
                    relation_name,
                    legacy_role,
                    grant_option
                );
            WHEN 'routine' THEN
                EXECUTE pg_catalog.format(
                    'GRANT EXECUTE ON ROUTINE %s TO %I%s',
                    capability.object_oid::pg_catalog.regprocedure,
                    legacy_role,
                    grant_option
                );
            WHEN 'type' THEN
                EXECUTE pg_catalog.format(
                    'GRANT USAGE ON TYPE %s TO %I%s',
                    capability.object_oid::pg_catalog.regtype,
                    legacy_role,
                    grant_option
                );
            WHEN 'parameter' THEN
                EXECUTE pg_catalog.format(
                    'GRANT %s ON PARAMETER %I TO %I%s',
                    capability.privilege,
                    capability.object_name,
                    legacy_role,
                    grant_option
                );
            WHEN 'large_object_creator' THEN
                EXECUTE pg_catalog.format(
                    'GRANT EXECUTE ON FUNCTION %s TO %I%s',
                    capability.object_name,
                    legacy_role,
                    grant_option
                );
            WHEN 'large_object' THEN
                EXECUTE pg_catalog.format(
                    'GRANT %s ON LARGE OBJECT %s TO %I%s',
                    capability.privilege,
                    capability.object_oid,
                    legacy_role,
                    grant_option
                );
            WHEN 'startup_setting' THEN
                RAISE EXCEPTION
                    'legacy startup setting changed during hardening';
            ELSE
                RAISE EXCEPTION USING
                    MESSAGE = 'unknown legacy capability surface',
                    DETAIL = capability.surface;
        END CASE;
    END LOOP;
END
$legacy_capability_restore$;

DO $legacy_capability_equality$
BEGIN
    IF EXISTS (
        (
            SELECT *
              FROM pg_temp.ingestion_legacy_capability_snapshot
            EXCEPT
            SELECT *
              FROM pg_temp.ingestion_legacy_current_capabilities
        )
        UNION ALL
        (
            SELECT *
              FROM pg_temp.ingestion_legacy_current_capabilities
            EXCEPT
            SELECT *
              FROM pg_temp.ingestion_legacy_capability_snapshot
        )
    ) THEN
        RAISE EXCEPTION
            'legacy capability changed during ingestion hardening';
    END IF;
END
$legacy_capability_equality$;

-- Fail closed on every effective runtime path after the mutation phase.
DO $runtime_hardening_postcondition$
DECLARE
    role_state RECORD;
    set_role_membership_mode TEXT := CASE
        WHEN pg_catalog.current_setting('server_version_num')::INTEGER >= 160000
            THEN 'SET'
        ELSE 'MEMBER'
    END;
BEGIN
    SELECT
        rolcanlogin,
        rolsuper,
        rolinherit,
        rolcreatedb,
        rolcreaterole,
        rolreplication,
        rolbypassrls,
        rolconnlimit,
        rolpassword
      INTO role_state
      FROM pg_catalog.pg_authid
     WHERE rolname = 'app_ingestion_runtime';
    IF NOT FOUND
       OR role_state.rolcanlogin
       OR role_state.rolsuper
       OR NOT role_state.rolinherit
       OR role_state.rolcreatedb
       OR role_state.rolcreaterole
       OR role_state.rolreplication
       OR role_state.rolbypassrls
       OR role_state.rolconnlimit <> -1
       OR role_state.rolpassword IS NOT NULL THEN
        RAISE EXCEPTION
            'app_ingestion_runtime has unsafe role attributes';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_auth_members AS membership
         WHERE membership.member IN (
                   SELECT oid FROM pg_temp.ingestion_runtime_roles
               )
           AND membership.roleid NOT IN (
                   SELECT oid FROM pg_temp.ingestion_runtime_roles
               )
    ) THEN
        RAISE EXCEPTION
            'app ingestion closure retains an external parent role';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_shdepend AS dependency
          JOIN pg_temp.ingestion_runtime_roles AS owner
            ON owner.oid = dependency.refobjid
         WHERE dependency.refclassid =
                   'pg_catalog.pg_authid'::pg_catalog.regclass
           AND dependency.deptype = 'o'
    ) THEN
        RAISE EXCEPTION
            'app_ingestion_runtime must not own database objects';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_runtime_reachable_roles AS reachable_role
          CROSS JOIN pg_temp.ingestion_parameter_names AS parameter
          CROSS JOIN (VALUES ('SET'), ('ALTER SYSTEM')) AS access(privilege)
         WHERE pg_catalog.has_parameter_privilege(
                   reachable_role.oid,
                   parameter.parname,
                   access.privilege
               )
    ) THEN
        RAISE EXCEPTION
            'app ingestion member retains a privileged parameter ACL';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_db_role_setting AS role_setting
          CROSS JOIN LATERAL
            unnest(role_setting.setconfig) AS configuration(setting)
          JOIN pg_catalog.pg_settings AS parameter
            ON parameter.name =
               pg_catalog.split_part(configuration.setting, '=', 1)
         WHERE parameter.context IN ('superuser', 'superuser-backend')
           AND (
               (
                   role_setting.setrole IN (
                       SELECT oid FROM pg_temp.ingestion_runtime_roles
                   )
                   AND role_setting.setdatabase IN (
                       0::OID,
                       (
                           SELECT database.oid
                             FROM pg_catalog.pg_database AS database
                            WHERE database.datname = pg_catalog.current_database()
                       )
                   )
               )
               OR (
                   role_setting.setrole = 0::OID
                   AND role_setting.setdatabase IN (
                       0::OID,
                       (
                           SELECT database.oid
                             FROM pg_catalog.pg_database AS database
                            WHERE database.datname = pg_catalog.current_database()
                       )
                   )
               )
           )
           AND NOT (
                   parameter.name = 'lo_compat_privileges'
               AND substring(
                       configuration.setting
                       FROM pg_catalog.strpos(configuration.setting, '=') + 1
                   )::BOOLEAN IS FALSE
               OR parameter.name = 'session_replication_role'
               AND substring(
                       configuration.setting
                       FROM pg_catalog.strpos(configuration.setting, '=') + 1
                   ) = 'origin'
           )
    ) THEN
        RAISE EXCEPTION
            'app ingestion member has an unsafe role parameter default';
    END IF;

    IF EXISTS (
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
                               WHERE database.datname =
                                     pg_catalog.current_database()
                          )
                      AND configuration.setting =
                          expected_setting.parameter_name || '=' ||
                          expected_setting.configuration_value
               )
    ) THEN
        RAISE EXCEPTION
            'safe runtime database parameter defaults are missing';
    END IF;

    IF EXISTS (
        WITH expected_setting(parameter_name, configuration_value) AS (
            VALUES
                ('session_replication_role', 'origin'),
                ('lo_compat_privileges', 'off')
        )
        SELECT 1
          FROM pg_temp.ingestion_runtime_roles AS runtime_role
          CROSS JOIN expected_setting
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM pg_catalog.pg_db_role_setting AS role_setting
                     CROSS JOIN LATERAL unnest(role_setting.setconfig)
                          AS configuration(setting)
                    WHERE role_setting.setrole = runtime_role.oid
                      AND role_setting.setdatabase = (
                              SELECT database.oid
                                FROM pg_catalog.pg_database AS database
                               WHERE database.datname =
                                     pg_catalog.current_database()
                          )
                      AND configuration.setting =
                          expected_setting.parameter_name || '=' ||
                          expected_setting.configuration_value
               )
    ) THEN
        RAISE EXCEPTION
            'safe runtime role parameter defaults are missing';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_temp.ingestion_large_object_creators AS creator
          CROSS JOIN pg_temp.ingestion_runtime_reachable_roles
               AS runtime_role
         WHERE pg_catalog.has_function_privilege(
                   runtime_role.oid,
                   creator.oid,
                   'EXECUTE'
               )
            OR pg_catalog.has_function_privilege(
                   runtime_role.oid,
                   creator.oid,
                   'EXECUTE WITH GRANT OPTION'
               )
        UNION ALL
        SELECT 1
          FROM pg_temp.ingestion_large_object_creators AS creator
          JOIN pg_catalog.pg_proc AS procedure ON procedure.oid = creator.oid
          CROSS JOIN LATERAL pg_catalog.aclexplode(
              COALESCE(
                  procedure.proacl,
                  pg_catalog.acldefault('f'::"char", procedure.proowner)
              )
          ) AS acl
         WHERE acl.privilege_type = 'EXECUTE'
           AND (
               acl.grantee = 0::OID
               OR EXISTS (
                   SELECT 1
                     FROM pg_temp.ingestion_runtime_reachable_roles
                          AS runtime_role
                    WHERE acl.grantee = runtime_role.oid
               )
           )
    ) THEN
        RAISE EXCEPTION
            'app ingestion member can execute a large-object creator';
    END IF;

    IF EXISTS (
        SELECT 1
         FROM pg_catalog.pg_largeobject_metadata AS large_object
         WHERE EXISTS (
                   SELECT 1
                     FROM pg_temp.ingestion_runtime_reachable_roles
                          AS runtime_role
                    WHERE runtime_role.rolsuper
                       OR large_object.lomowner = runtime_role.oid
                       OR pg_catalog.pg_has_role(
                           runtime_role.oid,
                           large_object.lomowner,
                           'USAGE'
                       )
                       OR pg_catalog.pg_has_role(
                           runtime_role.oid,
                           large_object.lomowner,
                           set_role_membership_mode
                       )
               )
            OR EXISTS (
                   SELECT 1
                     FROM pg_catalog.aclexplode(
                         COALESCE(
                             large_object.lomacl,
                             pg_catalog.acldefault(
                                 'L'::"char",
                                 large_object.lomowner
                             )
                         )
                     ) AS acl
                    WHERE (
                              acl.privilege_type IN ('SELECT', 'UPDATE')
                              OR acl.is_grantable
                          )
                      AND (
                          acl.grantee = 0::OID
                          OR EXISTS (
                              SELECT 1
                                FROM pg_temp.ingestion_runtime_reachable_roles
                                     AS runtime_role
                               WHERE acl.grantee = runtime_role.oid
                          )
                      )
               )
    ) THEN
        RAISE EXCEPTION
            'app ingestion member has a large-object capability';
    END IF;

    IF EXISTS (
        WITH object_classes AS (
            SELECT *
              FROM (VALUES
                  ('r'::"char", 'TABLES'::TEXT),
                  ('S'::"char", 'SEQUENCES'::TEXT),
                  ('f'::"char", 'ROUTINES'::TEXT),
                  ('T'::"char", 'TYPES'::TEXT)
              ) AS classes(code, name)
        ),
        public_creators AS (
            SELECT role.oid
              FROM pg_catalog.pg_roles AS role
             WHERE pg_catalog.has_schema_privilege(role.oid, 'public', 'CREATE')
        ),
        defaults AS (
            SELECT
                creator.oid AS creator_oid,
                class.code,
                COALESCE(
                    global_defaults.defaclacl,
                    pg_catalog.acldefault(class.code, creator.oid)
                ) || COALESCE(
                    schema_defaults.defaclacl,
                    '{}'::pg_catalog.aclitem[]
                ) AS effective_acl
              FROM public_creators AS creator
              CROSS JOIN object_classes AS class
              LEFT JOIN pg_catalog.pg_default_acl AS global_defaults
                ON global_defaults.defaclrole = creator.oid
               AND global_defaults.defaclnamespace = 0::OID
               AND global_defaults.defaclobjtype = class.code
              LEFT JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.nspname = 'public'
              LEFT JOIN pg_catalog.pg_default_acl AS schema_defaults
                ON schema_defaults.defaclrole = creator.oid
               AND schema_defaults.defaclnamespace = namespace.oid
               AND schema_defaults.defaclobjtype = class.code
        )
        SELECT 1
          FROM defaults
          CROSS JOIN LATERAL
            pg_catalog.aclexplode(defaults.effective_acl) AS acl
         WHERE acl.grantee = 0::OID
            OR EXISTS (
                SELECT 1
                  FROM pg_temp.ingestion_runtime_reachable_roles
                       AS runtime_role
                 WHERE acl.grantee = runtime_role.oid
            )
    ) THEN
        RAISE EXCEPTION
            'future public objects expose PUBLIC or an app ingestion member';
    END IF;
END
$runtime_hardening_postcondition$;
