-- Independent, canonical oracle for the configured legacy login's effective
-- capabilities across every surface touched by ingestion hardening.
\set ON_ERROR_STOP on
\if :{?legacy_role}
\else
    DO $missing_legacy_snapshot_role$
    BEGIN
        RAISE EXCEPTION 'legacy snapshot role must be defined';
    END
    $missing_legacy_snapshot_role$;
\endif

\set QUIET on
BEGIN;
SELECT pg_catalog.set_config(
    'ci.legacy_role', :'legacy_role', FALSE
) AS configured_legacy_role \gset
DO $legacy_snapshot_preflight$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = pg_catalog.current_setting('ci.legacy_role')
    ) THEN
        RAISE EXCEPTION 'legacy snapshot role does not exist';
    END IF;
END
$legacy_snapshot_preflight$;

CREATE TEMP TABLE ci_legacy_effective_roles (
    oid OID PRIMARY KEY,
    rolsuper BOOLEAN NOT NULL
) ON COMMIT DROP;

WITH legacy AS (
    SELECT oid
    FROM pg_catalog.pg_roles
    WHERE rolname = pg_catalog.current_setting('ci.legacy_role')
), membership_mode AS (
    SELECT CASE
        WHEN current_setting('server_version_num')::INTEGER >= 160000
            THEN 'SET'
        ELSE 'MEMBER'
    END AS name
)
INSERT INTO pg_temp.ci_legacy_effective_roles (oid, rolsuper)
SELECT candidate.oid, candidate.rolsuper
FROM legacy
CROSS JOIN membership_mode
CROSS JOIN pg_catalog.pg_roles AS candidate
WHERE candidate.oid = legacy.oid
   OR pg_catalog.pg_has_role(legacy.oid, candidate.oid, membership_mode.name);

CREATE TEMP TABLE ci_legacy_startup_setting (
    parameter_name TEXT PRIMARY KEY,
    configuration_value TEXT NOT NULL
) ON COMMIT DROP;

WITH legacy AS (
    SELECT oid
    FROM pg_catalog.pg_roles
    WHERE rolname = pg_catalog.current_setting('ci.legacy_role')
), database AS (
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
        END AS priority
    FROM legacy
    CROSS JOIN database
    JOIN pg_catalog.pg_db_role_setting AS role_setting
      ON role_setting.setrole IN (0::OID, legacy.oid)
     AND role_setting.setdatabase IN (0::OID, database.oid)
    CROSS JOIN LATERAL
      unnest(role_setting.setconfig) AS configuration(setting)
    WHERE pg_catalog.split_part(configuration.setting, '=', 1) IN (
        'lo_compat_privileges', 'session_replication_role'
    )
), ranked_candidate AS (
    SELECT
        parameter_name,
        configuration_value,
        pg_catalog.row_number() OVER (
            PARTITION BY parameter_name ORDER BY priority DESC
        ) AS precedence
    FROM catalog_candidate
), selected AS (
    SELECT parameter_name, configuration_value
    FROM ranked_candidate
    WHERE precedence = 1
), shared_base AS (
    SELECT name AS parameter_name, setting AS configuration_value
    FROM pg_catalog.pg_settings
    WHERE name IN ('lo_compat_privileges', 'session_replication_role')
      AND source IN (
          'default',
          'configuration file',
          'environment variable',
          'command line',
          'override'
      )
)
INSERT INTO pg_temp.ci_legacy_startup_setting (
    parameter_name,
    configuration_value
)
SELECT parameter_name, configuration_value FROM selected
UNION ALL
SELECT base.parameter_name, base.configuration_value
FROM shared_base AS base
WHERE NOT EXISTS (
    SELECT 1
    FROM selected
    WHERE selected.parameter_name = base.parameter_name
);

DO $legacy_startup_setting_preflight$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_temp.ci_legacy_startup_setting
        WHERE parameter_name = 'lo_compat_privileges'
    ) THEN
        RAISE EXCEPTION
            'legacy lo_compat_privileges default is ambiguous';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_temp.ci_legacy_startup_setting
        WHERE parameter_name = 'session_replication_role'
    ) THEN
        RAISE EXCEPTION
            'legacy session_replication_role default is ambiguous';
    END IF;
END
$legacy_startup_setting_preflight$;

WITH legacy AS (
    SELECT oid
    FROM pg_catalog.pg_roles
    WHERE rolname = pg_catalog.current_setting('ci.legacy_role')
),
table_privileges AS (
    SELECT privilege
    FROM unnest(
        CASE
            WHEN current_setting('server_version_num')::INTEGER >= 170000
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
parameter_names AS (
    SELECT setting.name
    FROM pg_catalog.pg_settings AS setting
),
capabilities AS (
    SELECT
        'database'::TEXT AS surface,
        database.oid AS object_oid,
        0 AS subid,
        privilege.name::TEXT AS privilege,
        CASE
            WHEN privilege.name = 'CONNECT' THEN
                has_database_privilege(
                    legacy.oid,
                    database.oid,
                    privilege.name
                )
            ELSE EXISTS (
                SELECT 1
                FROM pg_temp.ci_legacy_effective_roles AS subject
                WHERE has_database_privilege(
                    subject.oid,
                    database.oid,
                    privilege.name
                )
            )
        END AS allowed,
        CASE
            WHEN privilege.name = 'CONNECT' THEN
                has_database_privilege(
                    legacy.oid,
                    database.oid,
                    privilege.name || ' WITH GRANT OPTION'
                )
            ELSE EXISTS (
                SELECT 1
                FROM pg_temp.ci_legacy_effective_roles AS subject
                WHERE has_database_privilege(
                    subject.oid,
                    database.oid,
                    privilege.name || ' WITH GRANT OPTION'
                )
            )
        END AS grantable
    FROM legacy
    JOIN pg_catalog.pg_database AS database
      ON database.datname = current_database()
    CROSS JOIN (VALUES
        ('CONNECT'), ('CREATE'), ('TEMPORARY')
    ) AS privilege(name)

    UNION ALL

    SELECT
        'schema',
        namespace.oid,
        0,
        privilege.name,
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_schema_privilege(
                subject.oid,
                namespace.oid,
                privilege.name
            )
        ),
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_schema_privilege(
                subject.oid,
                namespace.oid,
                privilege.name || ' WITH GRANT OPTION'
            )
        )
    FROM legacy
    JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.nspname = 'public'
    CROSS JOIN (VALUES ('USAGE'), ('CREATE')) AS privilege(name)

    UNION ALL

    SELECT
        'relation',
        relation.oid,
        0,
        privilege.privilege,
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_table_privilege(
                subject.oid,
                relation.oid,
                privilege.privilege
            )
        ),
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_table_privilege(
                subject.oid,
                relation.oid,
                privilege.privilege || ' WITH GRANT OPTION'
            )
        )
    FROM legacy
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
        privilege.name,
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_column_privilege(
                subject.oid,
                relation.oid,
                attribute.attnum,
                privilege.name
            )
        ),
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_column_privilege(
                subject.oid,
                relation.oid,
                attribute.attnum,
                privilege.name || ' WITH GRANT OPTION'
            )
        )
    FROM legacy
    CROSS JOIN pg_catalog.pg_attribute AS attribute
    JOIN pg_catalog.pg_class AS relation
      ON relation.oid = attribute.attrelid
    JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.oid = relation.relnamespace
    CROSS JOIN (VALUES
        ('SELECT'), ('INSERT'), ('UPDATE'), ('REFERENCES')
    ) AS privilege(name)
    WHERE namespace.nspname = 'public'
      AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
      AND attribute.attnum > 0
      AND NOT attribute.attisdropped

    UNION ALL

    SELECT
        'sequence',
        relation.oid,
        0,
        privilege.name,
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_sequence_privilege(
                subject.oid,
                relation.oid,
                privilege.name
            )
        ),
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_sequence_privilege(
                subject.oid,
                relation.oid,
                privilege.name || ' WITH GRANT OPTION'
            )
        )
    FROM legacy
    CROSS JOIN pg_catalog.pg_class AS relation
    JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.oid = relation.relnamespace
    CROSS JOIN (VALUES
        ('USAGE'), ('SELECT'), ('UPDATE')
    ) AS privilege(name)
    WHERE namespace.nspname = 'public'
      AND relation.relkind = 'S'

    UNION ALL

    SELECT
        'routine',
        procedure.oid,
        0,
        'EXECUTE',
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_function_privilege(
                subject.oid,
                procedure.oid,
                'EXECUTE'
            )
        ),
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_function_privilege(
                subject.oid,
                procedure.oid,
                'EXECUTE WITH GRANT OPTION'
            )
        )
    FROM legacy
    CROSS JOIN pg_catalog.pg_proc AS procedure
    JOIN pg_catalog.pg_namespace AS namespace
      ON namespace.oid = procedure.pronamespace
    WHERE namespace.nspname = 'public'

    UNION ALL

    SELECT
        'type',
        type.oid,
        0,
        'USAGE',
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_type_privilege(subject.oid, type.oid, 'USAGE')
        ),
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_type_privilege(
                subject.oid,
                type.oid,
                'USAGE WITH GRANT OPTION'
            )
        )
    FROM legacy
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
        0,
        0,
        parameter.name || ':' || privilege.name,
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_parameter_privilege(
                subject.oid,
                parameter.name,
                privilege.name
            )
        ),
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_parameter_privilege(
                subject.oid,
                parameter.name,
                privilege.name || ' WITH GRANT OPTION'
            )
        )
    FROM legacy
    CROSS JOIN parameter_names AS parameter
    CROSS JOIN (VALUES ('SET'), ('ALTER SYSTEM')) AS privilege(name)

    UNION ALL

    SELECT
        'startup_setting',
        0,
        0,
        setting.parameter_name || '=' || setting.configuration_value,
        TRUE,
        FALSE
    FROM legacy
    CROSS JOIN pg_temp.ci_legacy_startup_setting AS setting

    UNION ALL

    SELECT
        'large_object_creator',
        procedure.oid,
        0,
        'EXECUTE',
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_function_privilege(
                subject.oid,
                procedure.oid,
                'EXECUTE'
            )
        ),
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE has_function_privilege(
                subject.oid,
                procedure.oid,
                'EXECUTE WITH GRANT OPTION'
            )
        )
    FROM legacy
    CROSS JOIN pg_catalog.pg_proc AS procedure
    WHERE procedure.oid IN (
        'pg_catalog.lo_create(oid)'::pg_catalog.regprocedure,
        'pg_catalog.lo_creat(integer)'::pg_catalog.regprocedure,
        'pg_catalog.lo_from_bytea(oid, bytea)'::pg_catalog.regprocedure,
        'pg_catalog.lo_import(text)'::pg_catalog.regprocedure,
        'pg_catalog.lo_import(text, oid)'::pg_catalog.regprocedure
    )

    UNION ALL

    SELECT
        'large_object',
        large_object.oid,
        0,
        privilege.name,
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE (
                      privilege.name IN ('SELECT', 'UPDATE')
                  AND (
                          SELECT configuration_value::BOOLEAN
                          FROM pg_temp.ci_legacy_startup_setting
                          WHERE parameter_name = 'lo_compat_privileges'
                      )
                  )
               OR subject.rolsuper
               OR large_object.lomowner = subject.oid
               OR pg_catalog.pg_has_role(
                   subject.oid,
                   large_object.lomowner,
                   'USAGE'
               )
               OR EXISTS (
                   SELECT 1
                   FROM pg_catalog.aclexplode(COALESCE(
                       large_object.lomacl,
                       pg_catalog.acldefault(
                           'L'::"char", large_object.lomowner
                       )
                   )) AS acl
                   WHERE acl.privilege_type = privilege.name
                     AND (
                         acl.grantee = 0::OID
                         OR pg_catalog.pg_has_role(
                             subject.oid,
                             acl.grantee,
                             'USAGE'
                         )
                     )
               )
        ),
        EXISTS (
            SELECT 1
            FROM pg_temp.ci_legacy_effective_roles AS subject
            WHERE subject.rolsuper
               OR large_object.lomowner = subject.oid
               OR pg_catalog.pg_has_role(
                   subject.oid,
                   large_object.lomowner,
                   'USAGE'
               )
               OR EXISTS (
                   SELECT 1
                   FROM pg_catalog.aclexplode(COALESCE(
                       large_object.lomacl,
                       pg_catalog.acldefault(
                           'L'::"char", large_object.lomowner
                       )
                   )) AS acl
                   WHERE acl.privilege_type = privilege.name
                     AND acl.is_grantable
                     AND (
                         acl.grantee = 0::OID
                         OR pg_catalog.pg_has_role(
                             subject.oid,
                             acl.grantee,
                             'USAGE'
                         )
                     )
               )
        )
    FROM legacy
    CROSS JOIN pg_catalog.pg_largeobject_metadata AS large_object
    CROSS JOIN (VALUES ('SELECT'), ('UPDATE')) AS privilege(name)
)
SELECT concat_ws(
    '|',
    surface,
    object_oid,
    subid,
    privilege,
    allowed::INTEGER,
    grantable::INTEGER
)
FROM capabilities
ORDER BY surface, object_oid, subid, privilege;
COMMIT;
