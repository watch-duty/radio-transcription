\if :{?runtime_role}
\else
\echo 'runtime_role psql variable is required'
\quit 3
\endif

BEGIN;
CREATE TEMPORARY TABLE ingestion_runtime_contract_input (
    runtime_role NAME PRIMARY KEY
) ON COMMIT DROP;
INSERT INTO ingestion_runtime_contract_input (runtime_role)
VALUES (:'runtime_role');

CREATE TEMP TABLE ingestion_contract_runtime_roles (
    oid OID PRIMARY KEY
) ON COMMIT DROP;

WITH RECURSIVE runtime_roles AS (
    SELECT role.oid
      FROM pg_catalog.pg_roles AS role
     WHERE role.rolname = 'app_ingestion_runtime'
    UNION
    SELECT member.oid
      FROM runtime_roles AS parent
      JOIN pg_catalog.pg_auth_members AS membership
        ON membership.roleid = parent.oid
      JOIN pg_catalog.pg_roles AS member
        ON member.oid = membership.member
)
INSERT INTO pg_temp.ingestion_contract_runtime_roles (oid)
SELECT oid FROM runtime_roles;

DO $contract$
DECLARE
    runtime_role_name NAME;
    runtime_role_oid OID;
    group_role_oid OID;
    runtime_state RECORD;
    group_state RECORD;
    table_privileges TEXT[] := ARRAY[
        'SELECT',
        'INSERT',
        'UPDATE',
        'DELETE',
        'TRUNCATE',
        'REFERENCES',
        'TRIGGER'
    ];
    column_privileges TEXT[] := ARRAY[
        'SELECT',
        'INSERT',
        'UPDATE',
        'REFERENCES'
    ];
    privilege_state RECORD;
    expected_allowed BOOLEAN;
    unexpected_count BIGINT;
    settable_parameter_grant BOOLEAN;
    set_role_membership_mode TEXT;
BEGIN
    SELECT runtime_role
      INTO runtime_role_name
      FROM ingestion_runtime_contract_input;

    SELECT
        oid,
        rolcanlogin,
        rolsuper,
        rolinherit,
        rolcreaterole,
        rolcreatedb,
        rolreplication,
        rolbypassrls
      INTO runtime_state
      FROM pg_catalog.pg_roles
     WHERE rolname = runtime_role_name;
    IF NOT FOUND
       OR NOT runtime_state.rolcanlogin
       OR runtime_state.rolsuper
       OR NOT runtime_state.rolinherit
       OR runtime_state.rolcreaterole
       OR runtime_state.rolcreatedb
       OR runtime_state.rolreplication
       OR runtime_state.rolbypassrls THEN
        RAISE EXCEPTION
            'limited ingestion runtime login has unsafe role attributes';
    END IF;
    runtime_role_oid := runtime_state.oid;

    SELECT
        oid,
        rolcanlogin,
        rolsuper,
        rolinherit,
        rolcreaterole,
        rolcreatedb,
        rolreplication,
        rolbypassrls
      INTO group_state
      FROM pg_catalog.pg_roles
     WHERE rolname = 'app_ingestion_runtime';
    IF NOT FOUND
       OR group_state.rolcanlogin
       OR group_state.rolsuper
       OR NOT group_state.rolinherit
       OR group_state.rolcreaterole
       OR group_state.rolcreatedb
       OR group_state.rolreplication
       OR group_state.rolbypassrls THEN
        RAISE EXCEPTION
            'app_ingestion_runtime has unsafe role attributes';
    END IF;
    group_role_oid := group_state.oid;

    SELECT pg_catalog.count(*)
      INTO unexpected_count
      FROM pg_catalog.pg_auth_members AS membership
     WHERE membership.member = runtime_role_oid
       AND (
           membership.roleid <> group_role_oid
           OR membership.admin_option
       );
    IF unexpected_count <> 0
       OR NOT pg_catalog.pg_has_role(
           runtime_role_name,
           'app_ingestion_runtime',
           'USAGE'
       )
       OR pg_catalog.pg_has_role(
           runtime_role_name,
           'postgres',
           'MEMBER'
       ) THEN
        RAISE EXCEPTION
            'limited ingestion runtime login has dangerous role membership';
    END IF;
    IF pg_catalog.current_setting('server_version_num')::INTEGER >= 160000 THEN
        IF pg_catalog.pg_has_role(
            runtime_role_name,
            'postgres',
            'SET'
        ) THEN
            RAISE EXCEPTION
                'limited ingestion runtime login can SET ROLE postgres';
        END IF;
    END IF;

    SELECT pg_catalog.count(*)
      INTO unexpected_count
      FROM pg_catalog.pg_auth_members
     WHERE member = group_role_oid;
    IF unexpected_count <> 0 THEN
        RAISE EXCEPTION
            'app_ingestion_runtime inherits a parent role';
    END IF;

    -- The ephemeral login receives no direct ACL. Effective access comes only
    -- from its one safe group plus allowed PUBLIC prerequisites.
    SELECT pg_catalog.count(*)
      INTO unexpected_count
      FROM (
          SELECT acl.grantee
            FROM pg_catalog.pg_database AS database
            CROSS JOIN LATERAL
                pg_catalog.aclexplode(database.datacl) AS acl
          UNION ALL
          SELECT acl.grantee
            FROM pg_catalog.pg_namespace AS namespace
            CROSS JOIN LATERAL
                pg_catalog.aclexplode(namespace.nspacl) AS acl
          UNION ALL
          SELECT acl.grantee
            FROM pg_catalog.pg_class AS relation
            CROSS JOIN LATERAL
                pg_catalog.aclexplode(relation.relacl) AS acl
          UNION ALL
          SELECT acl.grantee
            FROM pg_catalog.pg_attribute AS attribute
            CROSS JOIN LATERAL
                pg_catalog.aclexplode(attribute.attacl) AS acl
          UNION ALL
          SELECT acl.grantee
            FROM pg_catalog.pg_proc AS procedure
            CROSS JOIN LATERAL
                pg_catalog.aclexplode(procedure.proacl) AS acl
          UNION ALL
          SELECT acl.grantee
            FROM pg_catalog.pg_type AS type
            CROSS JOIN LATERAL
                pg_catalog.aclexplode(type.typacl) AS acl
          UNION ALL
          SELECT acl.grantee
            FROM pg_catalog.pg_parameter_acl AS parameter_acl
            CROSS JOIN LATERAL
                pg_catalog.aclexplode(parameter_acl.paracl) AS acl
          UNION ALL
          SELECT acl.grantee
            FROM pg_catalog.pg_largeobject_metadata AS large_object
            CROSS JOIN LATERAL
                pg_catalog.aclexplode(large_object.lomacl) AS acl
      ) AS direct_acl
     WHERE direct_acl.grantee = runtime_role_oid;
    IF unexpected_count <> 0 THEN
        RAISE EXCEPTION
            'limited ingestion runtime login has a direct object ACL';
    END IF;

    IF EXISTS (
        WITH privileged_parameters AS (
            SELECT parameter_acl.parname
              FROM pg_catalog.pg_parameter_acl AS parameter_acl
            UNION
            SELECT 'session_replication_role'::TEXT
            UNION
            SELECT 'lo_compat_privileges'::TEXT
        )
        SELECT 1
          FROM pg_temp.ingestion_contract_runtime_roles AS runtime_role
          CROSS JOIN privileged_parameters AS parameter
          CROSS JOIN (VALUES ('SET'), ('ALTER SYSTEM')) AS access(privilege)
         WHERE pg_catalog.has_parameter_privilege(
                   runtime_role.oid,
                   parameter.parname,
                   access.privilege
               )
    ) THEN
        RAISE EXCEPTION
            'runtime member has an effective privileged-parameter grant';
    END IF;
    IF pg_catalog.current_setting('server_version_num')::INTEGER >= 160000 THEN
        EXECUTE $parameter_set_query$
            SELECT EXISTS (
                SELECT 1
                  FROM pg_temp.ingestion_contract_runtime_roles
                       AS runtime_role
                  CROSS JOIN pg_catalog.pg_parameter_acl AS parameter_acl
                  CROSS JOIN LATERAL
                    pg_catalog.aclexplode(parameter_acl.paracl) AS acl
                 WHERE acl.grantee <> 0::OID
                   AND pg_catalog.pg_has_role(
                       runtime_role.oid,
                       acl.grantee,
                       'SET'
                   )
            )
        $parameter_set_query$
        INTO settable_parameter_grant;
    ELSE
        SELECT EXISTS (
            SELECT 1
              FROM pg_temp.ingestion_contract_runtime_roles AS runtime_role
              CROSS JOIN pg_catalog.pg_parameter_acl AS parameter_acl
              CROSS JOIN LATERAL
                pg_catalog.aclexplode(parameter_acl.paracl) AS acl
             WHERE acl.grantee <> 0::OID
               AND pg_catalog.pg_has_role(
                   runtime_role.oid,
                   acl.grantee,
                   'MEMBER'
               )
        )
        INTO settable_parameter_grant;
    END IF;
    IF settable_parameter_grant THEN
        RAISE EXCEPTION
            'runtime member can SET ROLE to a parameter grantee';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_db_role_setting AS role_setting
          CROSS JOIN LATERAL
            unnest(role_setting.setconfig) AS configuration(setting)
          JOIN pg_catalog.pg_settings AS parameter
            ON parameter.name =
               pg_catalog.split_part(configuration.setting, '=', 1)
         WHERE (
                   (
                       role_setting.setrole IN (
                           SELECT runtime_role.oid
                             FROM pg_temp.ingestion_contract_runtime_roles
                                  AS runtime_role
                       )
                       AND role_setting.setdatabase IN (
                           0::OID,
                           (
                               SELECT database.oid
                                 FROM pg_catalog.pg_database AS database
                                WHERE database.datname =
                                      pg_catalog.current_database()
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
                                WHERE database.datname =
                                      pg_catalog.current_database()
                           )
                       )
                   )
               )
           AND parameter.context IN ('superuser', 'superuser-backend')
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
            'runtime member has an unsafe role parameter default';
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
          FROM pg_temp.ingestion_contract_runtime_roles AS runtime_role
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

    set_role_membership_mode := CASE
        WHEN pg_catalog.current_setting('server_version_num')::INTEGER >= 160000
        THEN 'SET'
        ELSE 'MEMBER'
    END;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_proc AS procedure
          CROSS JOIN pg_temp.ingestion_contract_runtime_roles AS runtime_role
         WHERE procedure.oid IN (
                   'pg_catalog.lo_create(oid)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_creat(integer)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_from_bytea(oid, bytea)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_import(text)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_import(text, oid)'::pg_catalog.regprocedure
               )
           AND (
               pg_catalog.has_function_privilege(
                   runtime_role.oid,
                   procedure.oid,
                   'EXECUTE'
               )
               OR pg_catalog.has_function_privilege(
                   runtime_role.oid,
                   procedure.oid,
                   'EXECUTE WITH GRANT OPTION'
               )
           )
        UNION ALL
        SELECT 1
          FROM pg_catalog.pg_proc AS procedure
          CROSS JOIN LATERAL pg_catalog.aclexplode(
              COALESCE(
                  procedure.proacl,
                  pg_catalog.acldefault('f'::"char", procedure.proowner)
              )
          ) AS acl
         WHERE procedure.oid IN (
                   'pg_catalog.lo_create(oid)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_creat(integer)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_from_bytea(oid, bytea)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_import(text)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_import(text, oid)'::pg_catalog.regprocedure
               )
           AND acl.privilege_type = 'EXECUTE'
           AND (
               acl.grantee = 0::OID
               OR EXISTS (
                   SELECT 1
                     FROM pg_temp.ingestion_contract_runtime_roles
                          AS runtime_role
                    WHERE acl.grantee = runtime_role.oid
                       OR (
                           acl.grantee <> 0::OID
                           AND (
                               pg_catalog.pg_has_role(
                                   runtime_role.oid,
                                   acl.grantee,
                                   'USAGE'
                               )
                               OR pg_catalog.pg_has_role(
                                   runtime_role.oid,
                                   acl.grantee,
                                   set_role_membership_mode
                               )
                           )
                       )
               )
           )
    ) THEN
        RAISE EXCEPTION
            'runtime member can execute a large-object creator';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_largeobject_metadata AS large_object
         WHERE EXISTS (
                   SELECT 1
                     FROM pg_temp.ingestion_contract_runtime_roles
                          AS runtime_role
                    WHERE large_object.lomowner = runtime_role.oid
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
                                FROM pg_temp.ingestion_contract_runtime_roles
                                     AS runtime_role
                               WHERE acl.grantee = runtime_role.oid
                                  OR (
                                      acl.grantee <> 0::OID
                                      AND (
                                          pg_catalog.pg_has_role(
                                              runtime_role.oid,
                                              acl.grantee,
                                              'USAGE'
                                          )
                                          OR pg_catalog.pg_has_role(
                                              runtime_role.oid,
                                              acl.grantee,
                                              set_role_membership_mode
                                          )
                                      )
                                  )
                          )
                      )
               )
    ) THEN
        RAISE EXCEPTION
            'runtime member has a large-object capability';
    END IF;

    IF EXISTS (
        WITH creators AS (
            SELECT role.oid
              FROM pg_catalog.pg_roles AS role
             WHERE pg_catalog.has_schema_privilege(
                       role.oid,
                       'public',
                       'CREATE'
                   )
        ),
        object_classes(defacl_type, acldefault_type) AS (
            VALUES
                ('r'::"char", 'r'::"char"),
                ('S'::"char", 's'::"char"),
                ('f'::"char", 'f'::"char"),
                ('T'::"char", 'T'::"char")
        ),
        physical_defaults AS (
            SELECT defaults.*
              FROM pg_catalog.pg_default_acl AS defaults
             WHERE defaults.defaclobjtype IN ('r', 'S', 'f', 'T')
        ),
        future_acl AS (
            SELECT
                creator.oid AS creator_oid,
                object_class.defacl_type,
                COALESCE(
                    global_acl.defaclacl,
                    pg_catalog.acldefault(
                        object_class.acldefault_type,
                        creator.oid
                    )
                ) || COALESCE(
                    schema_acl.defaclacl,
                    '{}'::pg_catalog.aclitem[]
                ) AS acl
              FROM creators AS creator
              CROSS JOIN object_classes AS object_class
              LEFT JOIN physical_defaults AS global_acl
                ON global_acl.defaclrole = creator.oid
               AND global_acl.defaclnamespace = 0
               AND global_acl.defaclobjtype = object_class.defacl_type
              LEFT JOIN physical_defaults AS schema_acl
                ON schema_acl.defaclrole = creator.oid
               AND schema_acl.defaclnamespace =
                       'public'::pg_catalog.regnamespace
               AND schema_acl.defaclobjtype = object_class.defacl_type
        )
        SELECT 1
          FROM future_acl
          CROSS JOIN LATERAL pg_catalog.aclexplode(future_acl.acl) AS acl
         WHERE acl.grantee = 0::OID
            OR EXISTS (
                SELECT 1
                  FROM pg_temp.ingestion_contract_runtime_roles
                       AS runtime_role
                 WHERE acl.grantee = runtime_role.oid
                    OR (
                        acl.grantee <> 0::OID
                        AND pg_catalog.pg_has_role(
                            runtime_role.oid,
                            acl.grantee,
                            'USAGE'
                        )
                    ) OR (
                        acl.grantee <> 0::OID
                        AND pg_catalog.pg_has_role(
                            runtime_role.oid,
                            acl.grantee,
                            set_role_membership_mode
                        )
                    )
            )
    ) THEN
        RAISE EXCEPTION
            'future public objects expose PUBLIC or a runtime member';
    END IF;

    IF pg_catalog.current_setting('server_version_num')::INTEGER >= 170000 THEN
        table_privileges := pg_catalog.array_append(
            table_privileges,
            'MAINTAIN'
        );
    END IF;

    FOR privilege_state IN
        SELECT relation.oid, relation.relname, privilege.name
          FROM pg_catalog.pg_class AS relation
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          CROSS JOIN unnest(table_privileges) AS privilege(name)
         WHERE namespace.nspname = 'public'
           AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
    LOOP
        expected_allowed := CASE privilege_state.relname
            WHEN 'ingestion_leases' THEN
                privilege_state.name IN ('SELECT', 'UPDATE')
            WHEN 'feeds' THEN
                privilege_state.name IN ('SELECT', 'UPDATE')
            WHEN 'feed_properties' THEN
                privilege_state.name = 'SELECT'
            WHEN 'feed_audit_events' THEN
                privilege_state.name IN ('SELECT', 'INSERT')
            ELSE FALSE
        END;

        IF pg_catalog.has_table_privilege(
            runtime_role_name,
            privilege_state.oid,
            privilege_state.name
        ) IS DISTINCT FROM expected_allowed
           OR pg_catalog.has_table_privilege(
               runtime_role_name,
               privilege_state.oid,
               privilege_state.name || ' WITH GRANT OPTION'
           ) THEN
            RAISE EXCEPTION USING
                MESSAGE =
                    'unexpected PUBLIC/inherited effective privilege',
                DETAIL = pg_catalog.format(
                    'relation=public.%I privilege=%s expected=%s',
                    privilege_state.relname,
                    privilege_state.name,
                    expected_allowed
                );
        END IF;

        IF privilege_state.name = ANY(column_privileges)
           AND pg_catalog.has_any_column_privilege(
               runtime_role_name,
               privilege_state.oid,
               privilege_state.name
           ) IS DISTINCT FROM expected_allowed THEN
            RAISE EXCEPTION USING
                MESSAGE =
                    'unexpected PUBLIC/inherited effective column privilege',
                DETAIL = pg_catalog.format(
                    'relation=public.%I privilege=%s expected=%s',
                    privilege_state.relname,
                    privilege_state.name,
                    expected_allowed
                );
        END IF;
    END LOOP;

    FOR privilege_state IN
        SELECT
            relation.oid,
            relation.relname,
            attribute.attnum,
            attribute.attname,
            privilege.name
          FROM pg_catalog.pg_attribute AS attribute
          JOIN pg_catalog.pg_class AS relation
            ON relation.oid = attribute.attrelid
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          CROSS JOIN unnest(column_privileges) AS privilege(name)
         WHERE namespace.nspname = 'public'
           AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped
    LOOP
        expected_allowed := CASE privilege_state.relname
            WHEN 'ingestion_leases' THEN
                privilege_state.name IN ('SELECT', 'UPDATE')
            WHEN 'feeds' THEN
                privilege_state.name IN ('SELECT', 'UPDATE')
            WHEN 'feed_properties' THEN
                privilege_state.name = 'SELECT'
            WHEN 'feed_audit_events' THEN
                privilege_state.name IN ('SELECT', 'INSERT')
            ELSE FALSE
        END;

        IF pg_catalog.has_column_privilege(
            runtime_role_name,
            privilege_state.oid,
            privilege_state.attnum,
            privilege_state.name
        ) IS DISTINCT FROM expected_allowed
           OR pg_catalog.has_column_privilege(
               runtime_role_name,
               privilege_state.oid,
               privilege_state.attnum,
               privilege_state.name || ' WITH GRANT OPTION'
           ) THEN
            RAISE EXCEPTION USING
                MESSAGE =
                    'unexpected PUBLIC/inherited effective column privilege',
                DETAIL = pg_catalog.format(
                    'relation=public.%I column=%I privilege=%s expected=%s',
                    privilege_state.relname,
                    privilege_state.attname,
                    privilege_state.name,
                    expected_allowed
                );
        END IF;
    END LOOP;

    IF NOT pg_catalog.has_database_privilege(
        runtime_role_name,
        pg_catalog.current_database(),
        'CONNECT'
    )
       OR pg_catalog.has_database_privilege(
           runtime_role_name,
           pg_catalog.current_database(),
           'CONNECT WITH GRANT OPTION'
       )
       OR pg_catalog.has_database_privilege(
           runtime_role_name,
           pg_catalog.current_database(),
           'CREATE'
       )
       OR pg_catalog.has_database_privilege(
           runtime_role_name,
           pg_catalog.current_database(),
           'TEMPORARY'
       )
       OR NOT pg_catalog.has_schema_privilege(
           runtime_role_name,
           'public',
           'USAGE'
       )
       OR pg_catalog.has_schema_privilege(
           runtime_role_name,
           'public',
           'USAGE WITH GRANT OPTION'
       )
       OR pg_catalog.has_schema_privilege(
           runtime_role_name,
           'public',
           'CREATE'
       )
       OR NOT pg_catalog.has_type_privilege(
           runtime_role_name,
           'public.feed_status',
           'USAGE'
       )
       OR pg_catalog.has_type_privilege(
           runtime_role_name,
           'public.feed_status',
           'USAGE WITH GRANT OPTION'
       ) THEN
        RAISE EXCEPTION
            'runtime database/schema/feed_status contract failed';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class AS relation
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          CROSS JOIN unnest(
              ARRAY['USAGE', 'SELECT', 'UPDATE']
          ) AS privilege(name)
         WHERE namespace.nspname = 'public'
           AND relation.relkind = 'S'
           AND pg_catalog.has_sequence_privilege(
               runtime_role_name,
               relation.oid,
               privilege.name
           )
    ) THEN
        RAISE EXCEPTION
            'runtime has an effective sequence privilege';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_proc AS procedure
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = procedure.pronamespace
         WHERE namespace.nspname = 'public'
           AND pg_catalog.has_function_privilege(
               runtime_role_name,
               procedure.oid,
               'EXECUTE'
           )
    ) THEN
        RAISE EXCEPTION
            'runtime has an effective application function privilege';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_type AS type
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = type.typnamespace
          LEFT JOIN pg_catalog.pg_class AS type_relation
            ON type_relation.oid = type.typrelid
         WHERE namespace.nspname = 'public'
           AND type.typelem = 0
           AND (
               (
                   type.typrelid = 0
                   AND type.typtype IN ('b', 'd', 'e', 'm', 'r')
               )
               OR (
                   type.typtype = 'c'
                   AND type_relation.relkind = 'c'
               )
           )
           AND type.typname <> 'feed_status'
           AND pg_catalog.has_type_privilege(
               runtime_role_name,
               type.oid,
               'USAGE'
           )
    ) THEN
        RAISE EXCEPTION
            'runtime has an unrelated application type privilege';
    END IF;

    SELECT pg_catalog.count(*)
      INTO unexpected_count
      FROM pg_catalog.pg_shdepend AS dependency
      JOIN pg_temp.ingestion_contract_runtime_roles AS owner
        ON owner.oid = dependency.refobjid
     WHERE dependency.refclassid =
               'pg_catalog.pg_authid'::pg_catalog.regclass
       AND dependency.deptype = 'o';
    IF unexpected_count <> 0 THEN
        RAISE EXCEPTION
            'runtime login or group owns database objects';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM (VALUES
              ('ingestion_leases'),
              ('feeds'),
              ('feed_properties'),
              ('feed_audit_events')
          ) AS expected(relation_name)
          LEFT JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.nspname = 'public'
          LEFT JOIN pg_catalog.pg_class AS relation
            ON relation.relnamespace = namespace.oid
           AND relation.relname = expected.relation_name
          LEFT JOIN pg_catalog.pg_roles AS owner
            ON owner.oid = relation.relowner
         WHERE namespace.nspname IS DISTINCT FROM 'public'
            OR relation.relkind IS DISTINCT FROM 'r'
            OR owner.rolname IS DISTINCT FROM 'postgres'
    ) THEN
        RAISE EXCEPTION
            'expected privilege tables must remain owned by postgres';
    END IF;
END
$contract$;
COMMIT;
