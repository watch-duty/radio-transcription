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
      ) AS direct_acl
     WHERE direct_acl.grantee = runtime_role_oid;
    IF unexpected_count <> 0 THEN
        RAISE EXCEPTION
            'limited ingestion runtime login has a direct object ACL';
    END IF;

    SELECT pg_catalog.count(*)
      INTO unexpected_count
      FROM pg_catalog.pg_default_acl AS defaults
      CROSS JOIN LATERAL pg_catalog.aclexplode(defaults.defaclacl) AS acl
     WHERE defaults.defaclobjtype = 'r'
       AND acl.grantee IN (0::OID, group_role_oid, runtime_role_oid)
       AND acl.privilege_type IN (
           'SELECT',
           'INSERT',
           'UPDATE',
           'DELETE',
           'TRUNCATE',
           'REFERENCES',
           'TRIGGER',
           'MAINTAIN'
       );
    IF unexpected_count <> 0 THEN
        RAISE EXCEPTION
            'future tables expose runtime DML through a default ACL';
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

    SELECT
        (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_database
          WHERE datdba IN (runtime_role_oid, group_role_oid))
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_namespace
          WHERE nspowner IN (runtime_role_oid, group_role_oid))
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_class
          WHERE relowner IN (runtime_role_oid, group_role_oid))
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_proc
          WHERE proowner IN (runtime_role_oid, group_role_oid))
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_type
          WHERE typowner IN (runtime_role_oid, group_role_oid))
      INTO unexpected_count;
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
