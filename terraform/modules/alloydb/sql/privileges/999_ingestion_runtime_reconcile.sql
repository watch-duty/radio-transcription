-- Reconcile the exact dormant ingestion runtime contract after every ordered
-- application migration. Revocations intentionally precede named grants.
BEGIN;

DO $preflight$
DECLARE
    expected_relation RECORD;
    actual_relation RECORD;
    actual_type RECORD;
    owned_object_count BIGINT;
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        'pg_catalog, public',
        TRUE
    );

    IF CURRENT_USER <> 'postgres' THEN
        RAISE EXCEPTION
            'ingestion privilege reconciliation must run as postgres';
    END IF;

    FOR expected_relation IN
        SELECT *
          FROM (VALUES
              ('ingestion_leases'),
              ('feeds'),
              ('feed_properties'),
              ('feed_audit_events')
          ) AS expected(relation_name)
    LOOP
        SELECT relation.relkind, owner.rolname AS owner_name
          INTO actual_relation
          FROM pg_catalog.pg_class AS relation
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
          JOIN pg_catalog.pg_roles AS owner
            ON owner.oid = relation.relowner
         WHERE namespace.nspname = 'public'
           AND relation.relname = expected_relation.relation_name;

        IF NOT FOUND OR actual_relation.relkind <> 'r' THEN
            RAISE EXCEPTION USING
                MESSAGE = pg_catalog.format(
                    'missing expected ingestion privilege object public.%I',
                    expected_relation.relation_name
                );
        END IF;
        IF actual_relation.owner_name <> 'postgres' THEN
            RAISE EXCEPTION USING
                MESSAGE = pg_catalog.format(
                    'public.%I must remain owned by postgres',
                    expected_relation.relation_name
                ),
                DETAIL = pg_catalog.format(
                    'actual_owner=%I',
                    actual_relation.owner_name
                );
        END IF;
    END LOOP;

    SELECT type.typtype, owner.rolname AS owner_name
      INTO actual_type
      FROM pg_catalog.pg_type AS type
      JOIN pg_catalog.pg_namespace AS namespace
        ON namespace.oid = type.typnamespace
      JOIN pg_catalog.pg_roles AS owner
        ON owner.oid = type.typowner
     WHERE namespace.nspname = 'public'
       AND type.typname = 'feed_status';

    IF NOT FOUND OR actual_type.typtype <> 'e' THEN
        RAISE EXCEPTION
            'missing expected ingestion privilege object public.feed_status';
    END IF;
    IF actual_type.owner_name <> 'postgres' THEN
        RAISE EXCEPTION
            'public.feed_status must remain owned by postgres';
    END IF;

    SELECT
        (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_database AS database
          WHERE database.datdba = role.oid)
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_namespace AS namespace
          WHERE namespace.nspowner = role.oid)
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_class AS relation
          WHERE relation.relowner = role.oid)
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_proc AS procedure
          WHERE procedure.proowner = role.oid)
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_type AS type
          WHERE type.typowner = role.oid)
      INTO owned_object_count
      FROM pg_catalog.pg_roles AS role
     WHERE role.rolname = 'app_ingestion_runtime';

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'missing expected ingestion privilege role app_ingestion_runtime';
    END IF;
    IF owned_object_count <> 0 THEN
        RAISE EXCEPTION
            'app_ingestion_runtime must not own database objects';
    END IF;
END
$preflight$;

ALTER ROLE app_ingestion_runtime
    NOLOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOREPLICATION
    NOBYPASSRLS
    CONNECTION LIMIT -1;
ALTER ROLE app_ingestion_runtime PASSWORD NULL;
ALTER ROLE app_ingestion_runtime RESET ALL;

DO $memberships$
DECLARE
    parent_role RECORD;
BEGIN
    FOR parent_role IN
        SELECT parent.rolname
          FROM pg_catalog.pg_auth_members AS membership
          JOIN pg_catalog.pg_roles AS parent
            ON parent.oid = membership.roleid
          JOIN pg_catalog.pg_roles AS member
            ON member.oid = membership.member
         WHERE member.rolname = 'app_ingestion_runtime'
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE %I FROM app_ingestion_runtime',
            parent_role.rolname
        );
    END LOOP;
END
$memberships$;

DO $database_revoke$
BEGIN
    EXECUTE pg_catalog.format(
        'REVOKE CREATE, TEMPORARY ON DATABASE %I FROM PUBLIC',
        pg_catalog.current_database()
    );
    EXECUTE pg_catalog.format(
        'REVOKE ALL PRIVILEGES ON DATABASE %I FROM app_ingestion_runtime',
        pg_catalog.current_database()
    );
END
$database_revoke$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL PRIVILEGES ON SCHEMA public FROM app_ingestion_runtime;
-- PostgreSQL also removes the corresponding per-column ACLs for these
-- table-level revocations. CI injects both direct and PUBLIC column drift.
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public
    FROM app_ingestion_runtime;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public
    FROM app_ingestion_runtime;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA public
    FROM app_ingestion_runtime;
REVOKE ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA public FROM PUBLIC;

DO $type_revoke$
DECLARE
    application_type RECORD;
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
               (
                   type.typrelid = 0
                   AND type.typtype IN ('b', 'd', 'e', 'm', 'r')
               )
               OR (
                   type.typtype = 'c'
                   AND type_relation.relkind = 'c'
               )
           )
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON TYPE %I.%I FROM app_ingestion_runtime',
            application_type.nspname,
            application_type.typname
        );
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON TYPE %I.%I FROM PUBLIC',
            application_type.nspname,
            application_type.typname
        );
    END LOOP;
END
$type_revoke$;

-- Global revokes remove built-in function/type PUBLIC defaults; scoped revokes
-- also remove explicit public-schema additions.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON TABLES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON SEQUENCES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON ROUTINES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE EXECUTE ON ROUTINES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON TYPES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE USAGE ON TYPES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON TABLES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON SEQUENCES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON ROUTINES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE EXECUTE ON ROUTINES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON TYPES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE USAGE ON TYPES FROM PUBLIC;

DO $database_grant$
BEGIN
    EXECUTE pg_catalog.format(
        'GRANT CONNECT ON DATABASE %I TO app_ingestion_runtime',
        pg_catalog.current_database()
    );
END
$database_grant$;

GRANT USAGE ON SCHEMA public TO app_ingestion_runtime;
GRANT USAGE ON TYPE public.feed_status TO app_ingestion_runtime;
GRANT SELECT, UPDATE ON TABLE public.ingestion_leases
    TO app_ingestion_runtime;
GRANT SELECT, UPDATE ON TABLE public.feeds TO app_ingestion_runtime;
GRANT SELECT ON TABLE public.feed_properties TO app_ingestion_runtime;
GRANT SELECT, INSERT ON TABLE public.feed_audit_events
    TO app_ingestion_runtime;

DO $postcondition$
DECLARE
    role_oid OID;
    role_state RECORD;
    parent_count BIGINT;
    default_table_grant_count BIGINT;
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
BEGIN
    SELECT
        oid,
        rolcanlogin,
        rolsuper,
        rolinherit,
        rolcreaterole,
        rolcreatedb,
        rolreplication,
        rolbypassrls,
        rolconnlimit,
        rolconfig
      INTO role_state
      FROM pg_catalog.pg_roles
     WHERE rolname = 'app_ingestion_runtime';

    IF NOT FOUND
       OR role_state.rolcanlogin
       OR role_state.rolsuper
       OR NOT role_state.rolinherit
       OR role_state.rolcreaterole
       OR role_state.rolcreatedb
       OR role_state.rolreplication
       OR role_state.rolbypassrls
       OR role_state.rolconnlimit <> -1
       OR role_state.rolconfig IS NOT NULL THEN
        RAISE EXCEPTION
            'app_ingestion_runtime has unsafe role attributes';
    END IF;
    role_oid := role_state.oid;

    SELECT pg_catalog.count(*)
      INTO parent_count
      FROM pg_catalog.pg_auth_members
     WHERE member = role_oid;
    IF parent_count <> 0 THEN
        RAISE EXCEPTION
            'app_ingestion_runtime retains an inherited parent role';
    END IF;

    SELECT pg_catalog.count(*)
      INTO default_table_grant_count
      FROM pg_catalog.pg_default_acl AS defaults
      CROSS JOIN LATERAL pg_catalog.aclexplode(defaults.defaclacl) AS acl
     WHERE defaults.defaclobjtype = 'r'
       AND acl.grantee IN (0::OID, role_oid)
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
    IF default_table_grant_count <> 0 THEN
        RAISE EXCEPTION
            'future tables expose DML to app_ingestion_runtime or PUBLIC';
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
            'app_ingestion_runtime',
            privilege_state.oid,
            privilege_state.name
        ) IS DISTINCT FROM expected_allowed
           OR pg_catalog.has_table_privilege(
               'app_ingestion_runtime',
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
               'app_ingestion_runtime',
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
            'app_ingestion_runtime',
            privilege_state.oid,
            privilege_state.attnum,
            privilege_state.name
        ) IS DISTINCT FROM expected_allowed
           OR pg_catalog.has_column_privilege(
               'app_ingestion_runtime',
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
        'app_ingestion_runtime',
        pg_catalog.current_database(),
        'CONNECT'
    )
       OR pg_catalog.has_database_privilege(
           'app_ingestion_runtime',
           pg_catalog.current_database(),
           'CONNECT WITH GRANT OPTION'
       )
       OR pg_catalog.has_database_privilege(
           'app_ingestion_runtime',
           pg_catalog.current_database(),
           'CREATE'
       )
       OR pg_catalog.has_database_privilege(
           'app_ingestion_runtime',
           pg_catalog.current_database(),
           'TEMPORARY'
       )
       OR NOT pg_catalog.has_schema_privilege(
           'app_ingestion_runtime',
           'public',
           'USAGE'
       )
       OR pg_catalog.has_schema_privilege(
           'app_ingestion_runtime',
           'public',
           'USAGE WITH GRANT OPTION'
       )
       OR pg_catalog.has_schema_privilege(
           'app_ingestion_runtime',
           'public',
           'CREATE'
       )
       OR NOT pg_catalog.has_type_privilege(
           'app_ingestion_runtime',
           'public.feed_status',
           'USAGE'
       )
       OR pg_catalog.has_type_privilege(
           'app_ingestion_runtime',
           'public.feed_status',
           'USAGE WITH GRANT OPTION'
       ) THEN
        RAISE EXCEPTION
            'app_ingestion_runtime database/schema/type contract failed';
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
               'app_ingestion_runtime',
               relation.oid,
               privilege.name
           )
    ) OR EXISTS (
        SELECT 1
          FROM pg_catalog.pg_proc AS procedure
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = procedure.pronamespace
         WHERE namespace.nspname = 'public'
           AND pg_catalog.has_function_privilege(
               'app_ingestion_runtime',
               procedure.oid,
               'EXECUTE'
           )
    ) OR EXISTS (
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
               'app_ingestion_runtime',
               type.oid,
               'USAGE'
           )
    ) THEN
        RAISE EXCEPTION
            'app_ingestion_runtime has sequence/function/unrelated-type rights';
    END IF;
END
$postcondition$;

COMMIT;
