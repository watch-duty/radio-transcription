-- Reconcile the exact dormant ingestion runtime contract after every ordered
-- application migration. Revocations intentionally precede named grants.
BEGIN;

DO $reconcile_preflight$
DECLARE
    expected_relation RECORD;
    actual_relation RECORD;
    actual_type RECORD;
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
      JOIN pg_catalog.pg_roles AS owner ON owner.oid = type.typowner
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
END
$reconcile_preflight$;

\ir 100_ingestion_runtime_hardening.sql

DO $reconcile_database_grant$
BEGIN
    EXECUTE pg_catalog.format(
        'GRANT CONNECT ON DATABASE %I TO app_ingestion_runtime',
        pg_catalog.current_database()
    );
END
$reconcile_database_grant$;

GRANT USAGE ON SCHEMA public TO app_ingestion_runtime;
GRANT USAGE ON TYPE public.feed_status TO app_ingestion_runtime;
GRANT SELECT, UPDATE ON TABLE public.ingestion_leases
    TO app_ingestion_runtime;
GRANT SELECT, UPDATE ON TABLE public.feeds TO app_ingestion_runtime;
GRANT SELECT ON TABLE public.feed_properties TO app_ingestion_runtime;
GRANT SELECT, INSERT ON TABLE public.feed_audit_events
    TO app_ingestion_runtime;

DO $reconcile_postcondition$
DECLARE
    privilege_state RECORD;
    expected_allowed BOOLEAN;
    table_privileges TEXT[] := ARRAY[
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE',
        'REFERENCES', 'TRIGGER'
    ];
    column_privileges TEXT[] := ARRAY[
        'SELECT', 'INSERT', 'UPDATE', 'REFERENCES'
    ];
BEGIN
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
                MESSAGE = 'unexpected PUBLIC/inherited effective privilege',
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
            RAISE EXCEPTION
                'unexpected PUBLIC/inherited effective column privilege';
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
               (type.typrelid = 0 AND type.typtype IN ('b', 'd', 'e', 'm', 'r'))
               OR (type.typtype = 'c' AND type_relation.relkind = 'c')
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
$reconcile_postcondition$;

COMMIT;
