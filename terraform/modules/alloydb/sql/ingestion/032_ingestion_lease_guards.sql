-- Install and verify the permanent Lease guards in one top-level statement.
-- PostgreSQL DDL is transactional, so psql -f cannot persist a partial repair.
DO $migration$
DECLARE
    lease_table_oid OID;
    lease_table_kind "char";
    lease_table_persistence "char";
    lease_table_is_partition BOOLEAN;
    lease_inheritance_links BIGINT;
    guard_function_oid OID;
    source_type_attnum SMALLINT;
    lease_key_attnum SMALLINT;
    fencing_token_attnum SMALLINT;
    trigger_names TEXT[];
    trigger_types SMALLINT[];
    trigger_columns TEXT[];
    trigger_index INTEGER;
    actual_function RECORD;
    actual_trigger RECORD;
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        'pg_catalog, public',
        TRUE
    );

    -- Serialize replay and schema-owner trigger DDL before catalog preflight.
    EXECUTE $lock$
        LOCK TABLE ONLY public.ingestion_leases
        IN SHARE ROW EXCLUSIVE MODE
    $lock$;

    SELECT
        c.oid,
        c.relkind,
        c.relpersistence,
        c.relispartition
      INTO
        lease_table_oid,
        lease_table_kind,
        lease_table_persistence,
        lease_table_is_partition
      FROM pg_catalog.pg_class AS c
      JOIN pg_catalog.pg_namespace AS n
        ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = 'ingestion_leases';

    SELECT pg_catalog.count(*)
      INTO lease_inheritance_links
      FROM pg_catalog.pg_inherits AS i
     WHERE i.inhrelid = lease_table_oid
        OR i.inhparent = lease_table_oid;

    IF lease_table_oid IS NULL
       OR lease_table_kind <> 'r'
       OR lease_table_persistence <> 'p'
       OR lease_table_is_partition
       OR lease_inheritance_links <> 0 THEN
        RAISE EXCEPTION
            'public.ingestion_leases is not a permanent standalone ordinary table';
    END IF;

    SELECT a.attnum
      INTO source_type_attnum
      FROM pg_catalog.pg_attribute AS a
     WHERE a.attrelid = lease_table_oid
       AND a.attname = 'source_type'
       AND a.attnum > 0
       AND NOT a.attisdropped;

    SELECT a.attnum
      INTO lease_key_attnum
      FROM pg_catalog.pg_attribute AS a
     WHERE a.attrelid = lease_table_oid
       AND a.attname = 'lease_key'
       AND a.attnum > 0
       AND NOT a.attisdropped;

    SELECT a.attnum
      INTO fencing_token_attnum
      FROM pg_catalog.pg_attribute AS a
     WHERE a.attrelid = lease_table_oid
       AND a.attname = 'fencing_token'
       AND a.attnum > 0
       AND NOT a.attisdropped;

    IF source_type_attnum IS NULL
       OR lease_key_attnum IS NULL
       OR fencing_token_attnum IS NULL THEN
        RAISE EXCEPTION
            'public.ingestion_leases is missing a guarded Lease column';
    END IF;

    trigger_names := ARRAY[
        'trg_ingestion_leases_prevent_delete',
        'trg_ingestion_leases_prevent_truncate',
        'trg_ingestion_leases_protect_identity_and_fence'
    ];
    trigger_types := ARRAY[11, 34, 19]::SMALLINT[];
    trigger_columns := ARRAY[
        '',
        '',
        pg_catalog.format(
            '%s %s %s',
            source_type_attnum,
            lease_key_attnum,
            fencing_token_attnum
        )
    ];

    SELECT p.oid
      INTO guard_function_oid
      FROM pg_catalog.pg_proc AS p
      JOIN pg_catalog.pg_namespace AS n
        ON n.oid = p.pronamespace
     WHERE n.nspname = 'public'
       AND p.proname = 'guard_ingestion_lease_identity'
       AND p.pronargs = 0;

    -- Reject every malformed same-name trigger before any catalog mutation.
    -- A disabled otherwise-exact trigger is a recoverable partial state.
    FOR trigger_index IN 1..pg_catalog.array_length(trigger_names, 1)
    LOOP
        SELECT
            t.tgfoid,
            t.tgtype,
            t.tgenabled,
            t.tgisinternal,
            t.tgconstraint,
            t.tgdeferrable,
            t.tginitdeferred,
            t.tgnargs,
            t.tgattr::TEXT AS trigger_columns,
            t.tgqual,
            t.tgoldtable,
            t.tgnewtable,
            pg_catalog.octet_length(t.tgargs) AS argument_bytes,
            pg_catalog.pg_get_triggerdef(t.oid, TRUE) AS definition
          INTO actual_trigger
          FROM pg_catalog.pg_trigger AS t
         WHERE t.tgrelid = lease_table_oid
           AND t.tgname = trigger_names[trigger_index];

        IF FOUND
           AND (
               actual_trigger.tgfoid IS DISTINCT FROM guard_function_oid
               OR actual_trigger.tgtype IS DISTINCT FROM
                  trigger_types[trigger_index]
               OR actual_trigger.tgisinternal
               OR actual_trigger.tgconstraint <> 0
               OR actual_trigger.tgdeferrable
               OR actual_trigger.tginitdeferred
               OR actual_trigger.tgnargs <> 0
               OR actual_trigger.trigger_columns IS DISTINCT FROM
                  trigger_columns[trigger_index]
               OR actual_trigger.tgqual IS NOT NULL
               OR actual_trigger.tgoldtable IS NOT NULL
               OR actual_trigger.tgnewtable IS NOT NULL
               OR actual_trigger.argument_bytes <> 0
           ) THEN
            RAISE EXCEPTION USING
                MESSAGE = pg_catalog.format(
                    'Existing Lease guard trigger %I has the wrong definition',
                    trigger_names[trigger_index]
                ),
                DETAIL = COALESCE(actual_trigger.definition, 'missing');
        END IF;
    END LOOP;

    EXECUTE $function_ddl$
        CREATE OR REPLACE FUNCTION public.guard_ingestion_lease_identity()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $guard$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'ingestion Lease identities cannot be deleted',
                    DETAIL = pg_catalog.format(
                        'DELETE rejected for Lease (%s, %s)',
                        OLD.source_type,
                        OLD.lease_key
                    ),
                    HINT = 'Exceptional repair requires reviewed schema-owner DDL that alters these guards and restores them before normal operation resumes.';
            ELSIF TG_OP = 'TRUNCATE' THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'ingestion Lease identities cannot be truncated',
                    DETAIL = 'TRUNCATE rejected for public.ingestion_leases',
                    HINT = 'Exceptional repair requires reviewed schema-owner DDL that alters these guards and restores them before normal operation resumes.';
            ELSIF TG_OP = 'UPDATE' THEN
                IF NEW.source_type IS DISTINCT FROM OLD.source_type THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '23514',
                        MESSAGE = 'ingestion Lease source identity cannot be changed',
                        DETAIL = pg_catalog.format(
                            'source_type re-key rejected from %s to %s for Lease %s',
                            OLD.source_type,
                            NEW.source_type,
                            OLD.lease_key
                        ),
                        HINT = 'Exceptional repair requires reviewed schema-owner DDL that alters these guards and restores them before normal operation resumes.';
                END IF;

                IF NEW.lease_key IS DISTINCT FROM OLD.lease_key THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '23514',
                        MESSAGE = 'ingestion Lease key cannot be changed',
                        DETAIL = pg_catalog.format(
                            'lease_key re-key rejected from %s to %s for source %s',
                            OLD.lease_key,
                            NEW.lease_key,
                            OLD.source_type
                        ),
                        HINT = 'Exceptional repair requires reviewed schema-owner DDL that alters these guards and restores them before normal operation resumes.';
                END IF;

                IF NEW.fencing_token < OLD.fencing_token THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '23514',
                        MESSAGE = 'ingestion Lease fencing token cannot regress',
                        DETAIL = pg_catalog.format(
                            'fencing_token regression rejected from %s to %s for Lease (%s, %s)',
                            OLD.fencing_token,
                            NEW.fencing_token,
                            OLD.source_type,
                            OLD.lease_key
                        ),
                        HINT = 'Exceptional repair requires reviewed schema-owner DDL that alters these guards and restores them before normal operation resumes.';
                END IF;

                RETURN NEW;
            END IF;

            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'unsupported ingestion Lease guard operation',
                DETAIL = pg_catalog.format(
                    'guard_ingestion_lease_identity received TG_OP=%s',
                    TG_OP
                ),
                HINT = 'Exceptional repair requires reviewed schema-owner DDL that alters these guards and restores them before normal operation resumes.';
        END;
        $guard$
    $function_ddl$;

    SELECT
        p.oid,
        p.prokind,
        p.pronargs,
        p.prorettype,
        p.prosecdef,
        p.proleakproof,
        p.proconfig,
        l.lanname,
        pg_catalog.pg_get_functiondef(p.oid) AS definition
      INTO actual_function
      FROM pg_catalog.pg_proc AS p
      JOIN pg_catalog.pg_namespace AS n
        ON n.oid = p.pronamespace
      JOIN pg_catalog.pg_language AS l
        ON l.oid = p.prolang
     WHERE n.nspname = 'public'
       AND p.proname = 'guard_ingestion_lease_identity'
       AND p.pronargs = 0;

    IF NOT FOUND
       OR actual_function.prokind <> 'f'
       OR actual_function.pronargs <> 0
       OR actual_function.prorettype <> 'trigger'::regtype
       OR actual_function.prosecdef
       OR actual_function.proleakproof
       OR actual_function.proconfig IS NOT NULL
       OR actual_function.lanname <> 'plpgsql'
       OR pg_catalog.strpos(
              actual_function.definition,
              $fragment$IF TG_OP = 'DELETE' THEN$fragment$
          ) = 0
       OR pg_catalog.strpos(
              actual_function.definition,
              $fragment$ELSIF TG_OP = 'TRUNCATE' THEN$fragment$
          ) = 0
       OR pg_catalog.strpos(
              actual_function.definition,
              $fragment$ELSIF TG_OP = 'UPDATE' THEN$fragment$
          ) = 0
       OR pg_catalog.strpos(
              actual_function.definition,
              $fragment$NEW.source_type IS DISTINCT FROM OLD.source_type$fragment$
          ) = 0
       OR pg_catalog.strpos(
              actual_function.definition,
              $fragment$NEW.lease_key IS DISTINCT FROM OLD.lease_key$fragment$
          ) = 0
       OR pg_catalog.strpos(
              actual_function.definition,
              $fragment$NEW.fencing_token < OLD.fencing_token$fragment$
          ) = 0
       OR pg_catalog.strpos(
              actual_function.definition,
              $fragment$ERRCODE = '23514'$fragment$
          ) = 0
       OR pg_catalog.strpos(
              actual_function.definition,
              'Exceptional repair requires reviewed schema-owner DDL'
          ) = 0 THEN
        RAISE EXCEPTION
            'public.guard_ingestion_lease_identity() has the wrong definition';
    END IF;

    guard_function_oid := actual_function.oid;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger AS t
         WHERE t.tgrelid = lease_table_oid
           AND t.tgname = trigger_names[1]
    ) THEN
        EXECUTE $trigger_ddl$
            CREATE TRIGGER trg_ingestion_leases_prevent_delete
            BEFORE DELETE ON public.ingestion_leases
            FOR EACH ROW
            EXECUTE FUNCTION public.guard_ingestion_lease_identity()
        $trigger_ddl$;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger AS t
         WHERE t.tgrelid = lease_table_oid
           AND t.tgname = trigger_names[2]
    ) THEN
        EXECUTE $trigger_ddl$
            CREATE TRIGGER trg_ingestion_leases_prevent_truncate
            BEFORE TRUNCATE ON public.ingestion_leases
            FOR EACH STATEMENT
            EXECUTE FUNCTION public.guard_ingestion_lease_identity()
        $trigger_ddl$;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger AS t
         WHERE t.tgrelid = lease_table_oid
           AND t.tgname = trigger_names[3]
    ) THEN
        EXECUTE $trigger_ddl$
            CREATE TRIGGER trg_ingestion_leases_protect_identity_and_fence
            BEFORE UPDATE OF source_type, lease_key, fencing_token
            ON public.ingestion_leases
            FOR EACH ROW
            EXECUTE FUNCTION public.guard_ingestion_lease_identity()
        $trigger_ddl$;
    END IF;

    -- Validate restored and pre-existing shapes before enabling any trigger.
    FOR trigger_index IN 1..pg_catalog.array_length(trigger_names, 1)
    LOOP
        SELECT
            t.tgfoid,
            t.tgtype,
            t.tgenabled,
            t.tgisinternal,
            t.tgconstraint,
            t.tgdeferrable,
            t.tginitdeferred,
            t.tgnargs,
            t.tgattr::TEXT AS trigger_columns,
            t.tgqual,
            t.tgoldtable,
            t.tgnewtable,
            pg_catalog.octet_length(t.tgargs) AS argument_bytes,
            pg_catalog.pg_get_triggerdef(t.oid, TRUE) AS definition
          INTO actual_trigger
          FROM pg_catalog.pg_trigger AS t
         WHERE t.tgrelid = lease_table_oid
           AND t.tgname = trigger_names[trigger_index];

        IF NOT FOUND
           OR actual_trigger.tgfoid IS DISTINCT FROM guard_function_oid
           OR actual_trigger.tgtype IS DISTINCT FROM
              trigger_types[trigger_index]
           OR actual_trigger.tgisinternal
           OR actual_trigger.tgconstraint <> 0
           OR actual_trigger.tgdeferrable
           OR actual_trigger.tginitdeferred
           OR actual_trigger.tgnargs <> 0
           OR actual_trigger.trigger_columns IS DISTINCT FROM
              trigger_columns[trigger_index]
           OR actual_trigger.tgqual IS NOT NULL
           OR actual_trigger.tgoldtable IS NOT NULL
           OR actual_trigger.tgnewtable IS NOT NULL
           OR actual_trigger.argument_bytes <> 0 THEN
            RAISE EXCEPTION USING
                MESSAGE = pg_catalog.format(
                    'Restored Lease guard trigger %I has the wrong definition',
                    trigger_names[trigger_index]
                ),
                DETAIL = COALESCE(actual_trigger.definition, 'missing');
        END IF;
    END LOOP;

    FOR trigger_index IN 1..pg_catalog.array_length(trigger_names, 1)
    LOOP
        EXECUTE pg_catalog.format(
            'ALTER TABLE public.ingestion_leases ENABLE ALWAYS TRIGGER %I',
            trigger_names[trigger_index]
        );
    END LOOP;

    -- Verify the exact final state before the one top-level statement commits.
    FOR trigger_index IN 1..pg_catalog.array_length(trigger_names, 1)
    LOOP
        SELECT
            t.tgfoid,
            t.tgtype,
            t.tgenabled,
            t.tgisinternal,
            t.tgconstraint,
            t.tgdeferrable,
            t.tginitdeferred,
            t.tgnargs,
            t.tgattr::TEXT AS trigger_columns,
            t.tgqual,
            t.tgoldtable,
            t.tgnewtable,
            pg_catalog.octet_length(t.tgargs) AS argument_bytes,
            pg_catalog.pg_get_triggerdef(t.oid, TRUE) AS definition
          INTO actual_trigger
          FROM pg_catalog.pg_trigger AS t
         WHERE t.tgrelid = lease_table_oid
           AND t.tgname = trigger_names[trigger_index];

        IF NOT FOUND
           OR actual_trigger.tgfoid IS DISTINCT FROM guard_function_oid
           OR actual_trigger.tgtype IS DISTINCT FROM
              trigger_types[trigger_index]
           OR actual_trigger.tgenabled <> 'A'
           OR actual_trigger.tgisinternal
           OR actual_trigger.tgconstraint <> 0
           OR actual_trigger.tgdeferrable
           OR actual_trigger.tginitdeferred
           OR actual_trigger.tgnargs <> 0
           OR actual_trigger.trigger_columns IS DISTINCT FROM
              trigger_columns[trigger_index]
           OR actual_trigger.tgqual IS NOT NULL
           OR actual_trigger.tgoldtable IS NOT NULL
           OR actual_trigger.tgnewtable IS NOT NULL
           OR actual_trigger.argument_bytes <> 0 THEN
            RAISE EXCEPTION USING
                MESSAGE = pg_catalog.format(
                    'Lease guard trigger %I has the wrong final definition',
                    trigger_names[trigger_index]
                ),
                DETAIL = COALESCE(actual_trigger.definition, 'missing');
        END IF;
    END LOOP;
END
$migration$;
