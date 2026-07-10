-- Permanent table-local guards for non-reusable Lease identities and monotonic
-- fencing tokens. Exceptional repair requires reviewed schema-owner DDL.
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
$guard$;

-- PostgreSQL has no CREATE TRIGGER IF NOT EXISTS. Create only missing names so
-- a replay never opens a trigger-free window; the postcondition below rejects
-- any same-name trigger with the wrong definition.
DO $install$
DECLARE
    lease_table_oid OID;
BEGIN
    SELECT c.oid
      INTO lease_table_oid
      FROM pg_catalog.pg_class AS c
      JOIN pg_catalog.pg_namespace AS n
        ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = 'ingestion_leases'
       AND c.relkind = 'r';

    IF lease_table_oid IS NULL THEN
        RAISE EXCEPTION
            'public.ingestion_leases is missing or is not an ordinary table';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger AS t
         WHERE t.tgrelid = lease_table_oid
           AND t.tgname = 'trg_ingestion_leases_prevent_delete'
    ) THEN
        EXECUTE $ddl$
            CREATE TRIGGER trg_ingestion_leases_prevent_delete
            BEFORE DELETE ON public.ingestion_leases
            FOR EACH ROW
            EXECUTE FUNCTION public.guard_ingestion_lease_identity()
        $ddl$;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger AS t
         WHERE t.tgrelid = lease_table_oid
           AND t.tgname = 'trg_ingestion_leases_prevent_truncate'
    ) THEN
        EXECUTE $ddl$
            CREATE TRIGGER trg_ingestion_leases_prevent_truncate
            BEFORE TRUNCATE ON public.ingestion_leases
            FOR EACH STATEMENT
            EXECUTE FUNCTION public.guard_ingestion_lease_identity()
        $ddl$;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger AS t
         WHERE t.tgrelid = lease_table_oid
           AND t.tgname = 'trg_ingestion_leases_protect_identity_and_fence'
    ) THEN
        EXECUTE $ddl$
            CREATE TRIGGER trg_ingestion_leases_protect_identity_and_fence
            BEFORE UPDATE OF source_type, lease_key, fencing_token
            ON public.ingestion_leases
            FOR EACH ROW
            EXECUTE FUNCTION public.guard_ingestion_lease_identity()
        $ddl$;
    END IF;
END
$install$;

ALTER TABLE public.ingestion_leases
    ENABLE ALWAYS TRIGGER trg_ingestion_leases_prevent_delete;
ALTER TABLE public.ingestion_leases
    ENABLE ALWAYS TRIGGER trg_ingestion_leases_prevent_truncate;
ALTER TABLE public.ingestion_leases
    ENABLE ALWAYS TRIGGER trg_ingestion_leases_protect_identity_and_fence;

-- Require the zero-argument PL/pgSQL function and the exact three permanent
-- triggers by table/function OID. Unrelated future triggers remain legal.
DO $postcondition$
DECLARE
    lease_table_oid OID;
    guard_function_oid OID;
    source_type_attnum SMALLINT;
    lease_key_attnum SMALLINT;
    fencing_token_attnum SMALLINT;
    actual_function RECORD;
    expected_trigger RECORD;
    actual_trigger RECORD;
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        'pg_catalog, public',
        TRUE
    );

    SELECT c.oid
      INTO lease_table_oid
      FROM pg_catalog.pg_class AS c
      JOIN pg_catalog.pg_namespace AS n
        ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = 'ingestion_leases'
       AND c.relkind = 'r';

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

    FOR expected_trigger IN
        SELECT *
          FROM (VALUES
              (
                  'trg_ingestion_leases_prevent_delete',
                  11::SMALLINT,
                  ''::TEXT
              ),
              (
                  'trg_ingestion_leases_prevent_truncate',
                  34::SMALLINT,
                  ''::TEXT
              ),
              (
                  'trg_ingestion_leases_protect_identity_and_fence',
                  19::SMALLINT,
                  pg_catalog.format(
                      '%s %s %s',
                      source_type_attnum,
                      lease_key_attnum,
                      fencing_token_attnum
                  )
              )
          ) AS required(trigger_name, trigger_type, trigger_columns)
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
           AND t.tgname = expected_trigger.trigger_name;

        IF NOT FOUND
           OR actual_trigger.tgfoid IS DISTINCT FROM guard_function_oid
           OR actual_trigger.tgtype IS DISTINCT FROM
              expected_trigger.trigger_type
           OR actual_trigger.tgenabled <> 'A'
           OR actual_trigger.tgisinternal
           OR actual_trigger.tgconstraint <> 0
           OR actual_trigger.tgdeferrable
           OR actual_trigger.tginitdeferred
           OR actual_trigger.tgnargs <> 0
           OR actual_trigger.trigger_columns IS DISTINCT FROM
              expected_trigger.trigger_columns
           OR actual_trigger.tgqual IS NOT NULL
           OR actual_trigger.tgoldtable IS NOT NULL
           OR actual_trigger.tgnewtable IS NOT NULL
           OR actual_trigger.argument_bytes <> 0 THEN
            RAISE EXCEPTION USING
                MESSAGE = pg_catalog.format(
                    'Lease guard trigger %I has the wrong definition',
                    expected_trigger.trigger_name
                ),
                DETAIL = COALESCE(actual_trigger.definition, 'missing');
        END IF;
    END LOOP;
END
$postcondition$;
