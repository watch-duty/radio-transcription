BEGIN;

-- OID-anchored Lease shape and structural behavior contract. All fixture
-- writes are rolled back at the end of this file.
DO $contract$
DECLARE
    lease_table_oid OID;
    source_types_oid OID;
    source_type_attnum SMALLINT;
    lease_key_attnum SMALLINT;
    source_slug_attnum SMALLINT;
    fixture_source_type TEXT;
    fixture_key TEXT;
    expected_column RECORD;
    actual_column RECORD;
    actual_constraint RECORD;
    lease_row public.ingestion_leases%ROWTYPE;
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

    IF lease_table_oid IS NULL THEN
        RAISE EXCEPTION 'public.ingestion_leases is not an ordinary table';
    END IF;

    FOR expected_column IN
        SELECT *
          FROM (VALUES
              ('source_type', 'text'::regtype::OID, -1, TRUE, NULL::TEXT),
              ('lease_key', 'text'::regtype::OID, -1, TRUE, NULL::TEXT),
              (
                  'status',
                  'public.feed_status'::regtype::OID,
                  -1,
                  TRUE,
                  '''deactivated''::feed_status'
              ),
              ('worker_id', 'uuid'::regtype::OID, -1, FALSE, NULL::TEXT),
              ('fencing_token', 'bigint'::regtype::OID, -1, TRUE, '0'),
              (
                  'last_heartbeat',
                  'timestamp with time zone'::regtype::OID,
                  -1,
                  FALSE,
                  NULL::TEXT
              ),
              ('failure_count', 'integer'::regtype::OID, -1, TRUE, '0'),
              (
                  'retry_after',
                  'timestamp with time zone'::regtype::OID,
                  -1,
                  FALSE,
                  NULL::TEXT
              ),
              (
                  'unclaimed_since',
                  'timestamp with time zone'::regtype::OID,
                  -1,
                  FALSE,
                  NULL::TEXT
              ),
              ('status_reason', 'text'::regtype::OID, -1, FALSE, NULL::TEXT),
              (
                  'status_reason_detail',
                  'text'::regtype::OID,
                  -1,
                  FALSE,
                  NULL::TEXT
              ),
              (
                  'status_reason_updated_at',
                  'timestamp with time zone'::regtype::OID,
                  -1,
                  FALSE,
                  NULL::TEXT
              ),
              ('audit_revision', 'bigint'::regtype::OID, -1, TRUE, '0'),
              (
                  'membership_revision',
                  'bigint'::regtype::OID,
                  -1,
                  TRUE,
                  '0'
              ),
              (
                  'created_at',
                  'timestamp with time zone'::regtype::OID,
                  -1,
                  TRUE,
                  'now()'
              ),
              (
                  'updated_at',
                  'timestamp with time zone'::regtype::OID,
                  -1,
                  TRUE,
                  'now()'
              )
          ) AS required(
              column_name,
              type_oid,
              type_modifier,
              is_not_null,
              default_expression
          )
    LOOP
        SELECT
            a.atttypid AS type_oid,
            a.atttypmod AS type_modifier,
            a.attnotnull AS is_not_null,
            pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_expression
          INTO actual_column
          FROM pg_catalog.pg_attribute AS a
          LEFT JOIN pg_catalog.pg_attrdef AS d
            ON d.adrelid = a.attrelid
           AND d.adnum = a.attnum
         WHERE a.attrelid = lease_table_oid
           AND a.attname = expected_column.column_name
           AND a.attnum > 0
           AND NOT a.attisdropped;

        IF NOT FOUND
           OR actual_column.type_oid IS DISTINCT FROM expected_column.type_oid
           OR actual_column.type_modifier IS DISTINCT FROM
              expected_column.type_modifier
           OR actual_column.is_not_null IS DISTINCT FROM
              expected_column.is_not_null
           OR actual_column.default_expression IS DISTINCT FROM
              expected_column.default_expression THEN
            RAISE EXCEPTION
                'Lease column % has the wrong schema contract',
                expected_column.column_name;
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_attribute AS a
         WHERE a.attrelid = lease_table_oid
           AND a.attnum > 0
           AND NOT a.attisdropped
           AND (
               a.attname ~ 'cursor'
               OR a.attname ~ 'bookmark'
               OR a.attname ~ 'processed_filename'
           )
    ) THEN
        RAISE EXCEPTION
            'public.ingestion_leases must not contain a durable cursor, bookmark, or processed-filename field';
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

    SELECT c.oid
      INTO source_types_oid
      FROM pg_catalog.pg_class AS c
      JOIN pg_catalog.pg_namespace AS n
        ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = 'source_types'
       AND c.relkind = 'r';

    SELECT a.attnum
      INTO source_slug_attnum
      FROM pg_catalog.pg_attribute AS a
     WHERE a.attrelid = source_types_oid
       AND a.attname = 'slug'
       AND a.attnum > 0
       AND NOT a.attisdropped;

    SELECT c.contype, c.conkey, c.convalidated
      INTO actual_constraint
      FROM pg_catalog.pg_constraint AS c
     WHERE c.conrelid = lease_table_oid
       AND c.conname = 'ingestion_leases_pkey';

    IF NOT FOUND
       OR actual_constraint.contype <> 'p'
       OR actual_constraint.conkey IS DISTINCT FROM
          ARRAY[source_type_attnum, lease_key_attnum]::SMALLINT[]
       OR NOT actual_constraint.convalidated THEN
        RAISE EXCEPTION 'Lease composite primary key contract failed';
    END IF;

    SELECT
        c.contype,
        c.conkey,
        c.confrelid,
        c.confkey,
        c.convalidated
      INTO actual_constraint
      FROM pg_catalog.pg_constraint AS c
     WHERE c.conrelid = lease_table_oid
       AND c.conname = 'ingestion_leases_source_type_fkey';

    IF NOT FOUND
       OR actual_constraint.contype <> 'f'
       OR actual_constraint.conkey IS DISTINCT FROM
          ARRAY[source_type_attnum]::SMALLINT[]
       OR actual_constraint.confrelid IS DISTINCT FROM source_types_oid
       OR actual_constraint.confkey IS DISTINCT FROM
          ARRAY[source_slug_attnum]::SMALLINT[]
       OR NOT actual_constraint.convalidated THEN
        RAISE EXCEPTION 'Lease source-type foreign key contract failed';
    END IF;

    SELECT st.slug
      INTO fixture_source_type
      FROM public.source_types AS st
     ORDER BY st.slug
     LIMIT 1;

    IF fixture_source_type IS NULL THEN
        RAISE EXCEPTION 'Phase 1 schema contract requires a source type';
    END IF;

    fixture_key := pg_catalog.format(
        'phase-1-lease-%s-%s',
        pg_catalog.pg_backend_pid(),
        pg_catalog.txid_current()
    );

    INSERT INTO public.ingestion_leases (source_type, lease_key)
    VALUES (fixture_source_type, fixture_key)
    RETURNING * INTO lease_row;

    IF lease_row.status <> 'deactivated'::public.feed_status
       OR lease_row.worker_id IS NOT NULL
       OR lease_row.fencing_token <> 0
       OR lease_row.last_heartbeat IS NOT NULL
       OR lease_row.failure_count <> 0
       OR lease_row.retry_after IS NOT NULL
       OR lease_row.unclaimed_since IS NOT NULL
       OR lease_row.status_reason IS NOT NULL
       OR lease_row.status_reason_detail IS NOT NULL
       OR lease_row.status_reason_updated_at IS NOT NULL
       OR lease_row.audit_revision <> 0
       OR lease_row.membership_revision <> 0
       OR lease_row.created_at IS NULL
       OR lease_row.updated_at IS NULL THEN
        RAISE EXCEPTION 'Minimal Lease defaults are not fail-closed';
    END IF;

    BEGIN
        INSERT INTO public.ingestion_leases (source_type, lease_key)
        VALUES (fixture_source_type, fixture_key);
        RAISE EXCEPTION 'Duplicate Lease identity was accepted';
    EXCEPTION
        WHEN SQLSTATE '23505' THEN NULL;
    END;

    BEGIN
        INSERT INTO public.ingestion_leases (source_type, lease_key)
        VALUES ('', fixture_key || '-empty-source');
        RAISE EXCEPTION 'Empty Lease source_type was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        INSERT INTO public.ingestion_leases (source_type, lease_key)
        VALUES (fixture_source_type, '');
        RAISE EXCEPTION 'Empty Lease lease_key was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            fencing_token
        ) VALUES (fixture_source_type, fixture_key || '-fence', -1);
        RAISE EXCEPTION 'Negative Lease fencing_token was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            failure_count
        ) VALUES (fixture_source_type, fixture_key || '-failure', -1);
        RAISE EXCEPTION 'Negative Lease failure_count was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            audit_revision
        ) VALUES (fixture_source_type, fixture_key || '-audit', -1);
        RAISE EXCEPTION 'Negative Lease audit_revision was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            membership_revision
        ) VALUES (fixture_source_type, fixture_key || '-membership', -1);
        RAISE EXCEPTION 'Negative Lease membership_revision was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            worker_id
        ) VALUES (
            fixture_source_type,
            fixture_key || '-owner-only',
            '00000000-0000-0000-0000-000000000001'::UUID
        );
        RAISE EXCEPTION 'Lease owner without heartbeat was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            last_heartbeat
        ) VALUES (
            fixture_source_type,
            fixture_key || '-heartbeat-only',
            NOW()
        );
        RAISE EXCEPTION 'Lease heartbeat without owner was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            status
        ) VALUES (
            fixture_source_type,
            fixture_key || '-active-ownerless',
            'active'::public.feed_status
        );
        RAISE EXCEPTION 'Ownerless active Lease was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            status_reason_detail
        ) VALUES (
            fixture_source_type,
            fixture_key || '-detail',
            pg_catalog.repeat('x', 2049)
        );
        RAISE EXCEPTION 'Oversized Lease status_reason_detail was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    INSERT INTO public.ingestion_leases (
        source_type,
        lease_key,
        status
    ) VALUES (
        fixture_source_type,
        fixture_key || '-failing-ownerless',
        'failing'::public.feed_status
    );

    INSERT INTO public.ingestion_leases (
        source_type,
        lease_key,
        status,
        worker_id,
        last_heartbeat
    ) VALUES (
        fixture_source_type,
        fixture_key || '-failing-owned',
        'failing'::public.feed_status,
        '00000000-0000-0000-0000-000000000002'::UUID,
        NOW()
    );
END
$contract$;

-- Guard catalog and ordinary-role behavior probes.
DO $guard_contract$
DECLARE
    lease_table_oid OID;
    guard_function_oid OID;
    source_type_attnum SMALLINT;
    lease_key_attnum SMALLINT;
    fencing_token_attnum SMALLINT;
    fixture_source_type TEXT;
    fixture_key TEXT;
    expected_trigger RECORD;
    actual_trigger RECORD;
BEGIN
    SELECT c.oid
      INTO lease_table_oid
      FROM pg_catalog.pg_class AS c
      JOIN pg_catalog.pg_namespace AS n
        ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = 'ingestion_leases'
       AND c.relkind = 'r';

    SELECT p.oid
      INTO guard_function_oid
      FROM pg_catalog.pg_proc AS p
      JOIN pg_catalog.pg_namespace AS n
        ON n.oid = p.pronamespace
      JOIN pg_catalog.pg_language AS l
        ON l.oid = p.prolang
     WHERE n.nspname = 'public'
       AND p.proname = 'guard_ingestion_lease_identity'
       AND p.pronargs = 0
       AND p.prorettype = 'trigger'::regtype
       AND l.lanname = 'plpgsql';

    IF lease_table_oid IS NULL OR guard_function_oid IS NULL THEN
        RAISE EXCEPTION 'Lease guard table/function catalog contract failed';
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
            t.tgnargs,
            t.tgattr::TEXT AS trigger_columns,
            t.tgqual
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
           OR actual_trigger.tgnargs <> 0
           OR actual_trigger.trigger_columns IS DISTINCT FROM
              expected_trigger.trigger_columns
           OR actual_trigger.tgqual IS NOT NULL THEN
            RAISE EXCEPTION
                'Lease guard trigger % failed the schema contract',
                expected_trigger.trigger_name;
        END IF;
    END LOOP;

    SELECT st.slug
      INTO fixture_source_type
      FROM public.source_types AS st
     ORDER BY st.slug
     LIMIT 1;

    fixture_key := pg_catalog.format(
        'phase-1-guard-%s-%s',
        pg_catalog.pg_backend_pid(),
        pg_catalog.txid_current()
    );

    INSERT INTO public.ingestion_leases (
        source_type,
        lease_key,
        fencing_token
    ) VALUES (fixture_source_type, fixture_key, 10);

    BEGIN
        DELETE FROM public.ingestion_leases
         WHERE source_type = fixture_source_type
           AND lease_key = fixture_key;
        RAISE EXCEPTION 'Lease DELETE guard did not return SQLSTATE 23514';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        TRUNCATE TABLE public.ingestion_leases;
        RAISE EXCEPTION 'Lease TRUNCATE guard did not return SQLSTATE 23514';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.ingestion_leases
           SET source_type = fixture_source_type || '-re-keyed'
         WHERE source_type = fixture_source_type
           AND lease_key = fixture_key;
        RAISE EXCEPTION 'Lease source re-key guard did not return SQLSTATE 23514';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.ingestion_leases
           SET lease_key = fixture_key || '-re-keyed'
         WHERE source_type = fixture_source_type
           AND lease_key = fixture_key;
        RAISE EXCEPTION 'Lease key re-key guard did not return SQLSTATE 23514';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.ingestion_leases
           SET fencing_token = 9
         WHERE source_type = fixture_source_type
           AND lease_key = fixture_key;
        RAISE EXCEPTION 'Lease fence regression guard did not return SQLSTATE 23514';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    IF NOT EXISTS (
        SELECT 1
          FROM public.ingestion_leases AS il
         WHERE il.source_type = fixture_source_type
           AND il.lease_key = fixture_key
           AND il.fencing_token = 10
    ) THEN
        RAISE EXCEPTION
            'Rejected Lease operations changed or hid the protected row';
    END IF;

    UPDATE public.ingestion_leases
       SET fencing_token = 10
     WHERE source_type = fixture_source_type
       AND lease_key = fixture_key;

    UPDATE public.ingestion_leases
       SET fencing_token = 11
     WHERE source_type = fixture_source_type
       AND lease_key = fixture_key;

    IF NOT EXISTS (
        SELECT 1
          FROM public.ingestion_leases AS il
         WHERE il.source_type = fixture_source_type
           AND il.lease_key = fixture_key
           AND il.fencing_token = 11
    ) THEN
        RAISE EXCEPTION
            'Equal or increasing Lease fencing-token update was rejected';
    END IF;
END
$guard_contract$;

SET LOCAL session_replication_role = replica;

-- ALWAYS triggers must still reject both protected row mutation and
-- destructive table mutation when ordinary triggers would be suppressed.
DO $replica_guard_contract$
DECLARE
    fixture_source_type TEXT;
    fixture_key TEXT;
BEGIN
    SELECT st.slug
      INTO fixture_source_type
      FROM public.source_types AS st
     ORDER BY st.slug
     LIMIT 1;

    fixture_key := pg_catalog.format(
        'phase-1-guard-%s-%s',
        pg_catalog.pg_backend_pid(),
        pg_catalog.txid_current()
    );

    BEGIN
        UPDATE public.ingestion_leases
           SET fencing_token = 10
         WHERE source_type = fixture_source_type
           AND lease_key = fixture_key;
        RAISE EXCEPTION
            'Replica-role fence guard did not return SQLSTATE 23514';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        TRUNCATE TABLE public.ingestion_leases;
        RAISE EXCEPTION
            'Replica-role TRUNCATE guard did not return SQLSTATE 23514';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    IF NOT EXISTS (
        SELECT 1
          FROM public.ingestion_leases AS il
         WHERE il.source_type = fixture_source_type
           AND il.lease_key = fixture_key
           AND il.fencing_token = 11
    ) THEN
        RAISE EXCEPTION
            'Replica-role protected operations changed or hid the Lease row';
    END IF;
END
$replica_guard_contract$;

SET LOCAL session_replication_role = origin;

-- Broadcastify Calls membership catalog and tuple-state contract. Positive
-- and expected-failure fixtures remain inside the outer rolled-back
-- transaction.
DO $membership_contract$
DECLARE
    feed_properties_oid OID;
    actual_constraint RECORD;
    actual_constraint_count INTEGER;
    actual_definition TEXT;
    expected_definition TEXT;
    fixture_suffix TEXT;
    calls_legacy_feed_id UUID := pg_catalog.gen_random_uuid();
    non_calls_legacy_feed_id UUID := pg_catalog.gen_random_uuid();
    trunked_feed_id UUID := pg_catalog.gen_random_uuid();
    nontrunked_feed_id UUID := pg_catalog.gen_random_uuid();
    negative_feed_id UUID := pg_catalog.gen_random_uuid();
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        'pg_catalog, public',
        TRUE
    );

    SELECT c.oid
      INTO feed_properties_oid
      FROM pg_catalog.pg_class AS c
      JOIN pg_catalog.pg_namespace AS n
        ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relname = 'feed_properties'
       AND c.relkind = 'r';

    IF feed_properties_oid IS NULL THEN
        RAISE EXCEPTION
            'public.feed_properties is not an ordinary table';
    END IF;

    CREATE TEMPORARY TABLE phase_1_expected_membership_constraint (
        source_type TEXT,
        source_feed_id TEXT,
        bcfy_calls_sid TEXT,
        bcfy_calls_group_id TEXT,
        bcfy_calls_is_trunked BOOLEAN,
        CONSTRAINT phase_1_expected_membership_check
        CHECK (
            CASE
                WHEN bcfy_calls_sid IS NULL
                 AND bcfy_calls_group_id IS NULL
                 AND bcfy_calls_is_trunked IS NULL
                    THEN TRUE
                WHEN source_type <> 'bcfy_calls'
                    THEN FALSE
                WHEN bcfy_calls_is_trunked IS TRUE
                    THEN bcfy_calls_sid IS NOT NULL
                     AND bcfy_calls_group_id IS NOT NULL
                     AND bcfy_calls_sid ~ '^[0-9]+$'
                     AND bcfy_calls_group_id ~ '^[0-9]+$'
                     AND source_feed_id =
                         bcfy_calls_sid || '-' || bcfy_calls_group_id
                WHEN bcfy_calls_is_trunked IS FALSE
                    THEN bcfy_calls_sid IS NULL
                     AND bcfy_calls_group_id IS NULL
                ELSE FALSE
            END
        )
    ) ON COMMIT DROP;

    SELECT pg_catalog.pg_get_expr(c.conbin, c.conrelid, TRUE)
      INTO expected_definition
      FROM pg_catalog.pg_constraint AS c
     WHERE c.conrelid =
           'pg_temp.phase_1_expected_membership_constraint'::regclass
       AND c.conname = 'phase_1_expected_membership_check';

    SELECT pg_catalog.count(*)
      INTO actual_constraint_count
      FROM pg_catalog.pg_constraint AS c
     WHERE c.conrelid = feed_properties_oid
       AND c.conname = 'feed_properties_bcfy_calls_membership_check';

    SELECT
        c.contype,
        c.condeferrable,
        c.condeferred,
        c.convalidated,
        c.conislocal,
        c.coninhcount,
        c.connoinherit,
        pg_catalog.pg_get_expr(c.conbin, c.conrelid, TRUE) AS definition
      INTO actual_constraint
      FROM pg_catalog.pg_constraint AS c
     WHERE c.conrelid = feed_properties_oid
       AND c.conname = 'feed_properties_bcfy_calls_membership_check';

    actual_definition := actual_constraint.definition;

    DROP TABLE phase_1_expected_membership_constraint;

    IF actual_constraint_count <> 1
       OR actual_constraint.contype IS DISTINCT FROM 'c'::"char"
       OR actual_constraint.condeferrable
       OR actual_constraint.condeferred
       OR NOT actual_constraint.convalidated
       OR NOT actual_constraint.conislocal
       OR actual_constraint.coninhcount <> 0
       OR actual_constraint.connoinherit
       OR actual_definition IS DISTINCT FROM expected_definition THEN
        RAISE EXCEPTION USING
            MESSAGE =
                'Broadcastify Calls membership constraint contract failed',
            DETAIL = pg_catalog.format(
                'count=%s validated=%s definition=%s',
                actual_constraint_count,
                COALESCE(
                    actual_constraint.convalidated::TEXT,
                    'NULL'
                ),
                COALESCE(actual_definition, 'NULL')
            );
    END IF;

    fixture_suffix := pg_catalog.format(
        '%s-%s',
        pg_catalog.pg_backend_pid(),
        pg_catalog.txid_current()
    );

    INSERT INTO public.feeds (id, name, source_type)
    VALUES
        (
            calls_legacy_feed_id,
            'phase-1-membership-calls-legacy-' || fixture_suffix,
            'bcfy_calls'
        ),
        (
            non_calls_legacy_feed_id,
            'phase-1-membership-noncalls-legacy-' || fixture_suffix,
            'bcfy_feeds'
        ),
        (
            trunked_feed_id,
            'phase-1-membership-trunked-' || fixture_suffix,
            'bcfy_calls'
        ),
        (
            nontrunked_feed_id,
            'phase-1-membership-nontrunked-' || fixture_suffix,
            'bcfy_calls'
        ),
        (
            negative_feed_id,
            'phase-1-membership-negative-' || fixture_suffix,
            'bcfy_calls'
        );

    -- Omitted membership is the valid legacy state for every source.
    INSERT INTO public.feed_properties (
        feed_id,
        source_feed_id,
        source_type
    ) VALUES
        (
            calls_legacy_feed_id,
            'phase-1-calls-legacy-' || fixture_suffix,
            'bcfy_calls'
        ),
        (
            non_calls_legacy_feed_id,
            'phase-1-noncalls-legacy-' || fixture_suffix,
            'bcfy_feeds'
        );

    INSERT INTO public.feed_properties (
        feed_id,
        source_feed_id,
        source_type,
        bcfy_calls_sid,
        bcfy_calls_group_id,
        bcfy_calls_is_trunked
    ) VALUES (
        trunked_feed_id,
        '001-002',
        'bcfy_calls',
        '001',
        '002',
        TRUE
    );

    INSERT INTO public.feed_properties (
        feed_id,
        source_feed_id,
        source_type,
        bcfy_calls_sid,
        bcfy_calls_group_id,
        bcfy_calls_is_trunked
    ) VALUES (
        nontrunked_feed_id,
        'phase-1-nontrunked-' || fixture_suffix,
        'bcfy_calls',
        NULL,
        NULL,
        FALSE
    );

    INSERT INTO public.feed_properties (
        feed_id,
        source_feed_id,
        source_type
    ) VALUES (
        negative_feed_id,
        'phase-1-negative-' || fixture_suffix,
        'bcfy_calls'
    );

    IF NOT EXISTS (
        SELECT 1
          FROM public.feed_properties AS fp
         WHERE fp.feed_id = trunked_feed_id
           AND fp.source_feed_id = '001-002'
           AND fp.bcfy_calls_sid = '001'
           AND fp.bcfy_calls_group_id = '002'
           AND fp.bcfy_calls_is_trunked IS TRUE
    ) OR NOT EXISTS (
        SELECT 1
          FROM public.feed_properties AS fp
         WHERE fp.feed_id = nontrunked_feed_id
           AND fp.bcfy_calls_sid IS NULL
           AND fp.bcfy_calls_group_id IS NULL
           AND fp.bcfy_calls_is_trunked IS FALSE
    ) THEN
        RAISE EXCEPTION
            'Positive Broadcastify Calls membership states were not retained';
    END IF;

    -- Every nonempty proper subset of the trunked tuple is invalid.
    BEGIN
        UPDATE public.feed_properties
           SET bcfy_calls_sid = '101'
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'SID-only membership tuple was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET bcfy_calls_group_id = '202'
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'group-only membership tuple was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET bcfy_calls_is_trunked = TRUE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'trunked-flag-only membership tuple was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET source_feed_id = '101-202',
               bcfy_calls_sid = '101',
               bcfy_calls_group_id = '202'
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'SID/group tuple without trunked flag was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET bcfy_calls_sid = '101',
               bcfy_calls_is_trunked = TRUE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'SID/trunked tuple without group was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET bcfy_calls_group_id = '202',
               bcfy_calls_is_trunked = TRUE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'group/trunked tuple without SID was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    -- A non-Calls source must remain in the all-null state.
    BEGIN
        UPDATE public.feed_properties
           SET source_feed_id = '301-401',
               bcfy_calls_sid = '301',
               bcfy_calls_group_id = '401',
               bcfy_calls_is_trunked = TRUE
         WHERE feed_id = non_calls_legacy_feed_id;
        RAISE EXCEPTION 'Populated non-Calls membership was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    -- Trunked identifiers must be nonempty ASCII numeric text. Keep source
    -- identity equal in these probes so the regex is the rejected invariant.
    BEGIN
        UPDATE public.feed_properties
           SET source_feed_id = '-202',
               bcfy_calls_sid = '',
               bcfy_calls_group_id = '202',
               bcfy_calls_is_trunked = TRUE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'Empty Broadcastify Calls SID was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET source_feed_id = '101-',
               bcfy_calls_sid = '101',
               bcfy_calls_group_id = '',
               bcfy_calls_is_trunked = TRUE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'Empty Broadcastify Calls group ID was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET source_feed_id = '١-202',
               bcfy_calls_sid = '١',
               bcfy_calls_group_id = '202',
               bcfy_calls_is_trunked = TRUE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'Non-ASCII Broadcastify Calls SID was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET source_feed_id = '101-٢',
               bcfy_calls_sid = '101',
               bcfy_calls_group_id = '٢',
               bcfy_calls_is_trunked = TRUE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'Non-ASCII Broadcastify Calls group ID was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET source_feed_id = '10x-202',
               bcfy_calls_sid = '10x',
               bcfy_calls_group_id = '202',
               bcfy_calls_is_trunked = TRUE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'Nonnumeric Broadcastify Calls SID was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET source_feed_id = '101-20x',
               bcfy_calls_sid = '101',
               bcfy_calls_group_id = '20x',
               bcfy_calls_is_trunked = TRUE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'Nonnumeric Broadcastify Calls group ID was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET source_feed_id = '101-999',
               bcfy_calls_sid = '101',
               bcfy_calls_group_id = '202',
               bcfy_calls_is_trunked = TRUE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'Mismatched Broadcastify Calls identity was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET bcfy_calls_sid = '101',
               bcfy_calls_is_trunked = FALSE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'Nontrunked membership with SID was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;

    BEGIN
        UPDATE public.feed_properties
           SET bcfy_calls_group_id = '202',
               bcfy_calls_is_trunked = FALSE
         WHERE feed_id = negative_feed_id;
        RAISE EXCEPTION 'Nontrunked membership with group ID was accepted';
    EXCEPTION
        WHEN SQLSTATE '23514' THEN NULL;
    END;
END
$membership_contract$;

-- Final OID-anchored membership-index shape and health contract. This
-- intentionally repeats the production postflight predicate.
DO $membership_index_contract$
DECLARE
    target_table_oid OID;
    named_relation_count INTEGER;
    actual_index RECORD;
    actual_definition TEXT;
    actual_definition_raw TEXT;
    actual_predicate TEXT;
    expected_definition CONSTANT TEXT :=
        'createindexidx_feed_properties_bcfy_calls_membershiponpublic.' ||
        'feed_propertiesusingbtreebcfy_calls_sid,' ||
        'bcfy_calls_group_id,feed_idwheresource_type=' ||
        '''bcfy_calls''::textandbcfy_calls_is_trunkedistrue';
    expected_predicate CONSTANT TEXT :=
        'source_type=''bcfy_calls''::textand' ||
        'bcfy_calls_is_trunkedistrue';
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        'pg_catalog, public',
        TRUE
    );

    SELECT c.oid
      INTO target_table_oid
      FROM pg_catalog.pg_class AS c
     WHERE c.relnamespace = 'public'::regnamespace
       AND c.relname = 'feed_properties'
       AND c.relkind = 'r';

    IF target_table_oid IS NULL THEN
        RAISE EXCEPTION
            'public.feed_properties is not an ordinary table';
    END IF;

    SELECT pg_catalog.count(*)
      INTO named_relation_count
      FROM pg_catalog.pg_class AS c
     WHERE c.relnamespace = 'public'::regnamespace
       AND c.relname = 'idx_feed_properties_bcfy_calls_membership';

    IF named_relation_count <> 1 THEN
        RAISE EXCEPTION USING
            MESSAGE =
                'Missing or duplicated membership-index relation in public',
            DETAIL = pg_catalog.format(
                'name=idx_feed_properties_bcfy_calls_membership count=%s',
                named_relation_count
            ),
            HINT =
                'Inspect the same-name objects. Then run standalone ' ||
                'DROP INDEX CONCURRENTLY public.idx_feed_properties_bcfy_calls_membership; ' ||
                'and reapply ' ||
                'the ordered migrations.';
    END IF;

    WITH named_relation AS (
        SELECT
            c.oid AS named_relation_oid,
            c.relkind AS relation_kind,
            c.reloptions AS index_reloptions,
            c.relam
          FROM pg_catalog.pg_class AS c
         WHERE c.relnamespace = 'public'::regnamespace
           AND c.relname =
               'idx_feed_properties_bcfy_calls_membership'
    ),
    catalog_rows AS (
        SELECT
            nr.named_relation_oid,
            nr.relation_kind,
            nr.index_reloptions,
            i.indexrelid,
            i.indrelid AS indexed_table_oid,
            i.indisunique,
            i.indisprimary,
            i.indisexclusion,
            i.indnkeyatts,
            i.indnatts,
            i.indisvalid,
            i.indisready,
            i.indislive,
            i.indexprs::TEXT AS index_expressions,
            table_namespace.nspname AS indexed_table_schema,
            indexed_table.relname AS indexed_table_name,
            access_method.amname AS access_method_name,
            key_entry.position AS key_position,
            key_attribute.attname AS key_name,
            key_attribute.attcollation AS source_collation_oid,
            opclass_entry.position AS opclass_position,
            opclass.opcname AS opclass_name,
            opclass_namespace.nspname AS opclass_schema,
            collation_entry.position AS collation_position,
            collation_entry.collation_oid AS index_collation_oid,
            index_collation.collname AS index_collation_name,
            option_entry.position AS option_position,
            option_entry.option_value,
            pg_catalog.pg_get_expr(
                i.indpred,
                i.indrelid,
                TRUE
            ) AS index_predicate,
            pg_catalog.pg_get_indexdef(i.indexrelid) AS index_definition
          FROM named_relation AS nr
          LEFT JOIN pg_catalog.pg_index AS i
            ON i.indexrelid = nr.named_relation_oid
          LEFT JOIN pg_catalog.pg_class AS indexed_table
            ON indexed_table.oid = i.indrelid
          LEFT JOIN pg_catalog.pg_namespace AS table_namespace
            ON table_namespace.oid = indexed_table.relnamespace
          LEFT JOIN pg_catalog.pg_am AS access_method
            ON access_method.oid = nr.relam
          LEFT JOIN LATERAL
            pg_catalog.unnest(i.indkey::SMALLINT[])
            WITH ORDINALITY AS key_entry(attnum, position)
            ON TRUE
          LEFT JOIN pg_catalog.pg_attribute AS key_attribute
            ON key_attribute.attrelid = i.indrelid
           AND key_attribute.attnum = key_entry.attnum
           AND key_attribute.attnum > 0
           AND NOT key_attribute.attisdropped
          LEFT JOIN LATERAL
            pg_catalog.unnest(i.indclass::OID[])
            WITH ORDINALITY AS opclass_entry(opclass_oid, position)
            ON opclass_entry.position = key_entry.position
          LEFT JOIN pg_catalog.pg_opclass AS opclass
            ON opclass.oid = opclass_entry.opclass_oid
          LEFT JOIN pg_catalog.pg_namespace AS opclass_namespace
            ON opclass_namespace.oid = opclass.opcnamespace
          LEFT JOIN LATERAL
            pg_catalog.unnest(i.indcollation::OID[])
            WITH ORDINALITY AS collation_entry(
                collation_oid,
                position
            )
            ON collation_entry.position = key_entry.position
          LEFT JOIN pg_catalog.pg_collation AS index_collation
            ON index_collation.oid = collation_entry.collation_oid
          LEFT JOIN LATERAL
            pg_catalog.unnest(i.indoption::SMALLINT[])
            WITH ORDINALITY AS option_entry(option_value, position)
            ON option_entry.position = key_entry.position
    )
    SELECT
        cr.named_relation_oid,
        cr.relation_kind,
        cr.index_reloptions,
        cr.indexrelid,
        cr.indexed_table_oid,
        cr.indisunique,
        cr.indisprimary,
        cr.indisexclusion,
        cr.indnkeyatts,
        cr.indnatts,
        cr.indisvalid,
        cr.indisready,
        cr.indislive,
        cr.index_expressions,
        cr.indexed_table_schema,
        cr.indexed_table_name,
        cr.access_method_name,
        COALESCE(
            pg_catalog.array_agg(
                cr.key_name ORDER BY cr.key_position
            ) FILTER (WHERE cr.key_position IS NOT NULL),
            ARRAY[]::TEXT[]
        ) AS key_names,
        COALESCE(
            pg_catalog.array_agg(
                cr.opclass_name ORDER BY cr.opclass_position
            ) FILTER (WHERE cr.opclass_position IS NOT NULL),
            ARRAY[]::TEXT[]
        ) AS opclass_names,
        COALESCE(
            pg_catalog.array_agg(
                cr.opclass_schema ORDER BY cr.opclass_position
            ) FILTER (WHERE cr.opclass_position IS NOT NULL),
            ARRAY[]::TEXT[]
        ) AS opclass_schemas,
        COALESCE(
            pg_catalog.array_agg(
                cr.index_collation_oid ORDER BY cr.collation_position
            ) FILTER (WHERE cr.collation_position IS NOT NULL),
            ARRAY[]::OID[]
        ) AS index_collation_oids,
        COALESCE(
            pg_catalog.array_agg(
                cr.source_collation_oid ORDER BY cr.key_position
            ) FILTER (WHERE cr.key_position IS NOT NULL),
            ARRAY[]::OID[]
        ) AS source_collation_oids,
        COALESCE(
            pg_catalog.array_agg(
                cr.option_value ORDER BY cr.option_position
            ) FILTER (WHERE cr.option_position IS NOT NULL),
            ARRAY[]::SMALLINT[]
        ) AS option_values,
        cr.index_predicate,
        cr.index_definition
      INTO actual_index
      FROM catalog_rows AS cr
     GROUP BY
        cr.named_relation_oid,
        cr.relation_kind,
        cr.index_reloptions,
        cr.indexrelid,
        cr.indexed_table_oid,
        cr.indisunique,
        cr.indisprimary,
        cr.indisexclusion,
        cr.indnkeyatts,
        cr.indnatts,
        cr.indisvalid,
        cr.indisready,
        cr.indislive,
        cr.index_expressions,
        cr.indexed_table_schema,
        cr.indexed_table_name,
        cr.access_method_name,
        cr.index_predicate,
        cr.index_definition;

    actual_predicate := pg_catalog.lower(
        pg_catalog.regexp_replace(
            actual_index.index_predicate,
            '[[:space:]()]',
            '',
            'g'
        )
    );
    actual_definition_raw := actual_index.index_definition;
    actual_definition := pg_catalog.lower(
        pg_catalog.regexp_replace(
            pg_catalog.regexp_replace(
                actual_definition_raw,
                ' ON ([^ ]+\.)?feed_properties ',
                ' ON public.feed_properties '
            ),
            '[[:space:]()]',
            '',
            'g'
        )
    );

    IF actual_index.named_relation_oid IS DISTINCT FROM
          actual_index.indexrelid
       OR actual_index.relation_kind IS DISTINCT FROM 'i'::"char"
       OR actual_index.indexed_table_oid IS DISTINCT FROM target_table_oid
       OR actual_index.indexed_table_schema IS DISTINCT FROM 'public'
       OR actual_index.indexed_table_name IS DISTINCT FROM 'feed_properties'
       OR actual_index.access_method_name IS DISTINCT FROM 'btree'
       OR actual_index.indisunique IS DISTINCT FROM FALSE
       OR actual_index.indisprimary IS DISTINCT FROM FALSE
       OR actual_index.indisexclusion IS DISTINCT FROM FALSE
       OR actual_index.indnkeyatts IS DISTINCT FROM 3
       OR actual_index.indnatts IS DISTINCT FROM 3
       OR actual_index.key_names IS DISTINCT FROM
          ARRAY[
              'bcfy_calls_sid',
              'bcfy_calls_group_id',
              'feed_id'
          ]::TEXT[]
       OR actual_index.opclass_names IS DISTINCT FROM
          ARRAY['text_ops', 'text_ops', 'uuid_ops']::TEXT[]
       OR actual_index.opclass_schemas IS DISTINCT FROM
          ARRAY['pg_catalog', 'pg_catalog', 'pg_catalog']::TEXT[]
       OR (actual_index.index_collation_oids)[1] IS DISTINCT FROM
          (actual_index.source_collation_oids)[1]
       OR (actual_index.index_collation_oids)[2] IS DISTINCT FROM
          (actual_index.source_collation_oids)[2]
       OR (actual_index.source_collation_oids)[3] IS DISTINCT FROM 0::OID
       OR (actual_index.index_collation_oids)[3] IS DISTINCT FROM 0::OID
       OR actual_index.option_values IS DISTINCT FROM
          ARRAY[0, 0, 0]::SMALLINT[]
       OR actual_index.index_expressions IS NOT NULL
       OR actual_index.index_reloptions IS NOT NULL
       OR actual_predicate IS DISTINCT FROM expected_predicate
       OR actual_definition IS DISTINCT FROM expected_definition
       OR actual_index.indisvalid IS DISTINCT FROM TRUE
       OR actual_index.indisready IS DISTINCT FROM TRUE
       OR actual_index.indislive IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION USING
            MESSAGE =
                'Unexpected or unhealthy membership-index relation in public',
            DETAIL = pg_catalog.format(
                'relkind=%s table=%s.%s method=%s unique=%s primary=%s ' ||
                'exclusion=%s nkeys=%s natts=%s keys=%s opclasses=%s ' ||
                'opclass_schemas=%s index_collations=%s ' ||
                'source_collations=%s options=%s reloptions=%s ' ||
                'predicate=%s definition=%s valid=%s ready=%s live=%s',
                COALESCE(actual_index.relation_kind::TEXT, 'NULL'),
                COALESCE(actual_index.indexed_table_schema, 'NULL'),
                COALESCE(actual_index.indexed_table_name, 'NULL'),
                COALESCE(actual_index.access_method_name, 'NULL'),
                COALESCE(actual_index.indisunique::TEXT, 'NULL'),
                COALESCE(actual_index.indisprimary::TEXT, 'NULL'),
                COALESCE(actual_index.indisexclusion::TEXT, 'NULL'),
                COALESCE(actual_index.indnkeyatts::TEXT, 'NULL'),
                COALESCE(actual_index.indnatts::TEXT, 'NULL'),
                COALESCE(actual_index.key_names::TEXT, 'NULL'),
                COALESCE(actual_index.opclass_names::TEXT, 'NULL'),
                COALESCE(actual_index.opclass_schemas::TEXT, 'NULL'),
                COALESCE(actual_index.index_collation_oids::TEXT, 'NULL'),
                COALESCE(actual_index.source_collation_oids::TEXT, 'NULL'),
                COALESCE(actual_index.option_values::TEXT, 'NULL'),
                COALESCE(actual_index.index_reloptions::TEXT, 'NULL'),
                COALESCE(actual_index.index_predicate, 'NULL'),
                COALESCE(actual_definition_raw, 'NULL'),
                COALESCE(actual_index.indisvalid::TEXT, 'NULL'),
                COALESCE(actual_index.indisready::TEXT, 'NULL'),
                COALESCE(actual_index.indislive::TEXT, 'NULL')
            ),
            HINT =
                'Inspect the same-name object. Then run standalone ' ||
                'DROP INDEX CONCURRENTLY public.idx_feed_properties_bcfy_calls_membership; ' ||
                'and reapply ' ||
                'the ordered migrations.';
    END IF;
END
$membership_index_contract$;

ROLLBACK;
