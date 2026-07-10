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

ROLLBACK;
