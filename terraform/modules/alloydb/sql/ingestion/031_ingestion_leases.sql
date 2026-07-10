-- Durable, generic ingestion Lease identities. Phase 1 intentionally creates
-- no rows and exposes no runtime mutation path.
CREATE TABLE IF NOT EXISTS public.ingestion_leases (
    source_type TEXT NOT NULL,
    lease_key TEXT NOT NULL,
    status public.feed_status NOT NULL
        DEFAULT 'deactivated'::public.feed_status,
    worker_id UUID NULL,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    last_heartbeat TIMESTAMPTZ NULL,
    failure_count INTEGER NOT NULL DEFAULT 0,
    retry_after TIMESTAMPTZ NULL,
    unclaimed_since TIMESTAMPTZ NULL,
    status_reason TEXT NULL,
    status_reason_detail TEXT NULL,
    status_reason_updated_at TIMESTAMPTZ NULL,
    audit_revision BIGINT NOT NULL DEFAULT 0,
    membership_revision BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ingestion_leases_pkey
        PRIMARY KEY (source_type, lease_key),
    CONSTRAINT ingestion_leases_source_type_fkey
        FOREIGN KEY (source_type)
        REFERENCES public.source_types (slug),
    CONSTRAINT ingestion_leases_identity_nonempty
        CHECK (source_type <> '' AND lease_key <> ''),
    CONSTRAINT ingestion_leases_fencing_token_nonnegative
        CHECK (fencing_token >= 0),
    CONSTRAINT ingestion_leases_failure_count_nonnegative
        CHECK (failure_count >= 0),
    CONSTRAINT ingestion_leases_audit_revision_nonnegative
        CHECK (audit_revision >= 0),
    CONSTRAINT ingestion_leases_membership_revision_nonnegative
        CHECK (membership_revision >= 0),
    CONSTRAINT ingestion_leases_owner_heartbeat_pair
        CHECK ((worker_id IS NULL) = (last_heartbeat IS NULL)),
    CONSTRAINT ingestion_leases_active_owned
        CHECK (
            status <> 'active'::public.feed_status
            OR worker_id IS NOT NULL
        ),
    CONSTRAINT ingestion_leases_status_reason_detail_length
        CHECK (
            status_reason_detail IS NULL
            OR char_length(status_reason_detail) <= 2048
        )
);

-- IF NOT EXISTS only checks the relation name. Resolve the public relation to
-- an OID and require every Phase 1 field and invariant so a wrong same-name
-- object fails closed. Unrelated future columns and constraints are allowed.
DO $migration$
DECLARE
    lease_table_oid OID;
    lease_table_kind "char";
    lease_table_persistence "char";
    lease_table_is_partition BOOLEAN;
    lease_inheritance_links BIGINT;
    source_types_oid OID;
    source_type_attnum SMALLINT;
    lease_key_attnum SMALLINT;
    source_slug_attnum SMALLINT;
    expected_column RECORD;
    actual_column RECORD;
    expected_constraint RECORD;
    actual_constraint RECORD;
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        'pg_catalog, public',
        TRUE
    );

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
        RAISE EXCEPTION USING
            MESSAGE =
                'public.ingestion_leases is not a permanent standalone ordinary table',
            DETAIL = pg_catalog.format(
                'resolved_oid=%s relkind=%s relpersistence=%s is_partition=%s inheritance_links=%s',
                COALESCE(lease_table_oid::TEXT, 'NULL'),
                COALESCE(lease_table_kind::TEXT, 'NULL'),
                COALESCE(lease_table_persistence::TEXT, 'NULL'),
                COALESCE(lease_table_is_partition::TEXT, 'NULL'),
                lease_inheritance_links
            );
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
            a.attidentity AS identity_kind,
            a.attgenerated AS generated_kind,
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

        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                MESSAGE = pg_catalog.format(
                    'public.ingestion_leases is missing required column %I',
                    expected_column.column_name
                );
        END IF;

        IF actual_column.type_oid IS DISTINCT FROM expected_column.type_oid
           OR actual_column.type_modifier IS DISTINCT FROM
              expected_column.type_modifier
           OR actual_column.is_not_null IS DISTINCT FROM
              expected_column.is_not_null
           OR actual_column.identity_kind <> ''
           OR actual_column.generated_kind <> ''
           OR actual_column.default_expression IS DISTINCT FROM
              expected_column.default_expression THEN
            RAISE EXCEPTION USING
                MESSAGE = pg_catalog.format(
                    'public.ingestion_leases column %I has the wrong definition',
                    expected_column.column_name
                ),
                DETAIL = pg_catalog.format(
                    'type=%s typmod=%s not_null=%s default=%s',
                    pg_catalog.format_type(
                        actual_column.type_oid,
                        actual_column.type_modifier
                    ),
                    actual_column.type_modifier,
                    actual_column.is_not_null,
                    COALESCE(
                        actual_column.default_expression,
                        'NULL'
                    )
                );
        END IF;
    END LOOP;

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

    SELECT
        c.contype,
        c.conkey,
        c.condeferrable,
        c.condeferred,
        c.convalidated,
        pg_catalog.pg_get_constraintdef(c.oid, TRUE) AS definition
      INTO actual_constraint
      FROM pg_catalog.pg_constraint AS c
     WHERE c.conrelid = lease_table_oid
       AND c.conname = 'ingestion_leases_pkey';

    IF NOT FOUND
       OR actual_constraint.contype <> 'p'
       OR actual_constraint.conkey IS DISTINCT FROM
          ARRAY[source_type_attnum, lease_key_attnum]::SMALLINT[]
       OR actual_constraint.condeferrable
       OR actual_constraint.condeferred
       OR NOT actual_constraint.convalidated
       OR actual_constraint.definition IS DISTINCT FROM
          'PRIMARY KEY (source_type, lease_key)' THEN
        RAISE EXCEPTION
            'public.ingestion_leases has the wrong ingestion_leases_pkey';
    END IF;

    SELECT
        c.contype,
        c.conkey,
        c.confrelid,
        c.confkey,
        c.confupdtype,
        c.confdeltype,
        c.confmatchtype,
        c.condeferrable,
        c.condeferred,
        c.convalidated,
        pg_catalog.pg_get_constraintdef(c.oid, TRUE) AS definition
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
       OR actual_constraint.confupdtype <> 'a'
       OR actual_constraint.confdeltype <> 'a'
       OR actual_constraint.confmatchtype <> 's'
       OR actual_constraint.condeferrable
       OR actual_constraint.condeferred
       OR NOT actual_constraint.convalidated
       OR actual_constraint.definition IS DISTINCT FROM
          'FOREIGN KEY (source_type) REFERENCES source_types(slug)' THEN
        RAISE EXCEPTION
            'public.ingestion_leases has the wrong ingestion_leases_source_type_fkey';
    END IF;

    FOR expected_constraint IN
        SELECT *
          FROM (VALUES
              (
                  'ingestion_leases_identity_nonempty',
                  $check$CHECK (source_type <> ''::text AND lease_key <> ''::text)$check$
              ),
              (
                  'ingestion_leases_fencing_token_nonnegative',
                  $check$CHECK (fencing_token >= 0)$check$
              ),
              (
                  'ingestion_leases_failure_count_nonnegative',
                  $check$CHECK (failure_count >= 0)$check$
              ),
              (
                  'ingestion_leases_audit_revision_nonnegative',
                  $check$CHECK (audit_revision >= 0)$check$
              ),
              (
                  'ingestion_leases_membership_revision_nonnegative',
                  $check$CHECK (membership_revision >= 0)$check$
              ),
              (
                  'ingestion_leases_owner_heartbeat_pair',
                  $check$CHECK ((worker_id IS NULL) = (last_heartbeat IS NULL))$check$
              ),
              (
                  'ingestion_leases_active_owned',
                  $check$CHECK (status <> 'active'::feed_status OR worker_id IS NOT NULL)$check$
              ),
              (
                  'ingestion_leases_status_reason_detail_length',
                  $check$CHECK (status_reason_detail IS NULL OR char_length(status_reason_detail) <= 2048)$check$
              )
          ) AS required(constraint_name, definition)
    LOOP
        SELECT
            c.contype,
            c.condeferrable,
            c.condeferred,
            c.convalidated,
            pg_catalog.replace(
                pg_catalog.pg_get_constraintdef(c.oid, TRUE),
                'public.feed_status',
                'feed_status'
            ) AS definition
          INTO actual_constraint
          FROM pg_catalog.pg_constraint AS c
         WHERE c.conrelid = lease_table_oid
           AND c.conname = expected_constraint.constraint_name;

        IF NOT FOUND
           OR actual_constraint.contype <> 'c'
           OR actual_constraint.condeferrable
           OR actual_constraint.condeferred
           OR NOT actual_constraint.convalidated
           OR actual_constraint.definition IS DISTINCT FROM
              expected_constraint.definition THEN
            RAISE EXCEPTION USING
                MESSAGE = pg_catalog.format(
                    'public.ingestion_leases constraint %I has the wrong definition',
                    expected_constraint.constraint_name
                ),
                DETAIL = COALESCE(
                    actual_constraint.definition,
                    'missing'
                );
        END IF;
    END LOOP;
END
$migration$;
