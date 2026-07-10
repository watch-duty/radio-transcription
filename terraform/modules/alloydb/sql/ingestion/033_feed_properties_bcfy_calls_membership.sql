-- Maintained Broadcastify Calls membership metadata. Existing rows remain in
-- the valid legacy state because all three columns are nullable with no
-- default and this migration performs no data rewrite.
ALTER TABLE public.feed_properties
    ADD COLUMN IF NOT EXISTS bcfy_calls_sid TEXT;

ALTER TABLE public.feed_properties
    ADD COLUMN IF NOT EXISTS bcfy_calls_group_id TEXT;

ALTER TABLE public.feed_properties
    ADD COLUMN IF NOT EXISTS bcfy_calls_is_trunked BOOLEAN;

-- Add the total tuple state machine only when the named constraint is absent.
-- A same-name wrong constraint is deliberately left in place for the exact
-- postcondition below to reject.
DO $migration$
DECLARE
    feed_properties_oid OID;
BEGIN
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

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_constraint AS c
         WHERE c.conrelid = feed_properties_oid
           AND c.conname =
               'feed_properties_bcfy_calls_membership_check'
    ) THEN
        ALTER TABLE public.feed_properties
            ADD CONSTRAINT feed_properties_bcfy_calls_membership_check
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
            ) NOT VALID;
    END IF;
END
$migration$;

-- IF NOT EXISTS only checks names. Require the exact schema-qualified column
-- shape and the exact parsed check expression. The expected temporary table
-- lets each supported server version canonicalize the expression itself.
DO $postcondition$
DECLARE
    feed_properties_oid OID;
    expected_column RECORD;
    actual_column RECORD;
    actual_constraint RECORD;
    actual_constraint_count INTEGER;
    actual_definition TEXT;
    expected_definition TEXT;
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

    FOR expected_column IN
        SELECT *
          FROM (VALUES
              ('bcfy_calls_sid', 'text'::regtype::OID),
              ('bcfy_calls_group_id', 'text'::regtype::OID),
              ('bcfy_calls_is_trunked', 'boolean'::regtype::OID)
          ) AS required(column_name, type_oid)
    LOOP
        SELECT
            a.atttypid AS type_oid,
            a.atttypmod AS type_modifier,
            a.attnotnull AS is_not_null,
            a.attidentity AS identity_kind,
            a.attgenerated AS generated_kind,
            pg_catalog.pg_get_expr(
                d.adbin,
                d.adrelid
            ) AS default_expression
          INTO actual_column
          FROM pg_catalog.pg_attribute AS a
          LEFT JOIN pg_catalog.pg_attrdef AS d
            ON d.adrelid = a.attrelid
           AND d.adnum = a.attnum
         WHERE a.attrelid = feed_properties_oid
           AND a.attname = expected_column.column_name
           AND a.attnum > 0
           AND NOT a.attisdropped;

        IF NOT FOUND
           OR actual_column.type_oid IS DISTINCT FROM
              expected_column.type_oid
           OR actual_column.type_modifier <> -1
           OR actual_column.is_not_null
           OR actual_column.identity_kind <> ''
           OR actual_column.generated_kind <> ''
           OR actual_column.default_expression IS NOT NULL THEN
            RAISE EXCEPTION USING
                MESSAGE = pg_catalog.format(
                    'public.feed_properties column %I has the wrong definition',
                    expected_column.column_name
                ),
                DETAIL = pg_catalog.format(
                    'type=%s typmod=%s not_null=%s default=%s',
                    COALESCE(
                        pg_catalog.format_type(
                            actual_column.type_oid,
                            actual_column.type_modifier
                        ),
                        'NULL'
                    ),
                    COALESCE(
                        actual_column.type_modifier::TEXT,
                        'NULL'
                    ),
                    COALESCE(
                        actual_column.is_not_null::TEXT,
                        'NULL'
                    ),
                    COALESCE(
                        actual_column.default_expression,
                        'NULL'
                    )
                );
        END IF;
    END LOOP;

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
       OR NOT actual_constraint.conislocal
       OR actual_constraint.coninhcount <> 0
       OR actual_constraint.connoinherit
       OR actual_definition IS DISTINCT FROM expected_definition THEN
        RAISE EXCEPTION USING
            MESSAGE =
                'public.feed_properties has the wrong membership check',
            DETAIL = pg_catalog.format(
                'count=%s definition=%s',
                actual_constraint_count,
                COALESCE(actual_definition, 'NULL')
            );
    END IF;
END
$postcondition$;
