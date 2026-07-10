-- Validate the already-enforced membership state machine without rewriting or
-- populating any existing row. Revalidating the final state is replay-safe.
ALTER TABLE public.feed_properties
    VALIDATE CONSTRAINT feed_properties_bcfy_calls_membership_check;

-- Require the exact expected check on the intended table and its final fully
-- validated state. The server canonicalizes both definitions so the check is
-- stable across PostgreSQL 15 and 16 deparser formatting.
DO $postcondition$
DECLARE
    feed_properties_oid OID;
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
                'public.feed_properties has the wrong validated membership check',
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
END
$postcondition$;
