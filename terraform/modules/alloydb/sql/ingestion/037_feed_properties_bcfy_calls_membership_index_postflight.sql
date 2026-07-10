-- Require the one exact healthy index after the concurrent build. Discovery
-- repeats the preflight's relation-name-first catalog path so absence, a race,
-- or any same-name drift fails closed.
DO $postflight$
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
$postflight$;
