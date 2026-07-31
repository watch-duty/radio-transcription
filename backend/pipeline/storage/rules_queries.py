"""SQL query constants for RulesStore."""

RULE_COLUMNS_SQL = """\
    id AS rule_id,
    rule_name,
    description,
    is_active,
    scope,
    conditions,
    COALESCE(tags, '[]'::jsonb) AS tags,
    json_build_object(
        'created_at', created_at,
        'updated_at', updated_at,
        'created_by', created_by
    ) AS metadata
"""

CREATE_RULE_SQL = (
    (  # noqa: S608
        "WITH inserted_rule AS (\n"
        "    INSERT INTO rules (\n"
        "        rule_name, description, is_active, scope, conditions, tags,\n"
        "        created_by, audit_revision\n"
        "    )\n"
        "    VALUES ($1, $2, $3, $4, $5, $6, $7, 1)\n"
        "    RETURNING *\n"
        "), audit_event AS (\n"
        "    INSERT INTO rule_audit_events (\n"
        "        rule_id, action, actor_id, rule_revision, before_values,\n"
        "        after_values\n"
        "    )\n"
        "    SELECT\n"
        "        id, 'rule.created', $8, audit_revision, '{}'::jsonb,\n"
        "        row_to_json(inserted_rule.*)::jsonb\n"
        "    FROM inserted_rule\n"
        ")\n"
        "SELECT\n"
    )
    + RULE_COLUMNS_SQL
    + "FROM inserted_rule\n"
)

GET_RULE_SQL = ("SELECT\n") + RULE_COLUMNS_SQL + ("FROM rules\nWHERE id = $1\n")

LIST_RULES_SQL = ("SELECT\n") + RULE_COLUMNS_SQL + ("FROM rules\n")

GET_RULES_BY_IDS_SQL = (
    ("SELECT\n") + RULE_COLUMNS_SQL + ("FROM rules\nWHERE id = ANY($1)\n")
)

DELETE_RULE_SQL = """\
WITH deleted_rule AS (
    DELETE FROM rules
    WHERE id = $1
    RETURNING *
), audit_event AS (
    INSERT INTO rule_audit_events (
        rule_id, action, actor_id, rule_revision, before_values, after_values
    )
    SELECT
        id, 'rule.deleted', $2, audit_revision + 1,
        row_to_json(deleted_rule.*)::jsonb, '{}'::jsonb
    FROM deleted_rule
)
SELECT id FROM deleted_rule
"""
