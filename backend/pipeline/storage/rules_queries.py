"""SQL query constants for RulesStore."""

RULE_COLUMNS_SQL = """\
    id AS rule_id,
    rule_name,
    description,
    is_active,
    scope,
    conditions,
    json_build_object(
        'created_at', created_at,
        'updated_at', updated_at,
        'created_by', created_by
    ) AS metadata
"""

CREATE_RULE_SQL = (
    "INSERT INTO rules (rule_name, description, is_active, scope, conditions, created_by)\n"
    "VALUES ($1, $2, $3, $4, $5, $6)\n"
    "RETURNING\n"
) + RULE_COLUMNS_SQL

GET_RULE_SQL = ("SELECT\n") + RULE_COLUMNS_SQL + ("FROM rules\nWHERE id = $1\n")

LIST_RULES_SQL = ("SELECT\n") + RULE_COLUMNS_SQL + ("FROM rules\n")

RULE_RETURNING_SQL = "RETURNING\n" + RULE_COLUMNS_SQL

DELETE_RULE_SQL = """\
DELETE FROM rules
WHERE id = $1
"""
