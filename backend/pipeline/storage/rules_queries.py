"""SQL query constants for RulesStore."""

RULE_COLUMNS_SQL = """\
    id AS rule_id,
    rule_name,
    description,
    conditions,
    json_build_object(
        'created_at', created_at,
        'updated_at', updated_at,
        'created_by', created_by
    ) AS metadata
"""

CREATE_RULE_SQL = (
    "INSERT INTO rules (rule_name, description, conditions, created_by)\n"
    "VALUES ($1, $2, $3, $4)\n"
    "RETURNING\n"
) + RULE_COLUMNS_SQL

GET_RULE_SQL = ("SELECT\n") + RULE_COLUMNS_SQL + ("FROM rules\nWHERE id = $1\n")

LIST_RULES_SQL = ("SELECT\n") + RULE_COLUMNS_SQL + ("FROM rules\n")

GET_RULES_BY_IDS_SQL = (
    ("SELECT\n")
    + RULE_COLUMNS_SQL
    + ("FROM rules\nWHERE id = ANY($1::uuid[])\n")
)

RULE_RETURNING_SQL = "RETURNING\n" + RULE_COLUMNS_SQL

DELETE_RULE_SQL = """\
DELETE FROM rules
WHERE id = $1
"""

GROUP_COLUMNS_SQL = """\
    id AS rule_id,
    rule_name,
    description,
    is_active,
    scope,
    operator,
    child_rule_ids,
    json_build_object(
        'created_at', created_at,
        'updated_at', updated_at,
        'created_by', created_by
    ) AS metadata
"""

CREATE_GROUP_SQL = (
    "INSERT INTO rule_groups (rule_name, description, is_active, scope, operator, child_rule_ids, created_by)\n"
    "VALUES ($1, $2, $3, $4, $5, $6, $7)\n"
    "RETURNING\n"
) + GROUP_COLUMNS_SQL

GET_GROUP_SQL = (
    ("SELECT\n") + GROUP_COLUMNS_SQL + ("FROM rule_groups\nWHERE id = $1\n")
)

LIST_GROUPS_SQL = ("SELECT\n") + GROUP_COLUMNS_SQL + ("FROM rule_groups\n")

GROUP_RETURNING_SQL = "RETURNING\n" + GROUP_COLUMNS_SQL

DELETE_GROUP_SQL = """\
DELETE FROM rule_groups
WHERE id = $1
"""
