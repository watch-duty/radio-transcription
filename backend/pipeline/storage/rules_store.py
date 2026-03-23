from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

from backend.pipeline.common.rules.models import Rule, RuleCreate, RuleUpdate

if TYPE_CHECKING:
    import asyncpg


_CREATE_RULE_SQL = """\
INSERT INTO rules (rule_name, description, is_active, scope, conditions, created_by)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING
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

_GET_RULE_SQL = """\
SELECT
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
FROM rules
WHERE id = $1
"""

_LIST_RULES_SQL = """\
SELECT
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
FROM rules
"""

_UPDATE_RULE_SQL = """\
UPDATE rules
SET rule_name = COALESCE($2, rule_name),
    description = COALESCE($3, description),
    is_active = COALESCE($4, is_active),
    scope = COALESCE($5, scope),
    conditions = COALESCE($6, conditions),
    updated_at = NOW()
WHERE id = $1
RETURNING
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

_DELETE_RULE_SQL = """\
DELETE FROM rules
WHERE id = $1
"""


class RulesStore:
    """
    Storage layer for transcription rules against AlloyDB.

    Provides atomic SQL operations for CRUD operations on rules.
    Uses asyncpg pool-level methods for automatic connection management.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def _prepare_row(self, row: asyncpg.Record) -> dict:
        """Prepare an asyncpg record for Pydantic validation."""
        data = dict(row)
        if data.get("rule_id"):
            data["rule_id"] = str(data["rule_id"])

        for field in ["scope", "conditions", "metadata"]:
            value = data.get(field)
            if value and isinstance(value, (str, bytes, bytearray)):
                data[field] = json.loads(value)
        return data

    async def create_rule(self, rule_in: RuleCreate) -> Rule:
        """Create a new transcription rule."""
        scope_json = json.dumps(rule_in.scope.model_dump(mode="json"))
        conditions_json = json.dumps(rule_in.conditions.model_dump(mode="json"))

        created_by = None
        if rule_in.metadata:
            created_by = rule_in.metadata.created_by

        row = await self._pool.fetchrow(
            _CREATE_RULE_SQL,
            rule_in.rule_name,
            rule_in.description,
            rule_in.is_active,
            scope_json,
            conditions_json,
            created_by,
        )
        return Rule.model_validate(self._prepare_row(row))

    async def get_rule(self, rule_id: str) -> Rule | None:
        """Fetch a specific transcription rule by ID."""
        try:
            uid = uuid.UUID(rule_id)
        except ValueError:
            return None

        row = await self._pool.fetchrow(_GET_RULE_SQL, uid)
        if row is None:
            return None

        return Rule.model_validate(self._prepare_row(row))

    async def list_rules(self) -> list[Rule]:
        """List all transcription rules."""
        rows = await self._pool.fetch(_LIST_RULES_SQL)
        return [Rule.model_validate(self._prepare_row(row)) for row in rows]

    async def update_rule(self, rule_id: str, rule_in: RuleUpdate) -> Rule | None:
        """Partially update an existing transcription rule."""
        try:
            uid = uuid.UUID(rule_id)
        except ValueError:
            return None

        update_data = rule_in.model_dump(exclude_unset=True, mode="json")

        # Prepare JSON fields if they are being updated
        scope_json = update_data.pop("scope", None)
        if scope_json is not None:
            scope_json = json.dumps(scope_json)

        conditions_json = update_data.pop("conditions", None)
        if conditions_json is not None:
            conditions_json = json.dumps(conditions_json)

        row = await self._pool.fetchrow(
            _UPDATE_RULE_SQL,
            uid,
            update_data.get("rule_name"),
            update_data.get("description"),
            update_data.get("is_active"),
            scope_json,
            conditions_json,
        )

        if row is None:
            return None

        return Rule.model_validate(self._prepare_row(row))

    async def delete_rule(self, rule_id: str) -> bool:
        """Delete a transcription rule."""
        try:
            uid = uuid.UUID(rule_id)
        except ValueError:
            return False

        result = await self._pool.execute(_DELETE_RULE_SQL, uid)
        return result == "DELETE 1"
