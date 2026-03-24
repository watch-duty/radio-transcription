from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

from backend.pipeline.common.rules.models import Rule, RuleCreate, RuleUpdate

if TYPE_CHECKING:
    import asyncpg

from . import rules_queries


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

        # Assign the mandatory created_by field
        created_by = rule_in.metadata.created_by
        if not created_by:
            msg = "created_by is mandatory for rule creation."
            raise ValueError(msg)

        row = await self._pool.fetchrow(
            rules_queries.CREATE_RULE_SQL,
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

        row = await self._pool.fetchrow(rules_queries.GET_RULE_SQL, uid)
        if row is None:
            return None

        return Rule.model_validate(self._prepare_row(row))

    async def list_rules(self) -> list[Rule]:
        """List all transcription rules."""
        rows = await self._pool.fetch(rules_queries.LIST_RULES_SQL)
        return [Rule.model_validate(self._prepare_row(row)) for row in rows]

    async def update_rule(self, rule_id: str, rule_in: RuleUpdate) -> Rule | None:
        """Partially update an existing transcription rule."""
        try:
            uid = uuid.UUID(rule_id)
        except ValueError:
            return None

        update_data = rule_in.model_dump(exclude_unset=True, mode="json")
        if not update_data:
            return await self.get_rule(rule_id)

        # Prepare fields for dynamic SQL
        set_clauses = []
        values: list[Any] = [uid]
        arg_idx = 2

        for key, value in update_data.items():
            db_value = value
            if key in {"scope", "conditions"} and db_value is not None:
                db_value = json.dumps(db_value)

            # Skip metadata for now as it contains immutable fields (created_at/by)
            # updated_at is handled automatically.
            if key == "metadata":
                continue

            set_clauses.append(f"{key} = ${arg_idx}")
            values.append(db_value)
            arg_idx += 1

        if not set_clauses:
            return await self.get_rule(rule_id)

        query_parts = [
            "UPDATE rules SET ",
            ", ".join(set_clauses),
            ", updated_at = NOW() WHERE id = $1 ",
            rules_queries.RULE_RETURNING_SQL,
        ]
        query = "".join(query_parts)

        row = await self._pool.fetchrow(query, *values)
        if row is None:
            return None

        return Rule.model_validate(self._prepare_row(row))

    async def delete_rule(self, rule_id: str) -> bool:
        """Delete a transcription rule."""
        try:
            uid = uuid.UUID(rule_id)
        except ValueError:
            return False

        result = await self._pool.execute(rules_queries.DELETE_RULE_SQL, uid)
        return result == "DELETE 1"
