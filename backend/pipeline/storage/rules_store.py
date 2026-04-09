from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

from backend.pipeline.common.rules.models import (
    ResolvedRuleGroup,
    Rule,
    RuleCreate,
    RuleGroup,
    RuleGroupCreate,
    RuleGroupUpdate,
    RuleUpdate,
)

if TYPE_CHECKING:
    import asyncpg

from . import rules_queries


class RulesStore:
    """
    Storage layer for transcription rules against AlloyDB.

    Provides atomic SQL operations for CRUD operations on rules and rule groups.
    Uses asyncpg pool-level methods for automatic connection management.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def _prepare_row(self, row: asyncpg.Record) -> dict:
        """Prepare an asyncpg record for Pydantic validation."""
        data = dict(row)
        if data.get("rule_id"):
            data["rule_id"] = str(data["rule_id"])

        if data.get("child_rule_ids"):
            data["child_rule_ids"] = [
                str(uid) for uid in data["child_rule_ids"]
            ]

        for field in ["scope", "conditions", "metadata"]:
            value = data.get(field)
            if value and isinstance(value, (str, bytes, bytearray)):
                data[field] = json.loads(value)
        return data

    # --- Rule Operations ---

    async def create_rule(self, rule_in: RuleCreate) -> Rule:
        """Create a new transcription rule."""
        conditions_json = json.dumps(rule_in.conditions.model_dump(mode="json"))
        created_by = rule_in.metadata.created_by
        if not created_by:
            msg = "created_by is mandatory for rule creation."
            raise ValueError(msg)

        row = await self._pool.fetchrow(
            rules_queries.CREATE_RULE_SQL,
            rule_in.rule_name,
            rule_in.description,
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

    async def update_rule(
        self, rule_id: str, rule_in: RuleUpdate
    ) -> Rule | None:
        """Update an existing transcription rule."""
        try:
            uid = uuid.UUID(rule_id)
        except ValueError:
            return None

        update_data = rule_in.model_dump(exclude_unset=True, mode="json")
        if not update_data:
            return await self.get_rule(rule_id)

        set_clauses = []
        values: list[Any] = [uid]
        arg_idx = 2

        for key, value in update_data.items():
            db_value = value
            if key == "conditions" and db_value is not None:
                db_value = json.dumps(db_value)

            set_clauses.append(f"{key} = ${arg_idx}")
            values.append(db_value)
            arg_idx += 1

        query = f"UPDATE rules SET {', '.join(set_clauses)}, updated_at = NOW() WHERE id = $1 RETURNING {rules_queries.RULE_COLUMNS_SQL}"  # noqa: S608
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

    # --- Rule Group Operations ---

    async def create_rule_group(self, group_in: RuleGroupCreate) -> RuleGroup:
        """Create a new rule group."""
        scope_json = json.dumps(group_in.scope.model_dump(mode="json"))
        created_by = group_in.metadata.created_by
        if not created_by:
            msg = "created_by is mandatory for rule creation."
            raise ValueError(msg)

        row = await self._pool.fetchrow(
            rules_queries.CREATE_GROUP_SQL,
            group_in.rule_name,
            group_in.description,
            group_in.is_active,
            scope_json,
            group_in.operator,
            group_in.child_rule_ids,
            created_by,
        )
        return RuleGroup.model_validate(self._prepare_row(row))

    async def get_rule_group(self, group_id: str) -> RuleGroup | None:
        """Fetch a specific rule group by ID."""
        try:
            uid = uuid.UUID(group_id)
        except ValueError:
            return None

        row = await self._pool.fetchrow(rules_queries.GET_GROUP_SQL, uid)
        if row is None:
            return None

        return RuleGroup.model_validate(self._prepare_row(row))

    async def list_rule_groups(self) -> list[RuleGroup]:
        """List all rule groups."""
        rows = await self._pool.fetch(rules_queries.LIST_GROUPS_SQL)
        return [
            RuleGroup.model_validate(self._prepare_row(row)) for row in rows
        ]

    async def update_rule_group(
        self, group_id: str, group_in: RuleGroupUpdate
    ) -> RuleGroup | None:
        """Update a rule group."""
        try:
            uid = uuid.UUID(group_id)
        except ValueError:
            return None

        update_data = group_in.model_dump(exclude_unset=True, mode="json")
        if not update_data:
            return await self.get_rule_group(group_id)

        set_clauses = []
        values: list[Any] = [uid]
        arg_idx = 2

        for key, value in update_data.items():
            db_value = value
            if key == "scope" and db_value is not None:
                db_value = json.dumps(db_value)

            set_clauses.append(f"{key} = ${arg_idx}")
            values.append(db_value)
            arg_idx += 1

        query = f"UPDATE rule_groups SET {', '.join(set_clauses)}, updated_at = NOW() WHERE id = $1 RETURNING {rules_queries.GROUP_COLUMNS_SQL}"  # noqa: S608
        row = await self._pool.fetchrow(query, *values)
        if row is None:
            return None
        return RuleGroup.model_validate(self._prepare_row(row))

    async def delete_rule_group(self, group_id: str) -> bool:
        """Delete a rule group."""
        try:
            uid = uuid.UUID(group_id)
        except ValueError:
            return False

        result = await self._pool.execute(rules_queries.DELETE_GROUP_SQL, uid)
        return result == "DELETE 1"

    # --- Resolution ---

    async def get_resolved_rule_group(
        self, group_id: str
    ) -> ResolvedRuleGroup | None:
        """Fetch a rule group and resolve its child rules."""
        group = await self.get_rule_group(group_id)
        if group is None:
            return None

        # Fetch all child rules in a single batch query to avoid N+1 queries
        child_rules = []
        if group.child_rule_ids:
            uids = [uuid.UUID(cid) for cid in group.child_rule_ids]
            rows = await self._pool.fetch(
                rules_queries.GET_RULES_BY_IDS_SQL, uids
            )
            child_rules = [
                Rule.model_validate(self._prepare_row(row)) for row in rows
            ]

        return ResolvedRuleGroup(
            rule_id=group.rule_id,
            rule_name=group.rule_name,
            description=group.description,
            is_active=group.is_active,
            scope=group.scope,
            operator=group.operator,
            child_rule_ids=group.child_rule_ids,
            child_rules=child_rules,
            metadata=group.metadata,
        )

    async def list_resolved_rule_groups(self) -> list[ResolvedRuleGroup]:
        """List all rule groups and resolve their child rules."""
        groups = await self.list_rule_groups()

        # Collect all unique child rule IDs across all groups to avoid N+1 queries
        all_child_ids = set()
        for group in groups:
            all_child_ids.update(group.child_rule_ids)

        # Fetch all required rules in a single batch query
        rules_by_id = {}
        if all_child_ids:
            uids = [uuid.UUID(cid) for cid in all_child_ids]
            rows = await self._pool.fetch(
                rules_queries.GET_RULES_BY_IDS_SQL, uids
            )
            for row in rows:
                rule = Rule.model_validate(self._prepare_row(row))
                rules_by_id[rule.rule_id] = rule

        resolved_groups = []
        for group in groups:
            child_rules = []
            for child_id in group.child_rule_ids:
                if child_id in rules_by_id:
                    child_rules.append(rules_by_id[child_id])
            resolved_groups.append(
                ResolvedRuleGroup(
                    rule_id=group.rule_id,
                    rule_name=group.rule_name,
                    description=group.description,
                    is_active=group.is_active,
                    scope=group.scope,
                    operator=group.operator,
                    child_rule_ids=group.child_rule_ids,
                    child_rules=child_rules,
                    metadata=group.metadata,
                )
            )
        return resolved_groups
