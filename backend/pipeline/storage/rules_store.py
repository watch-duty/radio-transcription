from __future__ import annotations

import base64
import datetime
import json
import uuid
from typing import TYPE_CHECKING, Any

from backend.pipeline.common.rules.models import (
    PaginatedRuleAuditEvents,
    Rule,
    RuleAuditEvent,
    RuleCreate,
    RuleUpdate,
)

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

        for field in ["scope", "conditions", "tags", "metadata"]:
            value = data.get(field)
            if value and isinstance(value, (str, bytes, bytearray)):
                data[field] = json.loads(value)
        return data

    async def create_rule(
        self, rule_in: RuleCreate, actor_id: str = ""
    ) -> Rule:
        """Create a new transcription rule."""
        scope_json = json.dumps(rule_in.scope.model_dump(mode="json"))
        conditions_json = json.dumps(rule_in.conditions.model_dump(mode="json"))
        tags_json = json.dumps(
            [tag.model_dump(mode="json") for tag in rule_in.tags]
        )

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
            tags_json,
            created_by,
            actor_id,
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

    async def list_rules(self, rule_ids: list[str] | None = None) -> list[Rule]:
        """List all transcription rules, optionally filtered by rule IDs."""
        if rule_ids:
            uids = []
            for rid in rule_ids:
                try:
                    uids.append(uuid.UUID(rid))
                except ValueError:
                    pass
            if not uids:
                return []
            rows = await self._pool.fetch(
                rules_queries.GET_RULES_BY_IDS_SQL, uids
            )
        else:
            rows = await self._pool.fetch(rules_queries.LIST_RULES_SQL)
        return [Rule.model_validate(self._prepare_row(row)) for row in rows]

    async def update_rule(
        self, rule_id: str, rule_in: RuleUpdate, actor_id: str = ""
    ) -> Rule | None:
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
            if key in {"scope", "conditions", "tags"} and db_value is not None:
                db_value = json.dumps(db_value)

            # Skip metadata for now as it contains immutable fields
            # (created_at/by)
            # updated_at is handled automatically.
            if key == "metadata":
                continue

            set_clauses.append(f"{key} = ${arg_idx}")
            values.append(db_value)
            arg_idx += 1

        if not set_clauses:
            return await self.get_rule(rule_id)

        values.append(actor_id)
        actor_idx = arg_idx

        query_parts = [
            "WITH updated_rule AS (\n",
            "  UPDATE rules SET ",
            ", ".join(set_clauses),
            "  , audit_revision = audit_revision + 1",
            "  , updated_at = NOW() WHERE id = $1\n",
            "  RETURNING *\n",
            "), audit_event AS (\n",
            "  INSERT INTO rule_audit_events (\n",
            "    rule_id, action, actor_id, rule_revision, before_values,\n",
            "    after_values\n",
            "  )\n",
            "  SELECT u.id, 'rule.updated', $"
            + str(actor_idx)
            + ", u.audit_revision,\n",
            "  row_to_json(r.*)::jsonb, row_to_json(u.*)::jsonb\n",
            "  FROM updated_rule u JOIN rules r ON r.id = u.id\n",
            ")\n",
            "SELECT\n",
            rules_queries.RULE_COLUMNS_SQL,
            "FROM updated_rule\n",
        ]
        query = "".join(query_parts)

        row = await self._pool.fetchrow(query, *values)
        if row is None:
            return None

        return Rule.model_validate(self._prepare_row(row))

    async def delete_rule(self, rule_id: str, actor_id: str = "") -> bool:
        """Delete a transcription rule."""
        try:
            uid = uuid.UUID(rule_id)
        except ValueError:
            return False

        row = await self._pool.fetchrow(
            rules_queries.DELETE_RULE_SQL, uid, actor_id
        )
        return row is not None

    async def get_rule_history(
        self, rule_id: str, limit: int = 50, next_token: str | None = None
    ) -> PaginatedRuleAuditEvents:
        """Fetch paginated audit history for a rule."""
        try:
            uid = uuid.UUID(rule_id)
        except ValueError:
            return PaginatedRuleAuditEvents(
                audit_events=[], next_token=None, total=0
            )

        cursor_time: datetime.datetime | None = None
        cursor_id: uuid.UUID | None = None

        if next_token:
            try:
                decoded = base64.b64decode(next_token.encode("utf-8")).decode(
                    "utf-8"
                )
                parts = decoded.split(",")
                if len(parts) == 2:
                    cursor_time = datetime.datetime.fromisoformat(parts[0])
                    cursor_id = uuid.UUID(parts[1])
            except (ValueError, UnicodeDecodeError):
                pass

        rows = await self._pool.fetch(
            rules_queries.GET_RULE_AUDIT_EVENTS_SQL,
            uid,
            cursor_time,
            cursor_id,
            limit,
        )

        total_row = await self._pool.fetchrow(
            rules_queries.COUNT_RULE_AUDIT_EVENTS_SQL, uid
        )
        total = total_row["count"] if total_row else 0

        events = []
        for row in rows:
            events.append(
                RuleAuditEvent(
                    id=str(row["id"]),
                    rule_id=str(row["rule_id"]),
                    action=row["action"],
                    actor_id=row["actor_id"],
                    occurred_at=row["occurred_at"],
                    rule_revision=row["rule_revision"],
                    before_values=(
                        json.loads(row["before_values"])
                        if isinstance(row["before_values"], str)
                        else row["before_values"]
                    ),
                    after_values=(
                        json.loads(row["after_values"])
                        if isinstance(row["after_values"], str)
                        else row["after_values"]
                    ),
                )
            )

        new_next_token = None
        if len(events) == limit:
            last_event = events[-1]
            token_str = f"{last_event.occurred_at.isoformat()},{last_event.id}"
            new_next_token = base64.b64encode(token_str.encode("utf-8")).decode(
                "utf-8"
            )

        return PaginatedRuleAuditEvents(
            audit_events=events,
            next_token=new_next_token,
            total=total,
        )
