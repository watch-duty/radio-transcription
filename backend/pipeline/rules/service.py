from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend.pipeline.common.rules.models import (
        ResolvedRuleGroup,
        Rule,
        RuleCreate,
        RuleGroup,
        RuleGroupCreate,
        RuleGroupUpdate,
        RuleUpdate,
    )
    from backend.pipeline.storage.rules_store import RulesStore


class BaseRulesService(ABC):
    """Abstract base class for Rules Service implementations."""

    # --- Rule Operations ---
    @abstractmethod
    async def create_rule(self, rule_in: RuleCreate) -> Rule:
        """Create a new transcription rule."""

    @abstractmethod
    async def get_rule(self, rule_id: str) -> Rule | None:
        """Fetch a specific transcription rule by ID."""

    @abstractmethod
    async def list_rules(self) -> list[Rule]:
        """List all transcription rules."""

    @abstractmethod
    async def update_rule(
        self, rule_id: str, rule_in: RuleUpdate
    ) -> Rule | None:
        """Update an existing transcription rule."""

    @abstractmethod
    async def delete_rule(self, rule_id: str) -> bool:
        """Delete a transcription rule."""

    # --- Rule Group Operations ---
    @abstractmethod
    async def create_rule_group(self, group_in: RuleGroupCreate) -> RuleGroup:
        """Create a new rule group."""

    @abstractmethod
    async def get_rule_group(self, group_id: str) -> RuleGroup | None:
        """Fetch a specific rule group by ID."""

    @abstractmethod
    async def list_rule_groups(self) -> list[RuleGroup]:
        """List all rule groups."""

    @abstractmethod
    async def update_rule_group(
        self, group_id: str, group_in: RuleGroupUpdate
    ) -> RuleGroup | None:
        """Update a rule group."""

    @abstractmethod
    async def delete_rule_group(self, group_id: str) -> bool:
        """Delete a rule group."""

    # --- Resolution ---
    @abstractmethod
    async def get_resolved_rule_group(
        self, group_id: str
    ) -> ResolvedRuleGroup | None:
        """Fetch a rule group and resolve its child rules."""

    @abstractmethod
    async def list_resolved_rule_groups(self) -> list[ResolvedRuleGroup]:
        """List all rule groups and resolve their child rules."""


class AlloyRulesService(BaseRulesService):
    """Implementation of the Rules Service using AlloyDB."""

    def __init__(self, store: RulesStore) -> None:
        self._store = store

    # --- Rule Operations ---
    async def create_rule(self, rule_in: RuleCreate) -> Rule:
        return await self._store.create_rule(rule_in)

    async def get_rule(self, rule_id: str) -> Rule | None:
        return await self._store.get_rule(rule_id)

    async def list_rules(self) -> list[Rule]:
        return await self._store.list_rules()

    async def update_rule(
        self, rule_id: str, rule_in: RuleUpdate
    ) -> Rule | None:
        return await self._store.update_rule(rule_id, rule_in)

    async def delete_rule(self, rule_id: str) -> bool:
        return await self._store.delete_rule(rule_id)

    # --- Rule Group Operations ---
    async def create_rule_group(self, group_in: RuleGroupCreate) -> RuleGroup:
        return await self._store.create_rule_group(group_in)

    async def get_rule_group(self, group_id: str) -> RuleGroup | None:
        return await self._store.get_rule_group(group_id)

    async def list_rule_groups(self) -> list[RuleGroup]:
        return await self._store.list_rule_groups()

    async def update_rule_group(
        self, group_id: str, group_in: RuleGroupUpdate
    ) -> RuleGroup | None:
        return await self._store.update_rule_group(group_id, group_in)

    async def delete_rule_group(self, group_id: str) -> bool:
        return await self._store.delete_rule_group(group_id)

    # --- Resolution ---
    async def get_resolved_rule_group(
        self, group_id: str
    ) -> ResolvedRuleGroup | None:
        return await self._store.get_resolved_rule_group(group_id)

    async def list_resolved_rule_groups(self) -> list[ResolvedRuleGroup]:
        return await self._store.list_resolved_rule_groups()
