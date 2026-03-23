from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from backend.pipeline.common.rules.models import Rule, RuleCreate, RuleUpdate
from backend.pipeline.storage.rules_store import RulesStore


class BaseRulesService(ABC):
    """Abstract base class for Rules Service implementations."""

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
    async def update_rule(self, rule_id: str, rule_in: RuleUpdate) -> Rule | None:
        """Fully update an existing transcription rule."""

    @abstractmethod
    async def delete_rule(self, rule_id: str) -> bool:
        """Delete a transcription rule."""


class AlloyRulesService(BaseRulesService):
    """Implementation of the Rules Service using AlloyDB."""

    def __init__(self, store: RulesStore) -> None:
        self._store = store

    async def create_rule(self, rule_in: RuleCreate) -> Rule:
        return await self._store.create_rule(rule_in)

    async def get_rule(self, rule_id: str) -> Rule | None:
        return await self._store.get_rule(rule_id)

    async def list_rules(self) -> list[Rule]:
        return await self._store.list_rules()

    async def update_rule(self, rule_id: str, rule_in: RuleUpdate) -> Rule | None:
        return await self._store.update_rule(rule_id, rule_in)

    async def delete_rule(self, rule_id: str) -> bool:
        return await self._store.delete_rule(rule_id)
