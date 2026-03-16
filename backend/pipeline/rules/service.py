from __future__ import annotations

import uuid
from abc import ABC, abstractmethod

from backend.pipeline.common.rules.models import Rule, RuleCreate, RuleUpdate


class BaseRulesService(ABC):
    """Abstract base class for Rules Service implementations."""

    @abstractmethod
    def create_rule(self, rule_in: RuleCreate) -> Rule:
        """Create a new transcription rule."""

    @abstractmethod
    def get_rule(self, rule_id: str) -> Rule | None:
        """Fetch a specific transcription rule by ID."""

    @abstractmethod
    def list_rules(self) -> list[Rule]:
        """List all transcription rules."""

    @abstractmethod
    def update_rule(self, rule_id: str, rule_in: RuleUpdate) -> Rule | None:
        """Fully update an existing transcription rule."""

    @abstractmethod
    def delete_rule(self, rule_id: str) -> bool:
        """Delete a transcription rule."""


class MockRulesService(BaseRulesService):
    """Mock in-memory implementation of the Rules Service."""

    def __init__(self) -> None:
        # Mock in-memory storage
        self._rules: dict[str, Rule] = {}

    def create_rule(self, rule_in: RuleCreate) -> Rule:
        rule_id = f"rule_{uuid.uuid4().hex[:8]}"
        temp_rule = Rule(rule_id=rule_id, **rule_in.model_dump())
        self._rules[rule_id] = temp_rule
        return temp_rule

    def get_rule(self, rule_id: str) -> Rule | None:
        return self._rules.get(rule_id)

    def list_rules(self) -> list[Rule]:
        return list(self._rules.values())

    def update_rule(self, rule_id: str, rule_in: RuleUpdate) -> Rule | None:
        if rule_id not in self._rules:
            return None

        existing_rule = self._rules[rule_id]
        update_data = rule_in.model_dump(exclude_unset=True)

        # Create a new version of the rule with updated data
        updated_rule_dict = existing_rule.model_dump()
        updated_rule_dict.update(update_data)

        updated_rule = Rule(**updated_rule_dict)        
        self._rules[rule_id] = updated_rule
        return updated_rule

    def delete_rule(self, rule_id: str) -> bool:
        if rule_id in self._rules:
            del self._rules[rule_id]
            return True
        return False


# Default service instance
rules_service: BaseRulesService = MockRulesService()
