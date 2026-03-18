import re
from abc import ABC, abstractmethod
from typing import TypedDict

from backend.pipeline.common.rules import models


class EvaluationResult(TypedDict):
    is_flagged: bool
    triggered_rules: list[str]


class BaseTextEvaluator(ABC):
    """
    Interface to ensure all evaluators return a consistent structure.
    """

    @abstractmethod
    def evaluate(self, text: str) -> EvaluationResult:
        """
        Evaluates the given text.

        Args:
            text: The text to evaluate.

        Returns:
            An EvaluationResult containing flagging status and triggered rules.
        """


class StaticTextEvaluator(BaseTextEvaluator):
    """
    Static implementation of text evaluation using the common Rule model.
    """

    _RULES: list[models.Rule] = [
        models.Rule(
            rule_id="basic_fire_terms",
            rule_name="Basic Fire Terms",
            scope=models.Scope(level=models.ScopeLevel.GLOBAL),
            conditions=models.RegexConditions(
                evaluation_type=models.EvaluationType.REGEX_MATCH,
                expression=r"\b(fire|burn|evacuation|spreading)\b",
                flags="i",
            ),
        ),
    ]

    @classmethod
    def evaluate(cls, text: str) -> EvaluationResult:
        """
        Static method to evaluate text using class-level rules.

        Args:
            text: The text to evaluate.

        Returns:
            An EvaluationResult containing flagging status and triggered rules.
        """
        if not text:
            return {"is_flagged": False, "triggered_rules": []}

        matches = []
        for rule in cls._RULES:
            if cls._evaluate_rule(rule, text):
                matches.append(rule.rule_id)

        return {"is_flagged": len(matches) > 0, "triggered_rules": matches}

    @classmethod
    def _evaluate_rule(cls, rule: models.Rule, text: str) -> bool:
        """
        Evaluates a single rule against the text.

        Args:
            rule: The rule to evaluate.
            text: The text to evaluate against.

        Returns:
            True if the rule triggers, False otherwise.
        """
        conditions = rule.conditions

        if isinstance(conditions, models.RegexConditions):
            flags = re.IGNORECASE if "i" in conditions.flags else 0
            return bool(re.search(conditions.expression, text, flags))

        if isinstance(conditions, models.KeywordConditions):
            flags = 0 if conditions.case_sensitive else re.IGNORECASE
            if conditions.operator == models.LogicalOperator.ANY:
                return any(
                    re.search(re.escape(k), text, flags) for k in conditions.keywords
                )
            if conditions.operator == models.LogicalOperator.ALL:
                return all(
                    re.search(re.escape(k), text, flags) for k in conditions.keywords
                )

        # For now, we skip GroupConditions as it requires a rule lookup
        return False
