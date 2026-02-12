import re
from abc import ABC, abstractmethod
from typing import TypedDict


class Rule(TypedDict):
    id: str
    pattern: re.Pattern[str]


class EvaluationResult(TypedDict):
    is_flagged: bool
    triggered_rules: list[str]


class BaseTextEvaluator(ABC):
    """
    Interface to ensure all evaluators return a consistent structure.
    Return the EvaluationResult
    """

    @abstractmethod
    def evaluate(self, text: str) -> EvaluationResult:
        pass


class StaticTextEvaluator(BaseTextEvaluator):
    """
    Static implementation of text evaluation.
    """

    _RULES: list[Rule] = [
        {
            "id": "basic_fire_terms",
            "pattern": re.compile(
                r"\b(fire|burn|evacuation|spreading)\b", re.IGNORECASE
            ),
        },
    ]

    @classmethod
    def evaluate(cls, text: str) -> EvaluationResult:
        """Static method to evaluate text using class-level rules."""
        if not text:
            return {"is_flagged": False, "triggered_rules": []}

        matches = [rule["id"] for rule in cls._RULES if rule["pattern"].search(text)]

        return {"is_flagged": len(matches) > 0, "triggered_rules": matches}
