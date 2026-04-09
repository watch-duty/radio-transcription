import logging
import re
from abc import ABC, abstractmethod
from typing import TypedDict

import cachetools
import requests

from backend.pipeline.common.auth import get_id_token
from backend.pipeline.common.env import is_gcp_env
from backend.pipeline.common.rules import models
from backend.pipeline.schema_types import (
    evaluated_transcribed_audio_pb2 as evaluated_pb2,
)

logger = logging.getLogger(__name__)


class EvaluationResult(TypedDict):
    is_flagged: bool
    triggered_rules: list[str]  # Will contain triggered group IDs
    errors: list[int]  # Using proto enum integer values


class OrganizedGroups:
    def __init__(self) -> None:
        self.global_groups: list[models.ResolvedRuleGroup] = []
        self.feed_specific_groups: dict[
            str, list[models.ResolvedRuleGroup]
        ] = {}


class BaseTextEvaluator(ABC):
    """
    Interface to ensure all evaluators return a consistent structure.
    """

    @abstractmethod
    def evaluate(self, text: str, feed_id: str) -> EvaluationResult:
        """
        Evaluates the given text.

        Args:
            text: The text to evaluate.
            feed_id: ID of the feed the text belongs to.

        Returns:
            An EvaluationResult containing flagging status and triggered rules.
        """

    def _evaluate_rule(self, rule: models.Rule, text: str) -> bool:
        """
        Evaluates a single rule against the text.
        """
        conditions = rule.conditions

        if isinstance(conditions, models.RegexConditions):
            flags = re.IGNORECASE if "i" in conditions.flags else 0
            return bool(re.search(conditions.expression, text, flags))

        if isinstance(conditions, models.KeywordConditions):
            flags = 0 if conditions.case_sensitive else re.IGNORECASE
            if conditions.operator == models.LogicalOperator.ANY:
                return any(
                    re.search(re.escape(k), text, flags)
                    for k in conditions.keywords
                )
            if conditions.operator == models.LogicalOperator.ALL:
                return all(
                    re.search(re.escape(k), text, flags)
                    for k in conditions.keywords
                )

        return False

    def _evaluate_group(
        self, group: models.ResolvedRuleGroup, text: str
    ) -> bool:
        """
        Evaluates a rule group by evaluating its child rules.
        """
        if not group.is_active:
            return False

        if group.operator == models.LogicalOperator.ANY:
            for rule in group.child_rules:
                if self._evaluate_rule(rule, text):
                    return True
            return False

        if group.operator == models.LogicalOperator.ALL:
            for rule in group.child_rules:
                if not self._evaluate_rule(rule, text):
                    return False
            return True
        return False

    def _organize_groups(
        self, groups: list[models.ResolvedRuleGroup]
    ) -> OrganizedGroups:
        organized = OrganizedGroups()
        for group in groups:
            if not group.is_active:
                continue

            if group.scope.level == models.ScopeLevel.GLOBAL:
                organized.global_groups.append(group)
            elif group.scope.level == models.ScopeLevel.FEED_SPECIFIC:
                for feed_id in group.scope.target_feeds:
                    if feed_id not in organized.feed_specific_groups:
                        organized.feed_specific_groups[feed_id] = []
                    organized.feed_specific_groups[feed_id].append(group)
        return organized

    def _get_applicable_groups(
        self, organized: OrganizedGroups, feed_id: str
    ) -> list[models.ResolvedRuleGroup]:
        return organized.global_groups + organized.feed_specific_groups.get(
            feed_id, []
        )

    def _evaluate_groupset(
        self, groups: list[models.ResolvedRuleGroup], text: str, feed_id: str
    ) -> EvaluationResult:
        if not text:
            return {"is_flagged": False, "triggered_rules": [], "errors": []}

        organized = self._organize_groups(groups)
        groups_to_evaluate = self._get_applicable_groups(organized, feed_id)

        matches = []
        for group in groups_to_evaluate:
            if self._evaluate_group(group, text):
                matches.append(group.rule_id)

        unique_matches = list(dict.fromkeys(matches))
        return {
            "is_flagged": len(unique_matches) > 0,
            "triggered_rules": unique_matches,
            "errors": [],
        }


class StaticTextEvaluator(BaseTextEvaluator):
    """
    Static implementation of text evaluation using hardcoded groups and rules.
    """

    _GROUPS: list[models.ResolvedRuleGroup] = [
        models.ResolvedRuleGroup(
            rule_id="basic_fire_group",
            rule_name="Basic Fire Group",
            scope=models.Scope(level=models.ScopeLevel.GLOBAL),
            operator=models.LogicalOperator.ANY,
            child_rule_ids=["basic_fire_terms"],
            child_rules=[
                models.Rule(
                    rule_id="basic_fire_terms",
                    rule_name="Basic Fire Terms",
                    conditions=models.RegexConditions(
                        evaluation_type=models.EvaluationType.REGEX_MATCH,
                        expression=r"\b(fire|burn|evacuation|spreading)\b",
                        flags="i",
                    ),
                )
            ],
        ),
    ]

    def evaluate(self, text: str, feed_id: str) -> EvaluationResult:
        return self._evaluate_groupset(self._GROUPS, text, feed_id)


class RemoteTextEvaluator(BaseTextEvaluator):
    """
    Implementation of text evaluation that fetches resolved groups from a remote API.
    """

    def __init__(self, api_url: str) -> None:
        self.api_url = api_url.rstrip("/")
        self.session = requests.Session()
        self._cache = cachetools.TTLCache(maxsize=1, ttl=60)

    def evaluate(self, text: str, feed_id: str) -> EvaluationResult:
        if not feed_id:
            return {
                "is_flagged": False,
                "triggered_rules": [],
                "errors": [
                    evaluated_pb2.EvaluatedTranscribedAudio.EvaluationErrorType.ERROR_FEED_ID_MISSING
                ],
            }
        try:
            groups = self._fetch_resolved_groups()
        except Exception:
            logger.exception("Failed to fetch resolved groups from API")
            return {
                "is_flagged": False,
                "triggered_rules": [],
                "errors": [
                    evaluated_pb2.EvaluatedTranscribedAudio.EvaluationErrorType.ERROR_RULES_FETCH_FAILED
                ],
            }

        return self._evaluate_groupset(groups, text, feed_id)

    def _fetch_resolved_groups(self) -> list[models.ResolvedRuleGroup]:
        if "groups" in self._cache:
            return self._cache["groups"]

        if is_gcp_env():
            token = get_id_token(self.api_url)
            self.session.headers.update({"Authorization": f"Bearer {token}"})

        response = self.session.get(
            f"{self.api_url}/v1/groups/resolved",
            timeout=10,
        )
        response.raise_for_status()

        groups_data = response.json()
        groups = [
            models.ResolvedRuleGroup.model_validate(g) for g in groups_data
        ]
        self._cache["groups"] = groups
        return groups
