import logging
import re
from abc import ABC, abstractmethod
from typing import TypedDict

import cachetools
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from backend.pipeline.common.auth_client import get_id_token
from backend.pipeline.common.env import is_gcp_env
from backend.pipeline.common.rules import models
from backend.pipeline.schema_types import EvaluationErrorType

logger = logging.getLogger(__name__)


class EvaluationResult(TypedDict):
    is_flagged: bool
    triggered_rules: list[str]
    errors: list[EvaluationErrorType]


class OrganizedRules:
    def __init__(self) -> None:
        self.global_rules: list[models.Rule] = []
        self.feed_specific_rules: dict[str, list[models.Rule]] = {}


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
                    re.search(re.escape(k), text, flags)
                    for k in conditions.keywords
                )
            if conditions.operator == models.LogicalOperator.ALL:
                return all(
                    re.search(re.escape(k), text, flags)
                    for k in conditions.keywords
                )

        # For now, we skip GroupConditions as it requires a rule lookup
        return False

    def _organize_rules(self, rules: list[models.Rule]) -> OrganizedRules:
        organized_rules = OrganizedRules()
        for rule in rules:
            if not rule.is_active:
                continue

            if rule.scope.level == models.ScopeLevel.GLOBAL:
                organized_rules.global_rules.append(rule)
            elif rule.scope.level == models.ScopeLevel.FEED_SPECIFIC:
                for feed_id in rule.scope.target_feeds:
                    if feed_id not in organized_rules.feed_specific_rules:
                        organized_rules.feed_specific_rules[feed_id] = []
                    organized_rules.feed_specific_rules[feed_id].append(rule)
        return organized_rules

    def _get_applicable_rules(
        self, organized_rules: OrganizedRules, feed_id: str
    ) -> list[models.Rule]:
        return (
            organized_rules.global_rules
            + organized_rules.feed_specific_rules.get(feed_id, [])
        )

    def _evaluate_ruleset(
        self, rules: list[models.Rule], text: str, feed_id: str
    ) -> EvaluationResult:
        if not text:
            return {"is_flagged": False, "triggered_rules": [], "errors": []}

        organized_rules = self._organize_rules(rules)
        rules_to_evaluate = self._get_applicable_rules(organized_rules, feed_id)

        matches = []
        for rule in rules_to_evaluate:
            if self._evaluate_rule(rule, text):
                matches.append(rule.rule_id)

        unique_matches = list(dict.fromkeys(matches))
        return {
            "is_flagged": len(unique_matches) > 0,
            "triggered_rules": unique_matches,
            "errors": [],
        }


class StaticTextEvaluator(BaseTextEvaluator):
    """
    Static implementation of text evaluation using the common Rule model.
    Can be used as a fallback if the remote API is unavailable.
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

    def evaluate(self, text: str, feed_id: str) -> EvaluationResult:
        """
        Evaluates text using class-level rules.

        Args:
            text: The text to evaluate.
            feed_id: The ID of the feed associated with the text.

        Returns:
            An EvaluationResult containing flagging status and triggered rules.
        """
        return self._evaluate_ruleset(self._RULES, text, feed_id)


class RemoteTextEvaluator(BaseTextEvaluator):
    """
    Implementation of text evaluation that fetches rules from a remote API.
    """

    def __init__(self, api_url: str, cache_ttl_seconds: float = 60) -> None:
        """
        Initializes the RemoteTextEvaluator.

        Args:
            api_url: The URL of the rules management service API.
            cache_ttl_seconds: How long to cache fetched rules. Use 0 to
                disable caching for local/integration environments that mutate
                rules during a run.
        """
        if cache_ttl_seconds < 0:
            msg = "cache_ttl_seconds must be non-negative"
            raise ValueError(msg)

        self.api_url = api_url.rstrip("/")
        self.session = requests.Session()

        # Configure automatic retries at the Session level
        retries = Retry(
            total=3,  # 1 initial attempt + 3 retries = 4 attempts total
            backoff_factor=1,  # Sleeps for 1s, 2s, 4s... between attempts
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=True,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self._cache_ttl_seconds = cache_ttl_seconds
        self._cache = cachetools.TTLCache(
            maxsize=1,
            ttl=cache_ttl_seconds,
        )

    def evaluate(self, text: str, feed_id: str) -> EvaluationResult:
        """
        Evaluates the given text by fetching rules from the API.

        Args:
            text: The text to evaluate.
            feed_id: The ID of the feed associated with the text.

        Returns:
            An EvaluationResult containing flagging status and triggered rules.
        """
        if not feed_id:
            return {
                "is_flagged": False,
                "triggered_rules": [],
                "errors": [EvaluationErrorType.ERROR_FEED_ID_MISSING],
            }
        try:
            rules = self._fetch_rules()
        except Exception:
            logger.exception("Failed to fetch rules from API")
            return {
                "is_flagged": False,
                "triggered_rules": [],
                "errors": [EvaluationErrorType.ERROR_RULES_FETCH_FAILED],
            }

        return self._evaluate_ruleset(rules, text, feed_id)

    def _fetch_rules(self) -> list[models.Rule]:
        """
        Fetches rules from the rules management service API.

        Returns:
            A list of Rule objects.
        """
        if self._cache_ttl_seconds > 0 and "rules" in self._cache:
            return self._cache["rules"]

        # When running on Cloud Run, use the metadata server to get an ID token
        if is_gcp_env():
            token = get_id_token(self.api_url)
            self.session.headers.update({"Authorization": f"Bearer {token}"})

        response = self.session.get(
            f"{self.api_url}/v1/rules",
            timeout=10,
        )
        response.raise_for_status()

        rules_data = response.json()
        rules = [models.Rule.model_validate(rule) for rule in rules_data]
        if self._cache_ttl_seconds > 0:
            self._cache["rules"] = rules
        return rules
