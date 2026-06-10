import functools
import logging
import re
from abc import ABC, abstractmethod
from typing import TypedDict

import cachetools

from backend.pipeline.common.auth_client import get_id_token
from backend.pipeline.common.clients.session_helper import (
    create_resilient_session,
)
from backend.pipeline.common.env import is_gcp_env
from backend.pipeline.common.rules import models
from backend.pipeline.common.tracing_utils import get_current_traceparent
from backend.pipeline.schema_types import EvaluationErrorType

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1024)
def _get_compiled_keyword_regex(
    keywords: tuple[str, ...],
    *,
    case_sensitive: bool,
    operator: models.LogicalOperator,
) -> re.Pattern | list[re.Pattern]:
    flags = 0 if case_sensitive else re.IGNORECASE

    def _to_pattern(k: str) -> str:
        pattern = ""
        if k and re.match(r"^\w", k):
            pattern += r"\b"
        pattern += re.escape(k)
        if k and re.match(r"\w$", k):
            pattern += r"\b"
        return pattern

    if operator == models.LogicalOperator.ANY:
        combined_pattern = "|".join(f"(?:{_to_pattern(k)})" for k in keywords)
        return re.compile(combined_pattern, flags)

    return [re.compile(_to_pattern(k), flags) for k in keywords]


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
            # Filter out empty, empty-string, or whitespace-only keywords
            valid_keywords = [
                k.strip() for k in conditions.keywords if k and k.strip()
            ]
            if not valid_keywords:
                return False

            # 1. Fast-path substring pre-check using optimized in-operator
            target_text = text if conditions.case_sensitive else text.lower()
            keywords_to_check = (
                valid_keywords
                if conditions.case_sensitive
                else [k.lower() for k in valid_keywords]
            )

            has_substrings = True
            if conditions.operator == models.LogicalOperator.ANY:
                has_substrings = any(
                    k in target_text for k in keywords_to_check
                )
            elif conditions.operator == models.LogicalOperator.ALL:
                has_substrings = all(
                    k in target_text for k in keywords_to_check
                )

            if not has_substrings:
                return False

            # 2. Retrieve cached/compiled regex for precise word boundary verification
            compiled = _get_compiled_keyword_regex(
                tuple(valid_keywords),
                case_sensitive=conditions.case_sensitive,
                operator=conditions.operator,
            )

            if conditions.operator == models.LogicalOperator.ANY and isinstance(
                compiled, re.Pattern
            ):
                return bool(compiled.search(text))
            if conditions.operator == models.LogicalOperator.ALL and isinstance(
                compiled, list
            ):
                return all(bool(p.search(text)) for p in compiled)

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
        self.session = create_resilient_session(
            max_retries=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=True,
        )

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

        headers = {}
        traceparent = get_current_traceparent()
        if traceparent:
            headers["traceparent"] = traceparent

        # When running on Cloud Run, use the metadata server to get an ID token
        if is_gcp_env():
            token = get_id_token(self.api_url)
            self.session.headers.update({"Authorization": f"Bearer {token}"})

        response = self.session.get(
            f"{self.api_url}/v1/rules",
            headers=headers,
            timeout=10,
        )

        response.raise_for_status()

        rules_data = response.json()
        rules = [models.Rule.model_validate(rule) for rule in rules_data]
        if self._cache_ttl_seconds > 0:
            self._cache["rules"] = rules
        return rules
