import io
import json
import unittest
from unittest.mock import ANY, Mock, patch

import urllib3
from urllib3.response import HTTPResponse

from backend.pipeline.common.rules import models
from backend.pipeline.evaluation.rules_evaluation import evaluator
from backend.pipeline.schema_types import EvaluationErrorType


class TestTextEvaluator(unittest.TestCase):
    """Tests for the StaticTextEvaluator class."""

    def setUp(self) -> None:
        self.static_evaluator = evaluator.StaticTextEvaluator()

    def test_inheritance(self) -> None:
        """Ensure the evaluator correctly implements the base interface."""
        self.assertTrue(
            issubclass(
                evaluator.StaticTextEvaluator, evaluator.BaseTextEvaluator
            )
        )
        self.assertTrue(
            issubclass(
                evaluator.RemoteTextEvaluator,
                evaluator.BaseTextEvaluator,
            )
        )

    def test_basic_match_fire(self) -> None:
        """Test that a simple sentence with 'fire' is flagged."""
        text = "There is a fire in the building."
        result = self.static_evaluator.evaluate(text, feed_id="test_feed")
        self.assertTrue(result["is_flagged"])
        self.assertEqual(len(result["triggered_rules"]), 1)
        self.assertEqual(result["triggered_rules"][0], "basic_fire_terms")
        annotations = result["rule_annotations"]
        self.assertEqual(len(annotations), 1)
        self.assertIn("basic_fire_terms", annotations)
        text_match = annotations["basic_fire_terms"].text_match
        assert text_match is not None
        spans = text_match
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].start, text.index("fire"))
        self.assertEqual(spans[0].end, text.index("fire") + 4)
        self.assertEqual(spans[0].matched_text, "fire")

    def test_case_insensitive_preserves_original_casing(self) -> None:
        """Spans should carry the matched substring exactly as it appears in the transcript."""
        text = "The FIRE is spreading."
        result = self.static_evaluator.evaluate(text, feed_id="test_feed")
        self.assertTrue(result["is_flagged"])
        text_match = result["rule_annotations"]["basic_fire_terms"].text_match
        assert text_match is not None
        spans = text_match
        matched_texts = [s.matched_text for s in spans]
        self.assertIn("FIRE", matched_texts)
        self.assertIn("spreading", matched_texts)

    def test_no_match_produces_no_annotation(self) -> None:
        """Non-firing rules contribute nothing to rule_annotations."""
        text = "The quick brown fox jumps over the dog."
        result = self.static_evaluator.evaluate(text, feed_id="test_feed")
        self.assertEqual(result["rule_annotations"], {})

    def test_basic_match_evacuation(self) -> None:
        """Test that 'evacuation' triggers the rule."""
        text = "We nmeed to start evacuation procedures."
        result = self.static_evaluator.evaluate(text, feed_id="test_feed")
        self.assertTrue(result["is_flagged"])
        self.assertEqual(result["triggered_rules"][0], "basic_fire_terms")

    def test_case_insensitivity(self) -> None:
        """Test that capitalization does not affect matching."""
        text = "The FIRE is spreading rapidly."
        result = self.static_evaluator.evaluate(text, feed_id="test_feed_1")
        self.assertTrue(result["is_flagged"], "Should match uppercase 'FIRE'")

        text2 = "Burn notice issued."
        result2 = self.static_evaluator.evaluate(text2, feed_id="test_feed_2")
        self.assertTrue(result2["is_flagged"], "Should match title case 'Burn'")

    def test_no_match(self) -> None:
        """Test that unrelated text returns no flags."""
        text = "The quick brown fox jumps over the dog."
        result = self.static_evaluator.evaluate(text, feed_id="test_feed")
        self.assertFalse(result["is_flagged"])
        self.assertEqual(result["triggered_rules"], [])

    def test_empty_string(self) -> None:
        """Test that empty input is handled gracefully."""
        result = self.static_evaluator.evaluate("", feed_id="test_feed")
        self.assertFalse(result["is_flagged"])
        self.assertEqual(result["triggered_rules"], [])

    def test_word_boundaries(self) -> None:
        r"""
        Test that words merely containing the keyword (but not exact matches)
        are NOT flagged because of the \b regex boundary.
        """
        # 'firefly' contains 'fire', but should not match due to \b
        text = "Look at that beautiful firefly."
        result = self.static_evaluator.evaluate(text, feed_id="test_feed_1")
        self.assertFalse(
            result["is_flagged"], "'firefly' should not trigger 'fire'"
        )

        # 'sideburns' contains 'burn', but should not match
        text2 = "He has impressive sideburns."
        result2 = self.static_evaluator.evaluate(text2, feed_id="test_feed_2")
        self.assertFalse(
            result2["is_flagged"], "'sideburns' should not trigger 'burn'"
        )

    def test_punctuation_boundaries(self) -> None:
        """Test that keywords next to punctuation are still caught."""
        text = "Help! Fire! Run!"
        result = self.static_evaluator.evaluate(text, "test_feed_2")
        self.assertTrue(
            result["is_flagged"], "Punctuation should not prevent matching."
        )


def _keyword_rule(
    rule_id: str,
    keywords: list[str],
    *,
    operator: models.LogicalOperator = models.LogicalOperator.ANY,
    case_sensitive: bool = False,
) -> models.Rule:
    return models.Rule(
        rule_id=rule_id,
        rule_name=rule_id,
        is_active=True,
        scope=models.Scope(level=models.ScopeLevel.GLOBAL),
        conditions=models.KeywordConditions(
            evaluation_type=models.EvaluationType.KEYWORD_MATCH,
            operator=operator,
            keywords=keywords,
            case_sensitive=case_sensitive,
        ),
    )


def _regex_rule(
    rule_id: str, expression: str, *, flags: str = "i"
) -> models.Rule:
    return models.Rule(
        rule_id=rule_id,
        rule_name=rule_id,
        is_active=True,
        scope=models.Scope(level=models.ScopeLevel.GLOBAL),
        conditions=models.RegexConditions(
            evaluation_type=models.EvaluationType.REGEX_MATCH,
            expression=expression,
            flags=flags,
        ),
    )


class _RulesetEvaluator(evaluator.BaseTextEvaluator):
    """Bare evaluator used to exercise _evaluate_ruleset directly."""

    def evaluate(self, text: str, feed_id: str) -> evaluator.EvaluationResult:
        raise NotImplementedError


class TestKeywordAnnotations(unittest.TestCase):
    """Tests for per-rule annotation emission across keyword rules."""

    def setUp(self) -> None:
        self.evaluator = _RulesetEvaluator()

    def test_any_operator_emits_all_keyword_spans(self) -> None:
        rule = _keyword_rule("r1", ["fire", "evacuate"])
        text = "Fire on the ridge, evacuate now"
        result = self.evaluator.evaluate_ruleset([rule], text, "feed-1")
        self.assertTrue(result["is_flagged"])
        text_match = result["rule_annotations"]["r1"].text_match
        assert text_match is not None
        spans = text_match
        matched = [(s.start, s.end, s.matched_text) for s in spans]
        self.assertEqual(
            matched,
            [(0, 4, "Fire"), (19, 27, "evacuate")],
        )

    def test_all_operator_fires_only_when_every_keyword_present(self) -> None:
        rule = _keyword_rule(
            "r1", ["fire", "evacuate"], operator=models.LogicalOperator.ALL
        )

        partial = self.evaluator.evaluate_ruleset(
            [rule], "Fire on the ridge", "feed-1"
        )
        self.assertEqual(partial["rule_annotations"], {})

        full = self.evaluator.evaluate_ruleset(
            [rule], "Fire on the ridge, evacuate now", "feed-1"
        )
        self.assertEqual(len(full["rule_annotations"]), 1)

    def test_case_sensitive_keyword_does_not_match_lowercase(self) -> None:
        rule = _keyword_rule("r1", ["Fire"], case_sensitive=True)
        result = self.evaluator.evaluate_ruleset(
            [rule], "fire on the ridge", "feed-1"
        )
        self.assertEqual(result["rule_annotations"], {})

    def test_multiple_rules_produce_one_annotation_each(self) -> None:
        r1 = _keyword_rule("r1", ["fire"])
        r2 = _keyword_rule("r2", ["evacuate"])
        result = self.evaluator.evaluate_ruleset(
            [r1, r2], "Fire, please evacuate", "feed-1"
        )
        self.assertEqual(list(result["rule_annotations"].keys()), ["r1", "r2"])
        self.assertEqual(result["triggered_rules"], ["r1", "r2"])

    def test_empty_text_returns_empty_annotations(self) -> None:
        rule = _keyword_rule("r1", ["fire"])
        result = self.evaluator.evaluate_ruleset([rule], "", "feed-1")
        self.assertEqual(result["rule_annotations"], {})


class TestRegexAnnotations(unittest.TestCase):
    """Tests for per-rule annotation emission across regex rules."""

    def setUp(self) -> None:
        self.evaluator = _RulesetEvaluator()

    def test_regex_emits_a_span_per_match(self) -> None:
        rule = _regex_rule("r1", r"\d+")
        result = self.evaluator.evaluate_ruleset(
            [rule], "unit 12 to unit 34", "feed-1"
        )
        self.assertTrue(result["is_flagged"])
        text_match = result["rule_annotations"]["r1"].text_match
        assert text_match is not None
        spans = text_match
        self.assertEqual(
            [(s.start, s.end, s.matched_text) for s in spans],
            [(5, 7, "12"), (16, 18, "34")],
        )

    def test_regex_case_insensitive_flag_preserves_casing(self) -> None:
        rule = _regex_rule("r1", r"fire", flags="i")
        result = self.evaluator.evaluate_ruleset(
            [rule], "FIRE on the ridge", "feed-1"
        )
        text_match = result["rule_annotations"]["r1"].text_match
        assert text_match is not None
        spans = text_match
        self.assertEqual(spans[0].matched_text, "FIRE")

    def test_regex_no_match_produces_no_annotation(self) -> None:
        rule = _regex_rule("r1", r"\d+")
        result = self.evaluator.evaluate_ruleset(
            [rule], "no numbers here", "feed-1"
        )
        self.assertEqual(result["rule_annotations"], {})


class TestRemoteTextEvaluator(unittest.TestCase):
    """Tests for the RemoteTextEvaluator class."""

    def setUp(self) -> None:
        self.api_url = "http://localhost:8080"
        self.remote_evaluator = evaluator.RemoteTextEvaluator(self.api_url)

    @patch(
        "backend.pipeline.evaluation.rules_evaluation.evaluator.get_id_token"
    )
    @patch("backend.pipeline.evaluation.rules_evaluation.evaluator.is_gcp_env")
    @patch("requests.Session.get")
    def test_evaluate_success(
        self, mock_get, mock_is_gcp, mock_get_id_token
    ) -> None:
        """Test that RemoteTextEvaluator successfully fetches and evaluates rules."""
        # Force GCP environment for this test
        mock_is_gcp.return_value = True
        # Mock token
        mock_get_id_token.return_value = "mock_token"
        # Mock rule from API
        mock_rule = {
            "rule_id": "test_rule_1",
            "rule_name": "Test Rule 1",
            "is_active": True,
            "scope": {"level": "GLOBAL", "target_feeds": []},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ANY",
                "keywords": ["test"],
                "case_sensitive": False,
            },
        }
        mock_get.return_value.json.return_value = [mock_rule]
        mock_get.return_value.status_code = 200

        text = "This is a test message."
        result = self.remote_evaluator.evaluate(text, feed_id="test_feed")
        self.assertTrue(result["is_flagged"])
        self.assertIn("test_rule_1", result["triggered_rules"])
        mock_get.assert_called_with(
            f"{self.api_url}/v1/rules", headers=ANY, timeout=10
        )

        # Verify Authorization header was set
        self.assertEqual(
            self.remote_evaluator.session.headers.get("Authorization"),
            "Bearer mock_token",
        )
        mock_get_id_token.assert_called_with(self.api_url)

    @patch(
        "backend.pipeline.evaluation.rules_evaluation.evaluator.get_id_token"
    )
    @patch("backend.pipeline.evaluation.rules_evaluation.evaluator.is_gcp_env")
    @patch("requests.Session.get")
    def test_evaluate_outside_gcp(
        self, mock_get, mock_is_gcp, mock_get_id_token
    ) -> None:
        """Test that RemoteTextEvaluator skips authentication when not in GCP."""
        # Force non-GCP environment for this test
        mock_is_gcp.return_value = False
        # Mock rule from API
        mock_rule = {
            "rule_id": "test_rule_1",
            "rule_name": "Test Rule 1",
            "is_active": True,
            "scope": {"level": "GLOBAL", "target_feeds": []},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ANY",
                "keywords": ["test"],
                "case_sensitive": False,
            },
        }
        mock_get.return_value.json.return_value = [mock_rule]
        mock_get.return_value.status_code = 200

        text = "This is a test message."
        result = self.remote_evaluator.evaluate(text, feed_id="test_feed")

        self.assertTrue(result["is_flagged"])
        mock_get_id_token.assert_not_called()
        self.assertIsNone(
            self.remote_evaluator.session.headers.get("Authorization")
        )

    @patch(
        "backend.pipeline.evaluation.rules_evaluation.evaluator.get_id_token"
    )
    @patch("requests.Session.get")
    def test_evaluate_inactive_rule(self, mock_get, mock_get_id_token) -> None:
        """Test that inactive rules are ignored."""
        mock_get_id_token.return_value = "mock_token"
        mock_rule = {
            "rule_id": "inactive_rule",
            "rule_name": "Inactive Rule",
            "is_active": False,
            "scope": {"level": "GLOBAL", "target_feeds": []},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ANY",
                "keywords": ["test"],
                "case_sensitive": False,
            },
        }
        mock_get.return_value.json.return_value = [mock_rule]
        mock_get.return_value.status_code = 200

        result = self.remote_evaluator.evaluate(
            "This is a test message.", feed_id="test_feed"
        )
        self.assertFalse(result["is_flagged"])

    @patch(
        "backend.pipeline.evaluation.rules_evaluation.evaluator.get_id_token"
    )
    @patch("requests.Session.get")
    def test_evaluate_feed_specific_rules(
        self, mock_get, mock_get_id_token
    ) -> None:
        """Test that global and feed-specific rules are correctly applied."""
        mock_get_id_token.return_value = "mock_token"

        rules_data = [
            {
                "rule_id": "global_rule",
                "rule_name": "Global Rule",
                "is_active": True,
                "scope": {"level": "GLOBAL", "target_feeds": []},
                "conditions": {
                    "evaluation_type": "KEYWORD_MATCH",
                    "operator": "ANY",
                    "keywords": ["global"],
                    "case_sensitive": False,
                },
            },
            {
                "rule_id": "feed_a_rule",
                "rule_name": "Feed A Rule",
                "is_active": True,
                "scope": {"level": "FEED_SPECIFIC", "target_feeds": ["feed_A"]},
                "conditions": {
                    "evaluation_type": "KEYWORD_MATCH",
                    "operator": "ANY",
                    "keywords": ["specific"],
                    "case_sensitive": False,
                },
            },
            {
                "rule_id": "feed_b_rule",
                "rule_name": "Feed B Rule",
                "is_active": True,
                "scope": {"level": "FEED_SPECIFIC", "target_feeds": ["feed_B"]},
                "conditions": {
                    "evaluation_type": "KEYWORD_MATCH",
                    "operator": "ANY",
                    "keywords": ["specific"],
                    "case_sensitive": False,
                },
            },
        ]
        mock_get.return_value.json.return_value = rules_data
        mock_get.return_value.status_code = 200

        text = "This matches global and specific words."

        # Test Feed A
        result_a = self.remote_evaluator.evaluate(text, feed_id="feed_A")
        self.assertTrue(result_a["is_flagged"])
        self.assertIn("global_rule", result_a["triggered_rules"])
        self.assertIn("feed_a_rule", result_a["triggered_rules"])
        self.assertNotIn("feed_b_rule", result_a["triggered_rules"])

        # Test Feed B
        result_b = self.remote_evaluator.evaluate(text, feed_id="feed_B")
        self.assertTrue(result_b["is_flagged"])
        self.assertIn("global_rule", result_b["triggered_rules"])
        self.assertIn("feed_b_rule", result_b["triggered_rules"])
        self.assertNotIn("feed_a_rule", result_b["triggered_rules"])

        # Test Other Feed (only global should match)
        result_other = self.remote_evaluator.evaluate(
            text, feed_id="other_feed"
        )
        self.assertTrue(result_other["is_flagged"])
        self.assertIn("global_rule", result_other["triggered_rules"])
        self.assertNotIn("feed_a_rule", result_other["triggered_rules"])
        self.assertNotIn("feed_b_rule", result_other["triggered_rules"])

    @patch("backend.pipeline.evaluation.rules_evaluation.evaluator.is_gcp_env")
    @patch("requests.Session.get")
    def test_evaluate_caches_rules(self, mock_get, mock_is_gcp) -> None:
        """Test that RemoteTextEvaluator caches rules and does not refetch within TTL."""
        mock_is_gcp.return_value = False
        mock_rule = {
            "rule_id": "test_rule_1",
            "rule_name": "Test Rule 1",
            "is_active": True,
            "scope": {"level": "GLOBAL", "target_feeds": []},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ANY",
                "keywords": ["test"],
                "case_sensitive": False,
            },
        }
        mock_get.return_value.json.return_value = [mock_rule]
        mock_get.return_value.status_code = 200

        text = "This is a test message."

        # First call should fetch
        result1 = self.remote_evaluator.evaluate(text, feed_id="test_feed")
        self.assertTrue(result1["is_flagged"])
        self.assertEqual(mock_get.call_count, 1)

        # Second call should use cache
        result2 = self.remote_evaluator.evaluate(text, feed_id="test_feed")
        self.assertTrue(result2["is_flagged"])
        self.assertEqual(mock_get.call_count, 1)  # Still 1

    @patch("backend.pipeline.evaluation.rules_evaluation.evaluator.is_gcp_env")
    @patch("requests.Session.get")
    def test_evaluate_refetches_rules_when_cache_ttl_is_zero(
        self, mock_get, mock_is_gcp
    ) -> None:
        """Test that a zero TTL disables rule caching."""
        mock_is_gcp.return_value = False
        evaluator_without_cache = evaluator.RemoteTextEvaluator(
            self.api_url,
            cache_ttl_seconds=0,
        )
        mock_rule = {
            "rule_id": "dynamic_rule",
            "rule_name": "Dynamic Rule",
            "is_active": True,
            "scope": {"level": "GLOBAL", "target_feeds": []},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ANY",
                "keywords": ["dynamic"],
                "case_sensitive": False,
            },
        }
        first_response = Mock()
        first_response.json.return_value = []
        first_response.status_code = 200
        second_response = Mock()
        second_response.json.return_value = [mock_rule]
        second_response.status_code = 200
        mock_get.side_effect = [first_response, second_response]

        result1 = evaluator_without_cache.evaluate(
            "dynamic message", feed_id="test_feed"
        )
        result2 = evaluator_without_cache.evaluate(
            "dynamic message", feed_id="test_feed"
        )

        self.assertFalse(result1["is_flagged"])
        self.assertTrue(result2["is_flagged"])
        self.assertEqual(mock_get.call_count, 2)

    def test_init_raises_value_error_for_negative_ttl(self) -> None:
        """Test that negative rule cache TTLs are rejected."""
        with self.assertRaises(ValueError) as ctx:
            evaluator.RemoteTextEvaluator(self.api_url, cache_ttl_seconds=-5)

        self.assertEqual(
            str(ctx.exception),
            "cache_ttl_seconds must be non-negative",
        )

    def test_evaluate_missing_feed_id(self) -> None:
        """Test that missing feed_id returns ERROR_FEED_ID_MISSING rule."""
        result = self.remote_evaluator.evaluate("Some text", feed_id="")
        self.assertFalse(result["is_flagged"])
        self.assertIn(
            EvaluationErrorType.ERROR_FEED_ID_MISSING,
            result["errors"],
        )

    @patch("requests.Session.get")
    def test_evaluate_rules_fetch_failure(self, mock_get) -> None:
        """Test that API failure returns ERROR_RULES_FETCH_FAILED rule."""
        mock_get.side_effect = Exception("API Down")

        result = self.remote_evaluator.evaluate(
            "Some text", feed_id="test_feed"
        )
        self.assertFalse(result["is_flagged"])
        self.assertIn(
            EvaluationErrorType.ERROR_RULES_FETCH_FAILED,
            result["errors"],
        )

    @patch("time.sleep", return_value=None)
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_evaluate_rules_fetch_retry_on_timeout_exhausted(
        self, mock_make_request, mock_sleep
    ) -> None:
        """Test that a retryable timeout error retries 3 times and then fails."""
        mock_make_request.side_effect = urllib3.exceptions.ReadTimeoutError(
            Mock(), "/v1/rules", "Read timed out"
        )

        result = self.remote_evaluator.evaluate(
            "Some text", feed_id="test_feed"
        )
        self.assertFalse(result["is_flagged"])
        self.assertIn(
            EvaluationErrorType.ERROR_RULES_FETCH_FAILED,
            result["errors"],
        )
        # total=3 means 1 initial attempt + 3 retries = 4 attempts total
        self.assertEqual(mock_make_request.call_count, 4)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("time.sleep", return_value=None)
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_evaluate_rules_fetch_retry_and_succeed(
        self, mock_make_request, mock_sleep
    ) -> None:
        """Test that a transient timeout error succeeds on a later retry."""
        mock_rule = {
            "rule_id": "test_rule_1",
            "rule_name": "Test Rule 1",
            "is_active": True,
            "scope": {"level": "GLOBAL", "target_feeds": []},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ANY",
                "keywords": ["test"],
                "case_sensitive": False,
            },
        }

        success_body = json.dumps([mock_rule]).encode("utf-8")
        success_response = HTTPResponse(
            body=io.BytesIO(success_body),
            headers={"Content-Type": "application/json"},
            status=200,
            preload_content=False,
        )

        # Fail first 2 times with Timeout, succeed on 3rd time
        mock_make_request.side_effect = [
            urllib3.exceptions.ReadTimeoutError(
                Mock(), "/v1/rules", "Read timed out 1"
            ),
            urllib3.exceptions.ReadTimeoutError(
                Mock(), "/v1/rules", "Read timed out 2"
            ),
            success_response,
        ]

        result = self.remote_evaluator.evaluate(
            "This is a test message.", feed_id="test_feed"
        )
        self.assertTrue(result["is_flagged"])
        self.assertEqual(mock_make_request.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 1)

    @patch("time.sleep", return_value=None)
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_evaluate_rules_fetch_no_retry_on_404(
        self, mock_make_request, mock_sleep
    ) -> None:
        """Test that non-retryable client errors (e.g. 404) fail immediately without retrying."""
        mock_response = HTTPResponse(
            body=io.BytesIO(b"[]"),
            headers={"Content-Type": "application/json"},
            status=404,
            preload_content=False,
        )
        mock_make_request.return_value = mock_response

        result = self.remote_evaluator.evaluate(
            "Some text", feed_id="test_feed"
        )
        self.assertFalse(result["is_flagged"])
        self.assertIn(
            EvaluationErrorType.ERROR_RULES_FETCH_FAILED,
            result["errors"],
        )
        # exactly 1 attempt
        self.assertEqual(mock_make_request.call_count, 1)
        mock_sleep.assert_not_called()

    @patch("time.sleep", return_value=None)
    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    def test_evaluate_rules_fetch_retry_on_503_server_error(
        self, mock_make_request, mock_sleep
    ) -> None:
        """Test that retryable server errors (503) retry and fail after exhausting attempts."""
        mock_response = HTTPResponse(
            body=io.BytesIO(b"[]"),
            headers={"Content-Type": "application/json"},
            status=503,
            preload_content=False,
        )
        mock_make_request.return_value = mock_response

        result = self.remote_evaluator.evaluate(
            "Some text", feed_id="test_feed"
        )
        self.assertFalse(result["is_flagged"])
        self.assertIn(
            EvaluationErrorType.ERROR_RULES_FETCH_FAILED,
            result["errors"],
        )
        # 1 initial attempt + 3 retries = 4 attempts total
        self.assertEqual(mock_make_request.call_count, 4)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("requests.Session.get")
    def test_keyword_match_word_boundaries(self, mock_get) -> None:
        """Test that KEYWORD_MATCH rules enforce word boundaries."""
        mock_rule = {
            "rule_id": "field_fire_rule",
            "rule_name": "Field Fire Rule",
            "is_active": True,
            "scope": {"level": "GLOBAL", "target_feeds": []},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ANY",
                "keywords": ["field fire"],
                "case_sensitive": False,
            },
        }
        mock_get.return_value.json.return_value = [mock_rule]
        mock_get.return_value.status_code = 200

        # Should NOT match Fairfield Fire because "field fire" is inside a word
        text = "Fairfield Fire, Fairfield Fire, Tavolack and Flower Hill Road"
        result = self.remote_evaluator.evaluate(text, feed_id="test_feed")
        self.assertFalse(
            result["is_flagged"], "Should not match substring 'Fairfield Fire'"
        )

        # Should match standalone "field fire"
        text2 = "There is a field fire on Flower Hill Road"
        result2 = self.remote_evaluator.evaluate(text2, feed_id="test_feed")
        self.assertTrue(
            result2["is_flagged"], "Should match standalone 'field fire'"
        )

    @patch("requests.Session.get")
    def test_evaluate_empty_keyword_set_all(self, mock_get) -> None:
        """Test that an ALL rule with empty keywords does not trigger."""
        mock_rule = {
            "rule_id": "malformed_all_rule",
            "rule_name": "Malformed ALL Rule",
            "is_active": True,
            "scope": {"level": "GLOBAL", "target_feeds": []},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ALL",
                "keywords": [],
                "case_sensitive": False,
            },
        }
        mock_get.return_value.json.return_value = [mock_rule]
        mock_get.return_value.status_code = 200

        # Non-empty ordinary transcript must NOT trigger
        result = self.remote_evaluator.evaluate(
            "There is a fire on Flower Hill Road", feed_id="test_feed"
        )
        self.assertFalse(result["is_flagged"])

    @patch("requests.Session.get")
    def test_evaluate_empty_string_keywords(self, mock_get) -> None:
        """Test that whitespace-only or empty-string keywords are ignored."""
        mock_rule = {
            "rule_id": "empty_string_rule",
            "rule_name": "Empty String Rule",
            "is_active": True,
            "scope": {"level": "GLOBAL", "target_feeds": []},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ALL",
                "keywords": ["", "   ", "  "],
                "case_sensitive": False,
            },
        }
        mock_get.return_value.json.return_value = [mock_rule]
        mock_get.return_value.status_code = 200

        result = self.remote_evaluator.evaluate(
            "This is a test transcript.", feed_id="test_feed"
        )
        self.assertFalse(result["is_flagged"])


if __name__ == "__main__":
    unittest.main()
