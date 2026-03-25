import unittest
from unittest.mock import patch

from backend.pipeline.evaluation.rules_evaluation import evaluator


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


class TestRemoteTextEvaluator(unittest.TestCase):
    """Tests for the RemoteTextEvaluator class."""

    def setUp(self) -> None:
        self.api_url = "http://localhost:8080"
        self.remote_evaluator = evaluator.RemoteTextEvaluator(self.api_url)

    @patch(
        "backend.pipeline.evaluation.rules_evaluation.evaluator.get_id_token"
    )
    @patch("requests.Session.get")
    def test_evaluate_success(self, mock_get, mock_get_id_token) -> None:
        """Test that RemoteTextEvaluator successfully fetches and evaluates rules."""
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
        mock_get.assert_called_with(f"{self.api_url}/v1/rules", timeout=10)

        # Verify Authorization header was set
        self.assertEqual(
            self.remote_evaluator.session.headers.get("Authorization"),
            "Bearer mock_token",
        )
        mock_get_id_token.assert_called_with(self.api_url)

    @patch(
        "backend.pipeline.evaluation.rules_evaluation.evaluator.get_id_token"
    )
    @patch("requests.Session.get")
    def test_evaluate_local_dev(self, mock_get, mock_get_id_token) -> None:
        """Test that RemoteTextEvaluator skips authentication in LOCAL_DEV mode."""
        with patch.dict("os.environ", {"LOCAL_DEV": "true"}):
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

        result = self.remote_evaluator.evaluate("This is a test message.", feed_id="test_feed")
        self.assertFalse(result["is_flagged"])

    @patch("backend.pipeline.evaluation.rules_evaluation.evaluator.get_id_token")
    @patch("requests.Session.get")
    def test_evaluate_feed_specific_rules(self, mock_get, mock_get_id_token) -> None:
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
        result_other = self.remote_evaluator.evaluate(text, feed_id="other_feed")
        self.assertTrue(result_other["is_flagged"])
        self.assertIn("global_rule", result_other["triggered_rules"])
        self.assertNotIn("feed_a_rule", result_other["triggered_rules"])
        self.assertNotIn("feed_b_rule", result_other["triggered_rules"])


if __name__ == "__main__":
    unittest.main()
