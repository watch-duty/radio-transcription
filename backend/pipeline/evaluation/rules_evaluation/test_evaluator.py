import unittest

from backend.pipeline.evaluation.rules_evaluation.evaluator import (
    BaseTextEvaluator,
    StaticTextEvaluator,
)


class TestTextEvaluator(unittest.TestCase):
    def test_inheritance(self) -> None:
        """Ensure the evaluator correctly implements the base interface."""
        self.assertTrue(issubclass(StaticTextEvaluator, BaseTextEvaluator))

    def test_basic_match_fire(self) -> None:
        """Test that a simple sentence with 'fire' is flagged."""
        text = "There is a fire in the building."
        result = StaticTextEvaluator.evaluate(text)
        self.assertTrue(result["is_flagged"])
        self.assertEqual(len(result["triggered_rules"]), 1)
        self.assertEqual(result["triggered_rules"][0], "basic_fire_terms")

    def test_basic_match_evacuation(self) -> None:
        """Test that 'evacuation' triggers the rule."""
        text = "We nmeed to start evacuation procedures."
        result = StaticTextEvaluator.evaluate(text)
        self.assertTrue(result["is_flagged"])
        self.assertEqual(result["triggered_rules"][0], "basic_fire_terms")

    def test_case_insensitivity(self) -> None:
        """Test that capitalization does not affect matching."""
        text = "The FIRE is spreading rapidly."
        result = StaticTextEvaluator.evaluate(text)
        self.assertTrue(result["is_flagged"], "Should match uppercase 'FIRE'")

        text2 = "Burn notice issued."
        result2 = StaticTextEvaluator.evaluate(text2)
        self.assertTrue(result2["is_flagged"], "Should match title case 'Burn'")

    def test_no_match(self) -> None:
        """Test that unrelated text returns no flags."""
        text = "The quick brown fox jumps over the dog."
        result = StaticTextEvaluator.evaluate(text)
        self.assertFalse(result["is_flagged"])
        self.assertEqual(result["triggered_rules"], [])

    def test_empty_string(self) -> None:
        """Test that empty input is handled gracefully."""
        result = StaticTextEvaluator.evaluate("")
        self.assertFalse(result["is_flagged"])
        self.assertEqual(result["triggered_rules"], [])

    def test_word_boundaries(self) -> None:
        r"""
        Test that words merely containing the keyword (but not exact matches)
        are NOT flagged because of the \b regex boundary.
        """
        # 'firefly' contains 'fire', but should not match due to \b
        text = "Look at that beautiful firefly."
        result = StaticTextEvaluator.evaluate(text)
        self.assertFalse(result["is_flagged"], "'firefly' should not trigger 'fire'")

        # 'sideburns' contains 'burn', but should not match
        text2 = "He has impressive sideburns."
        result2 = StaticTextEvaluator.evaluate(text2)
        self.assertFalse(result2["is_flagged"], "'sideburns' should not trigger 'burn'")

    def test_punctuation_boundaries(self) -> None:
        """Test that keywords next to punctuation are still caught."""
        text = "Help! Fire! Run!"
        result = StaticTextEvaluator.evaluate(text)
        self.assertTrue(
            result["is_flagged"], "Punctuation should not prevent matching."
        )


if __name__ == "__main__":
    unittest.main()
