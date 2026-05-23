import importlib
import os
import sys
import unittest
from unittest.mock import patch

MODULE_NAME = "backend.pipeline.evaluation.main"


class TestMainConfiguration(unittest.TestCase):
    """Tests for evaluation service module configuration."""

    def _load_main_with_rules_cache_ttl(
        self,
        raw_ttl: str | None,
    ):
        env = {
            "RULES_EVALUATION_RESULTS_TOPIC": "projects/test/topics/results",
            "TRANSCRIPTS_API_URL": "http://transcripts-api:8087",
            "RULES_API_URL": "http://rules-api:8086",
        }
        if raw_ttl is not None:
            env["RULES_CACHE_TTL_SECONDS"] = raw_ttl

        with patch.dict(os.environ, env):
            if raw_ttl is None:
                os.environ.pop("RULES_CACHE_TTL_SECONDS", None)
            if MODULE_NAME in sys.modules:
                return importlib.reload(sys.modules[MODULE_NAME])
            return importlib.import_module(MODULE_NAME)

    def test_invalid_rules_cache_ttl_defaults_to_sixty_seconds(self) -> None:
        main = self._load_main_with_rules_cache_ttl("60s")

        self.assertEqual(main.RULES_CACHE_TTL_SECONDS, 60.0)

    def test_empty_rules_cache_ttl_defaults_to_sixty_seconds(self) -> None:
        main = self._load_main_with_rules_cache_ttl("")

        self.assertEqual(main.RULES_CACHE_TTL_SECONDS, 60.0)

    def test_negative_rules_cache_ttl_defaults_to_sixty_seconds(self) -> None:
        main = self._load_main_with_rules_cache_ttl("-1")

        self.assertEqual(main.RULES_CACHE_TTL_SECONDS, 60.0)

    def test_subsecond_rules_cache_ttl_is_preserved(self) -> None:
        main = self._load_main_with_rules_cache_ttl("0.5")

        self.assertEqual(main.RULES_CACHE_TTL_SECONDS, 0.5)


if __name__ == "__main__":
    unittest.main()
