import importlib
import os
import unittest
from unittest.mock import patch

MODULE_NAME = "backend.pipeline.evaluation.main"


class TestMainConfiguration(unittest.TestCase):
    """Tests for evaluation service module configuration."""

    @classmethod
    def setUpClass(cls) -> None:
        cls._env_patcher = patch.dict(
            os.environ,
            {
                "RULES_EVALUATION_RESULTS_TOPIC": (
                    "projects/test/topics/results"
                ),
                "AUDIO_SEGMENTS_API_URL": "http://audio-segments-api:8091",
                "RULES_API_URL": "http://rules-api:8086",
            },
        )
        cls._env_patcher.start()
        cls.main = importlib.import_module(MODULE_NAME)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._env_patcher.stop()

    def _get_rules_cache_ttl(
        self,
        raw_ttl: str | None,
    ) -> float:
        env: dict[str, str] = {}
        if raw_ttl is not None:
            env["RULES_CACHE_TTL_SECONDS"] = raw_ttl

        with patch.dict(os.environ, env):
            if raw_ttl is None:
                os.environ.pop("RULES_CACHE_TTL_SECONDS", None)
            return self.main._get_rules_cache_ttl_seconds()

    def test_missing_rules_cache_ttl_defaults_to_sixty_seconds(self) -> None:
        ttl_seconds = self._get_rules_cache_ttl(None)

        self.assertEqual(ttl_seconds, 60.0)

    def test_invalid_rules_cache_ttl_defaults_to_sixty_seconds(self) -> None:
        ttl_seconds = self._get_rules_cache_ttl("60s")

        self.assertEqual(ttl_seconds, 60.0)

    def test_empty_rules_cache_ttl_defaults_to_sixty_seconds(self) -> None:
        ttl_seconds = self._get_rules_cache_ttl("")

        self.assertEqual(ttl_seconds, 60.0)

    def test_negative_rules_cache_ttl_defaults_to_sixty_seconds(self) -> None:
        ttl_seconds = self._get_rules_cache_ttl("-1")

        self.assertEqual(ttl_seconds, 60.0)

    def test_nan_rules_cache_ttl_defaults_to_sixty_seconds(self) -> None:
        ttl_seconds = self._get_rules_cache_ttl("nan")

        self.assertEqual(ttl_seconds, 60.0)

    def test_subsecond_rules_cache_ttl_is_preserved(self) -> None:
        ttl_seconds = self._get_rules_cache_ttl("0.5")

        self.assertEqual(ttl_seconds, 0.5)


if __name__ == "__main__":
    unittest.main()
