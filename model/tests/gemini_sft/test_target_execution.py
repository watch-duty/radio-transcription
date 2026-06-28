from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SRC_DIR = str(Path(__file__).resolve().parents[2] / "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from gemini_sft.config import EvalExecutionConfig, EvalModelTarget  # noqa: E402
from gemini_sft.target_execution import resolve_target_backend  # noqa: E402


class TestTargetBackendResolver(unittest.TestCase):
    def test_publisher_model_defaults_to_batch(self) -> None:
        backend = resolve_target_backend(
            EvalModelTarget(label="base", model="gemini-3.1-flash-lite"),
            EvalExecutionConfig(),
        )

        self.assertEqual(backend, "batch")

    def test_endpoint_resource_defaults_to_online(self) -> None:
        backend = resolve_target_backend(
            EvalModelTarget(
                label="checkpoint_6",
                model="projects/p/locations/us-central1/endpoints/123",
            ),
            EvalExecutionConfig(),
        )

        self.assertEqual(backend, "online")

    def test_forced_backend_overrides_model_shape(self) -> None:
        publisher = EvalModelTarget(
            label="base",
            model="gemini-3.1-flash-lite",
        )
        endpoint = EvalModelTarget(
            label="checkpoint_6",
            model="projects/p/locations/us-central1/endpoints/123",
        )

        self.assertEqual(
            resolve_target_backend(
                publisher,
                EvalExecutionConfig(backend="online"),
            ),
            "online",
        )
        self.assertEqual(
            resolve_target_backend(
                endpoint,
                EvalExecutionConfig(backend="batch"),
            ),
            "batch",
        )


if __name__ == "__main__":
    unittest.main()
