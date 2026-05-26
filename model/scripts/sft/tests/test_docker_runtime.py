"""Regression tests for the Docker runtime used by the SFT CLI."""

from __future__ import annotations

import unittest
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[4]


class TestNotebookDockerRuntime(unittest.TestCase):
    def test_notebook_compose_service_builds_from_model_context(self) -> None:
        compose = (_REPO_ROOT / "asr-eval-docker-compose.yml").read_text()

        notebooks_section = compose.split(
            "notebooks-cpu: &notebooks-base", maxsplit=1
        )[1].split("\n  notebooks:", maxsplit=1)[0]

        self.assertIn("context: ./model", notebooks_section)
        self.assertIn(
            "dockerfile: notebook_docker/Dockerfile", notebooks_section
        )

    def test_notebook_image_bootstraps_local_common_package(self) -> None:
        dockerfile = (
            _REPO_ROOT / "model" / "notebook_docker" / "Dockerfile"
        ).read_text()
        entrypoint_path = (
            _REPO_ROOT / "model" / "notebook_docker" / "entrypoint.sh"
        )

        self.assertIn("notebook_docker/entrypoint.sh", dockerfile)
        self.assertIn("ENTRYPOINT", dockerfile)
        self.assertTrue(entrypoint_path.exists())

        entrypoint = entrypoint_path.read_text()
        self.assertIn("/workspace/model", entrypoint)
        self.assertIn("[scoring,vertex]", entrypoint)
        self.assertIn('exec "$@"', entrypoint)

    def test_notebook_requirements_align_with_common_scoring_extra(self) -> None:
        requirements = (
            _REPO_ROOT / "model" / "notebook_docker" / "requirements.txt"
        ).read_text()

        self.assertIn("jiwer>=3.1,<4", requirements)


if __name__ == "__main__":
    unittest.main()
