import os
import unittest
from unittest.mock import patch

from backend.pipeline.common.env import is_gcp_env


class TestEnv(unittest.TestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_is_gcp_env_default_false(self) -> None:
        """Verifies that is_gcp_env returns False by default when no env vars are set."""
        self.assertFalse(is_gcp_env())

    @patch.dict(os.environ, {"IS_GCP": "true"}, clear=True)
    def test_is_gcp_env_with_is_gcp_true(self) -> None:
        """Verifies that is_gcp_env returns True when IS_GCP is set to 'true'."""
        self.assertTrue(is_gcp_env())

    @patch.dict(os.environ, {"IS_GCP": "false"}, clear=True)
    def test_is_gcp_env_with_is_gcp_false(self) -> None:
        """Verifies that is_gcp_env returns False when IS_GCP is set to 'false'."""
        self.assertFalse(is_gcp_env())

    @patch.dict(os.environ, {"GOOGLE_CLOUD_PROJECT": "local-project"}, clear=True)
    def test_is_gcp_env_with_local_project(self) -> None:
        """Verifies that is_gcp_env returns False when GOOGLE_CLOUD_PROJECT is 'local-project'."""
        self.assertFalse(is_gcp_env())

    @patch.dict(os.environ, {"GOOGLE_CLOUD_PROJECT": "my-gcp-project-123"}, clear=True)
    def test_is_gcp_env_with_real_project(self) -> None:
        """Verifies that is_gcp_env returns True when GOOGLE_CLOUD_PROJECT is a real project ID."""
        self.assertTrue(is_gcp_env())

    @patch.dict(
        os.environ,
        {"IS_GCP": "false", "GOOGLE_CLOUD_PROJECT": "my-gcp-project-123"},
        clear=True,
    )
    def test_is_gcp_env_with_is_gcp_false_but_real_project(self) -> None:
        """Verifies that is_gcp_env returns True even if IS_GCP is 'false' but a real project ID is set."""
        self.assertTrue(is_gcp_env())
