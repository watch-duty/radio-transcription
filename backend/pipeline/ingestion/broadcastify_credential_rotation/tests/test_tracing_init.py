import importlib
import unittest
from unittest import mock

from backend.pipeline.ingestion.broadcastify_credential_rotation import (
    main as broadcastify_credential_rotation_main,
)


class TestTracingInitialization(unittest.TestCase):
    def test_tracing_not_initialized_on_import(self) -> None:
        """Verifies that setup_logging is not called during module import."""
        with mock.patch(
            "backend.pipeline.common.log_helper.setup_logging"
        ) as mock_setup_logging:
            importlib.reload(broadcastify_credential_rotation_main)
            mock_setup_logging.assert_not_called()

    @mock.patch(
        "backend.pipeline.ingestion.broadcastify_credential_rotation.main.setup_logging"
    )
    def test_tracing_initialized_on_handler(
        self,
        mock_setup_logging: mock.MagicMock,
    ) -> None:
        """Verifies setup_logging is called when the HTTP handler runs."""
        mock_request = mock.MagicMock()
        with (
            mock.patch(
                "backend.pipeline.ingestion.broadcastify_credential_rotation.main._authenticate"
            ) as mock_auth,
            mock.patch(
                "backend.pipeline.ingestion.broadcastify_credential_rotation.main._generate_jwt"
            ) as mock_jwt,
            mock.patch(
                "backend.pipeline.ingestion.broadcastify_credential_rotation.main.add_secret_version"
            ),
            mock.patch(
                "backend.pipeline.ingestion.broadcastify_credential_rotation.main.secretmanager.SecretManagerServiceClient"
            ),
        ):
            mock_auth.return_value = {"uid": "test-uid", "token": "test-token"}
            mock_jwt.return_value = "test-jwt"
            broadcastify_credential_rotation_main.broadcastify_credential_rotation(
                mock_request
            )
            mock_setup_logging.assert_called_once()
