import importlib
import unittest
from unittest import mock

from backend.pipeline.ingestion.broadcastify_credential_rotation import (
    main as broadcastify_credential_rotation_main,
)


class TestTracingInitialization(unittest.TestCase):
    def test_tracing_not_initialized_on_import(self) -> None:
        """Verifies that setup_tracing is not called during module import."""
        with mock.patch(
            "backend.pipeline.ingestion.broadcastify_credential_rotation.main.setup_tracing"
        ) as mock_setup_tracing:
            importlib.reload(broadcastify_credential_rotation_main)
            mock_setup_tracing.assert_not_called()

    @mock.patch(
        "backend.pipeline.ingestion.broadcastify_credential_rotation.main.setup_tracing"
    )
    def test_tracing_initialized_on_handler(
        self,
        mock_setup_tracing: mock.MagicMock,
    ) -> None:
        """Verifies setup_tracing is called when the HTTP handler runs."""
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
            mock_setup_tracing.assert_called_once_with(
                service_name="broadcastify-credential-rotation-service",
                use_batch=False,
            )

    @mock.patch(
        "backend.pipeline.ingestion.broadcastify_credential_rotation.main.setup_tracing"
    )
    @mock.patch(
        "backend.pipeline.ingestion.broadcastify_credential_rotation.main.with_tracer_context"
    )
    def test_trace_context_propagation(
        self,
        mock_with_tracer_context: mock.MagicMock,
        mock_setup_tracing: mock.MagicMock,
    ) -> None:
        """Verifies request headers are propagated into with_tracer_context."""
        del mock_setup_tracing
        mock_request = mock.MagicMock()
        mock_request.headers = {"traceparent": "00-testtraceid-testspan-01"}
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
            mock_with_tracer_context.assert_called_once_with(
                {"traceparent": "00-testtraceid-testspan-01"},
                "broadcastify_credential_rotation",
                "backend.pipeline.ingestion.broadcastify_credential_rotation.main",
            )
