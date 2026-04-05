from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import jwt
import pytest

from backend.pipeline.ingestion.broadcastify_credential_rotation import main

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def configured_module() -> Generator[None]:
    """Configure required module globals for deterministic unit tests."""
    with mock.patch.multiple(
        main,
        PROJECT_ID="test-project",
        BROADCASTIFY_USERNAME="test-user",
        BROADCASTIFY_PASSWORD="test-pass",
        BROADCASTIFY_API_KEY="test-api-key",
        BROADCASTIFY_API_APP_ID="test-app-id",
        BROADCASTIFY_API_KEY_ID="test-key-id",
        SECRET_JWT="broadcastify-jwt",
        AUTH_URL="https://api.bcfy.io/common/v1/auth",
        secret_client=None,
    ):
        yield


class TestAddSecretVersion:
    def test_add_secret_version_adds_encoded_payload(
        self, configured_module: None
    ) -> None:
        del configured_module
        secret_client = mock.MagicMock()
        secret_client.secret_path.return_value = "projects/p/secrets/s"
        secret_client.add_secret_version.return_value.name = (
            "projects/p/secrets/s/versions/1"
        )
        main.secret_client = secret_client

        result = main.add_secret_version(
            secret_client, "broadcastify-jwt", "token-123"
        )

        assert result == "projects/p/secrets/s/versions/1"
        secret_client.secret_path.assert_called_once_with(
            "test-project", "broadcastify-jwt"
        )
        secret_client.add_secret_version.assert_called_once_with(
            request={
                "parent": "projects/p/secrets/s",
                "payload": {"data": b"token-123"},
            }
        )


class TestGenerateJwt:
    def test_generate_jwt_raises_when_api_key_is_missing(
        self, configured_module: None
    ) -> None:
        del configured_module

        with mock.patch.object(main, "BROADCASTIFY_API_KEY", ""):
            with pytest.raises(RuntimeError, match="BROADCASTIFY_API_KEY"):
                main._generate_jwt()

    def test_generate_jwt_has_expected_headers_and_claims(
        self, configured_module: None
    ) -> None:
        del configured_module
        api_key = "signing-secret-that-is-at-least-32-bytes"

        with (
            mock.patch.object(main, "BROADCASTIFY_API_KEY", api_key),
            mock.patch.object(main.time, "time", return_value=1700000000),
        ):
            token = main._generate_jwt({"sub": "uid-1", "utk": "utk-1"})

        decoded = jwt.decode(
            token,
            api_key,
            algorithms=["HS256"],
            options={"verify_exp": False},
        )
        header = jwt.get_unverified_header(token)

        assert header["alg"] == "HS256"
        assert header["typ"] == "JWT"
        assert header["kid"] == "test-key-id"
        assert decoded["iss"] == "test-app-id"
        assert decoded["iat"] == 1700000000
        assert decoded["exp"] == 1700003600
        assert decoded["sub"] == "uid-1"
        assert decoded["utk"] == "utk-1"


class TestBroadcastifyCredentialRotation:
    def test_rotation_success_updates_secret(
        self, configured_module: None
    ) -> None:
        del configured_module
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "uid": "uid-123",
            "token": "utk-456",
        }

        mock_http_client = mock.MagicMock()
        mock_http_client.post.return_value = mock_response
        mock_http_context = mock.MagicMock()
        mock_http_context.__enter__.return_value = mock_http_client

        fake_secret_client = mock.MagicMock()

        with (
            mock.patch.object(
                main.requests, "Session", return_value=mock_http_context
            ),
            mock.patch.object(
                main,
                "_generate_jwt",
                side_effect=["unauth-jwt", "auth-jwt"],
            ) as mock_generate,
            mock.patch.object(main, "add_secret_version") as mock_add,
            mock.patch.object(
                main.secretmanager,
                "SecretManagerServiceClient",
                return_value=fake_secret_client,
            ) as mock_secret_manager,
        ):
            message, status = main.broadcastify_credential_rotation(
                mock.MagicMock()
            )

        assert status == 200
        assert "Successfully updated" in message

        mock_secret_manager.assert_called_once_with()
        assert main.secret_client is fake_secret_client

        mock_http_client.post.assert_called_once_with(
            "https://api.bcfy.io/common/v1/auth",
            headers={"Authorization": "Bearer unauth-jwt"},
            data={"username": "test-user", "password": "test-pass"},
            timeout=30.0,
        )

        assert mock_generate.call_count == 2
        assert mock_generate.call_args_list[1].args[0] == {
            "sub": "uid-123",
            "utk": "utk-456",
        }

        mock_add.assert_called_once_with(
            fake_secret_client,
            "broadcastify-jwt",
            "auth-jwt",
        )

    def test_rotation_raises_on_auth_http_error(
        self, configured_module: None
    ) -> None:
        del configured_module
        mock_response = mock.MagicMock()
        mock_response.status_code = 401
        mock_response.text = "unauthorized"

        mock_http_client = mock.MagicMock()
        mock_http_client.post.return_value = mock_response
        mock_http_context = mock.MagicMock()
        mock_http_context.__enter__.return_value = mock_http_client

        with (
            mock.patch.object(
                main.requests, "Session", return_value=mock_http_context
            ),
            mock.patch.object(main, "_generate_jwt", return_value="unauth-jwt"),
            mock.patch.object(main.secretmanager, "SecretManagerServiceClient"),
        ):
            with pytest.raises(RuntimeError, match="Authentication failed"):
                main.broadcastify_credential_rotation(mock.MagicMock())

    @pytest.mark.parametrize(
        "payload",
        [{"uid": "uid-123"}, {"token": "utk-456"}, {}],
    )
    def test_rotation_raises_when_auth_payload_missing_fields(
        self, configured_module: None, payload: dict[str, str]
    ) -> None:
        del configured_module
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = payload

        mock_http_client = mock.MagicMock()
        mock_http_client.post.return_value = mock_response
        mock_http_context = mock.MagicMock()
        mock_http_context.__enter__.return_value = mock_http_client

        with (
            mock.patch.object(
                main.requests, "Session", return_value=mock_http_context
            ),
            mock.patch.object(
                main, "_generate_jwt", return_value="unauth-jwt"
            ) as mock_generate,
            mock.patch.object(main, "add_secret_version") as mock_add,
            mock.patch.object(main.secretmanager, "SecretManagerServiceClient"),
        ):
            with pytest.raises(
                RuntimeError,
                match="Authentication response missing expected fields",
            ):
                main.broadcastify_credential_rotation(mock.MagicMock())

        mock_generate.assert_called_once_with()
        mock_add.assert_not_called()

    def test_rotation_configures_retries(self, configured_module: None) -> None:
        del configured_module
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "uid": "uid-123",
            "token": "utk-456",
        }

        mock_http_client = mock.MagicMock()
        mock_http_client.post.return_value = mock_response
        mock_http_context = mock.MagicMock()
        mock_http_context.__enter__.return_value = mock_http_client

        with (
            mock.patch.object(
                main.requests, "Session", return_value=mock_http_context
            ),
            mock.patch.object(main, "_generate_jwt", return_value="unauth-jwt"),
            mock.patch.object(main, "add_secret_version"),
            mock.patch.object(main.secretmanager, "SecretManagerServiceClient"),
            mock.patch.object(main, "Retry") as mock_retry,
            mock.patch.object(main, "HTTPAdapter") as mock_adapter,
        ):
            mock_retry.return_value = "mock_retry_instance"
            mock_adapter.return_value = "mock_adapter_instance"

            main.broadcastify_credential_rotation(mock.MagicMock())

        mock_retry.assert_called_once_with(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        mock_adapter.assert_called_once_with(max_retries="mock_retry_instance")
        mock_http_client.mount.assert_called_once_with(
            "https://", "mock_adapter_instance"
        )
