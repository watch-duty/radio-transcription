from __future__ import annotations

import importlib
import os
import sys
import unittest
from unittest import mock

import jwt

MODULE_PATH = (
    "backend.pipeline.ingestion.broadcastify_credential_rotation.main"
)


def _required_env() -> dict[str, str]:
    return {
        "GOOGLE_CLOUD_PROJECT": "test-project",
        "BROADCASTIFY_USERNAME": "test-user",
        "BROADCASTIFY_PASSWORD": "test-pass",
        "BROADCASTIFY_API_KEY": "test-api-key",
        "BROADCASTIFY_API_APP_ID": "test-app-id",
        "BROADCASTIFY_API_KEY_ID": "test-key-id",
    }


def _load_module(
    env_overrides: dict[str, str] | None = None,
) -> tuple[object, mock.MagicMock]:
    env = _required_env()
    if env_overrides:
        env.update(env_overrides)

    fake_secret_client = mock.MagicMock()

    with mock.patch.dict(os.environ, env, clear=True):
        with mock.patch(
            "google.cloud.secretmanager.SecretManagerServiceClient",
            return_value=fake_secret_client,
        ):
            sys.modules.pop(MODULE_PATH, None)
            module = importlib.import_module(MODULE_PATH)

    return module, fake_secret_client


class TestAddSecretVersion(unittest.TestCase):
    def test_add_secret_version_adds_encoded_payload(self) -> None:
        module, secret_client = _load_module()

        secret_client.secret_path.return_value = "projects/p/secrets/s"
        secret_client.add_secret_version.return_value.name = (
            "projects/p/secrets/s/versions/1"
        )

        result = module.add_secret_version("broadcastify-jwt", "token-123")

        self.assertEqual(result, "projects/p/secrets/s/versions/1")
        secret_client.secret_path.assert_called_once_with(
            "test-project", "broadcastify-jwt"
        )
        secret_client.add_secret_version.assert_called_once_with(
            request={
                "parent": "projects/p/secrets/s",
                "payload": {"data": b"token-123"},
            }
        )


class TestGenerateJwt(unittest.TestCase):
    def test_generate_jwt_raises_when_api_key_is_missing(self) -> None:
        module, _ = _load_module({"BROADCASTIFY_API_KEY": ""})

        with self.assertRaises(ValueError):
            module._generate_jwt()

    def test_generate_jwt_has_expected_headers_and_claims(self) -> None:
        api_key = "signing-secret-that-is-at-least-32-bytes"
        module, _ = _load_module({"BROADCASTIFY_API_KEY": api_key})

        with mock.patch.object(module.time, "time", return_value=1700000000):
            token = module._generate_jwt({"sub": "uid-1", "utk": "utk-1"})

        decoded = jwt.decode(
            token,
            api_key,
            algorithms=["HS256"],
            options={"verify_exp": False},
        )
        header = jwt.get_unverified_header(token)

        self.assertEqual(header["alg"], "HS256")
        self.assertEqual(header["typ"], "JWT")
        self.assertEqual(header["kid"], "test-key-id")
        self.assertEqual(decoded["iss"], "test-app-id")
        self.assertEqual(decoded["iat"], 1700000000)
        self.assertEqual(decoded["exp"], 1700002100)
        self.assertEqual(decoded["sub"], "uid-1")
        self.assertEqual(decoded["utk"], "utk-1")


class TestBroadcastifyCredentialRotation(unittest.TestCase):
    def test_rotation_success_updates_all_secrets(self) -> None:
        module, _ = _load_module()

        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"uid": "uid-123", "token": "utk-456"}

        mock_http_client = mock.MagicMock()
        mock_http_client.post.return_value = mock_response

        with mock.patch.object(module.httpx, "Client", return_value=mock_http_client):
            with mock.patch.object(
                module,
                "_generate_jwt",
                side_effect=["unauth-jwt", "auth-jwt"],
            ) as mock_generate:
                with mock.patch.object(module, "add_secret_version") as mock_add:
                    message, status = module.broadcastify_credential_rotation()

        self.assertEqual(status, 200)
        self.assertIn("Successfully updated", message)

        mock_http_client.post.assert_called_once_with(
            "https://api.bcfy.io/common/v1/auth",
            headers={"Authorization": "Bearer unauth-jwt"},
            data={"username": "test-user", "password": "test-pass"},
        )

        self.assertEqual(mock_generate.call_count, 2)
        self.assertEqual(mock_generate.call_args_list[1].args[0], {
            "sub": "uid-123",
            "utk": "utk-456",
        })

        self.assertEqual(mock_add.call_count, 3)
        mock_add.assert_any_call("broadcastify-jwt", "auth-jwt")
        mock_add.assert_any_call("broadcastify-utk", "utk-456")
        mock_add.assert_any_call("broadcastify-uid", "uid-123")

    def test_rotation_raises_on_auth_http_error(self) -> None:
        module, _ = _load_module()

        mock_response = mock.MagicMock()
        mock_response.status_code = 401
        mock_response.text = "unauthorized"

        mock_http_client = mock.MagicMock()
        mock_http_client.post.return_value = mock_response

        with mock.patch.object(module.httpx, "Client", return_value=mock_http_client):
            with mock.patch.object(module, "_generate_jwt", return_value="unauth-jwt"):
                with self.assertRaises(RuntimeError) as context:
                    module.broadcastify_credential_rotation()

        self.assertIn("Authentication Failed: 401 - unauthorized", str(context.exception))

    def test_rotation_logs_and_errors_when_auth_payload_missing_fields(self) -> None:
        module, _ = _load_module()

        for payload in ({"uid": "uid-123"}, {"token": "utk-456"}, {}):
            with self.subTest(payload=payload):
                mock_response = mock.MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = payload

                mock_http_client = mock.MagicMock()
                mock_http_client.post.return_value = mock_response

                with mock.patch.object(
                    module.httpx,
                    "Client",
                    return_value=mock_http_client,
                ):
                    with mock.patch.object(
                        module,
                        "_generate_jwt",
                        return_value="unauth-jwt",
                    ) as mock_generate:
                        with mock.patch.object(module.logger, "exception") as mock_exc:
                            with self.assertRaises(KeyError):
                                module.broadcastify_credential_rotation()

                mock_generate.assert_called_once_with()
                mock_exc.assert_called_once()


if __name__ == "__main__":
    unittest.main()
