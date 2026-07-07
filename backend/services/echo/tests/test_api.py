import unittest
from unittest.mock import MagicMock, patch

from backend.services.echo.main import verify_directory


class TestEchoAPI(unittest.TestCase):
    @patch("backend.services.echo.main.container")
    def test_missing_payload(self, mock_container) -> None:
        mock_request = MagicMock()
        mock_request.get_json.return_value = None

        resp, status = verify_directory(mock_request)

        self.assertEqual(status, 400)
        self.assertIn("error", resp)

    @patch("backend.services.echo.main.container")
    def test_missing_source_feed_id(self, mock_container) -> None:
        mock_request = MagicMock()
        mock_request.get_json.return_value = {"something": "else"}

        resp, status = verify_directory(mock_request)

        self.assertEqual(status, 400)
        self.assertIn("error", resp)

    @patch("backend.services.echo.main.container")
    def test_exists(self, mock_container) -> None:
        mock_backend = MagicMock()
        mock_backend.verify_directory_exists.return_value = True
        mock_container.get_echo_backend.return_value = mock_backend

        mock_request = MagicMock()
        mock_request.get_json.return_value = {"source_feed_id": "test"}

        resp, status = verify_directory(mock_request)

        self.assertEqual(status, 200)
        self.assertTrue(resp["exists"])
        mock_backend.verify_directory_exists.assert_called_once_with("test")

    @patch("backend.services.echo.main.container")
    def test_does_not_exist(self, mock_container) -> None:
        mock_backend = MagicMock()
        mock_backend.verify_directory_exists.return_value = False
        mock_container.get_echo_backend.return_value = mock_backend

        mock_request = MagicMock()
        mock_request.get_json.return_value = {"source_feed_id": "test"}

        resp, status = verify_directory(mock_request)

        self.assertEqual(status, 200)
        self.assertFalse(resp["exists"])

    @patch("backend.services.echo.main.container")
    def test_backend_error(self, mock_container) -> None:
        mock_backend = MagicMock()
        mock_backend.verify_directory_exists.side_effect = Exception(
            "API error"
        )
        mock_container.get_echo_backend.return_value = mock_backend

        mock_request = MagicMock()
        mock_request.get_json.return_value = {"source_feed_id": "test"}

        resp, status = verify_directory(mock_request)

        self.assertEqual(status, 500)
        self.assertEqual(resp["error"], "API error")
