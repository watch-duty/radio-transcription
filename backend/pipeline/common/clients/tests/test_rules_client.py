import unittest
from unittest.mock import MagicMock, patch

import httpx

from backend.pipeline.common.clients.rules_client import RulesClient


class TestRulesClient(unittest.TestCase):
    def setUp(self) -> None:
        self.api_url = "http://test-rules-api.com"
        self.client = RulesClient(self.api_url)

    def tearDown(self) -> None:
        self.client.close()

    @patch("httpx.Client.get")
    def test_get_rule_tags_dedups_union_across_rules(self, mock_get) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "rule_id": "r1",
                "tags": [{"key": "geo_event_type", "value": "flooding"}],
            },
            {
                "rule_id": "r2",
                "tags": [
                    {"key": "geo_event_type", "value": "flooding"},
                    {"key": "team", "value": "goo"},
                ],
            },
        ]
        mock_get.return_value = mock_response

        tags = self.client.get_rule_tags(["r1", "r2"])

        self.assertEqual(
            [(t.key, t.value) for t in tags or []],
            [("geo_event_type", "flooding"), ("team", "goo")],
        )
        mock_get.assert_called_once_with(
            "http://test-rules-api.com/v1/rules",
            params={"rule_ids": ["r1", "r2"]},
            headers={},
            timeout=5,
        )

    @patch("httpx.Client.get")
    def test_get_rule_tags_empty_ids_skips_call(self, mock_get) -> None:
        tags = self.client.get_rule_tags([])
        self.assertEqual(tags, [])
        mock_get.assert_not_called()

    @patch("httpx.Client.get")
    def test_get_rule_tags_rules_without_tags(self, mock_get) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"rule_id": "r1", "tags": []}]
        mock_get.return_value = mock_response

        tags = self.client.get_rule_tags(["r1"])
        self.assertEqual(tags, [])

    @patch("time.sleep", return_value=None)
    @patch("httpx.Client.get")
    def test_get_rule_tags_retry_exhausted_returns_none(
        self, mock_get, mock_sleep
    ) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Service Unavailable",
            request=MagicMock(spec=httpx.Request),
            response=mock_response,
        )
        mock_get.return_value = mock_response

        tags = self.client.get_rule_tags(["r1"])
        self.assertIsNone(tags)
        self.assertEqual(mock_get.call_count, 4)


if __name__ == "__main__":
    unittest.main()
