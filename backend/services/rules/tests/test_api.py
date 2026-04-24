import unittest
from unittest.mock import AsyncMock

from fastapi import status
from fastapi.testclient import TestClient

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.common.rules.models import (
    EvaluationType,
    KeywordConditions,
    Rule,
    RuleMetadata,
    Scope,
    ScopeLevel,
)

from ..main import app, get_rules_service


async def skip_auth() -> dict[str, str]:
    """Mock dependency to bypass authentication in tests."""
    return {"sub": "test@example.com", "email": "test@example.com"}


class TestRulesAPI(unittest.TestCase):
    def setUp(self) -> None:
        """Set up a test client and dependency overrides before each test."""
        self.mock_service = AsyncMock()
        app.dependency_overrides[verify_oidc_token] = skip_auth
        app.dependency_overrides[get_rules_service] = lambda: self.mock_service
        self.client = TestClient(app)

    def tearDown(self) -> None:
        """Clean up after each test."""
        app.dependency_overrides.clear()

    def test_create_rule(self) -> None:
        """Test creating a rule successfully."""
        payload = {
            "rule_name": "Test Rule",
            "is_active": True,
            "scope": {"level": "GLOBAL"},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "keywords": ["test"],
            },
        }
        mock_rule = Rule(
            rule_id="rule_123",
            rule_name="Test Rule",
            is_active=True,
            scope=Scope(level=ScopeLevel.GLOBAL),
            conditions=KeywordConditions(
                evaluation_type=EvaluationType.KEYWORD_MATCH, keywords=["test"]
            ),
            metadata=RuleMetadata(created_by="test@example.com"),
        )
        self.mock_service.create_rule.return_value = mock_rule

        response = self.client.post("/v1/rules", json=payload)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(data["rule_id"], "rule_123")
        self.mock_service.create_rule.assert_called_once()

    def test_get_rule_success(self) -> None:
        """Test fetching an existing rule."""
        rule_id = "rule_123"
        mock_rule = Rule(
            rule_id=rule_id,
            rule_name="Existing Rule",
            is_active=True,
            scope=Scope(level=ScopeLevel.GLOBAL),
            conditions=KeywordConditions(
                evaluation_type=EvaluationType.KEYWORD_MATCH, keywords=["test"]
            ),
        )
        self.mock_service.get_rule.return_value = mock_rule

        response = self.client.get(f"/v1/rules/{rule_id}")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()["rule_id"], rule_id)
        self.mock_service.get_rule.assert_called_once_with(rule_id)

    def test_get_rule_not_found(self) -> None:
        """Test fetching a non-existent rule returns 404."""
        self.mock_service.get_rule.return_value = None
        response = self.client.get("/v1/rules/missing")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_list_rules(self) -> None:
        """Test listing rules."""
        self.mock_service.list_rules.return_value = []
        response = self.client.get("/v1/rules")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsInstance(response.json(), list)

    def test_update_rule_success(self) -> None:
        """Test updating an existing rule."""
        rule_id = "rule_123"
        payload = {"rule_name": "Updated Name"}
        mock_rule = Rule(
            rule_id=rule_id,
            rule_name="Updated Name",
            is_active=True,
            scope=Scope(level=ScopeLevel.GLOBAL),
            conditions=KeywordConditions(
                evaluation_type=EvaluationType.KEYWORD_MATCH, keywords=["test"]
            ),
        )
        self.mock_service.update_rule.return_value = mock_rule

        response = self.client.put(f"/v1/rules/{rule_id}", json=payload)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()["rule_name"], "Updated Name")

    def test_update_rule_not_found(self) -> None:
        """Test updating a non-existent rule returns 404."""
        self.mock_service.update_rule.return_value = None
        response = self.client.put(
            "/v1/rules/missing", json={"rule_name": "New"}
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_delete_rule_success(self) -> None:
        """Test deleting a rule successfully."""
        rule_id = "rule_123"
        self.mock_service.delete_rule.return_value = True

        response = self.client.delete(f"/v1/rules/{rule_id}")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.mock_service.delete_rule.assert_called_once_with(rule_id)

    def test_delete_rule_not_found(self) -> None:
        """Test deleting a non-existent rule returns 404."""
        self.mock_service.delete_rule.return_value = False
        response = self.client.delete("/v1/rules/missing")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


if __name__ == "__main__":
    unittest.main()
