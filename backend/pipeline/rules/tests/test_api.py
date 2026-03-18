import unittest

from fastapi.testclient import TestClient

from ..main import app


class TestRulesAPI(unittest.TestCase):
    def setUp(self) -> None:
        """Set up a test client before each test."""
        self.client = TestClient(app)

    def test_create_keyword_rule(self) -> None:
        """Test creating a rule with keyword-based conditions."""
        payload = {
            "rule_name": "Structure Fire Dispatch",
            "description": "Triggers on standard structure fire terminology.",
            "is_active": True,
            "scope": {
                "level": "FEED_SPECIFIC",
                "target_feeds": ["feed_calfire_shasta"],
            },
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ANY",
                "keywords": ["structure fire", "working fire"],
                "case_sensitive": False,
            },
        }
        response = self.client.post("/v1/rules", json=payload)
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data["rule_name"], "Structure Fire Dispatch")
        self.assertTrue(data["rule_id"].startswith("rule_"))
        self.assertEqual(data["conditions"]["evaluation_type"], "KEYWORD_MATCH")

    def test_create_regex_rule(self) -> None:
        """Test creating a rule with regex-based conditions."""
        payload = {
            "rule_name": "Evacuation Order Mention",
            "description": "Catches any mention of evacuation zones or orders globally.",
            "is_active": True,
            "scope": {"level": "GLOBAL"},
            "conditions": {
                "evaluation_type": "REGEX_MATCH",
                "expression": "evacuation (order|warning)",
                "flags": "i",
            },
        }
        response = self.client.post("/v1/rules", json=payload)
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data["rule_name"], "Evacuation Order Mention")
        self.assertEqual(data["conditions"]["expression"], "evacuation (order|warning)")

    def test_get_rule(self) -> None:
        """Test retrieving a single, specific rule by its ID."""
        # First, create a rule to ensure one exists
        payload = {
            "rule_name": "Test Rule for Get",
            "scope": {"level": "GLOBAL"},
            "conditions": {"evaluation_type": "KEYWORD_MATCH", "keywords": ["test"]},
        }
        create_response = self.client.post("/v1/rules", json=payload)
        rule_id = create_response.json()["rule_id"]

        # Now, fetch it
        get_response = self.client.get(f"/v1/rules/{rule_id}")
        self.assertEqual(get_response.status_code, 200)
        data = get_response.json()
        self.assertEqual(data["rule_id"], rule_id)
        self.assertEqual(data["rule_name"], "Test Rule for Get")

    def test_list_rules(self) -> None:
        """Test listing all rules, ensuring it returns a list."""
        # Create a rule to make sure the list isn't empty
        self.client.post(
            "/v1/rules",
            json={
                "rule_name": "List Test",
                "scope": {"level": "GLOBAL"},
                "conditions": {"evaluation_type": "KEYWORD_MATCH", "keywords": ["a"]},
            },
        )

        response = self.client.get("/v1/rules")
        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response.json(), list)
        self.assertGreater(len(response.json()), 0)

    def test_update_rule(self) -> None:
        """Test updating an existing rule's name and description."""
        # Create a rule
        payload = {
            "rule_name": "Original Name",
            "scope": {"level": "GLOBAL"},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "keywords": ["original"],
            },
        }
        rule_id = self.client.post("/v1/rules", json=payload).json()["rule_id"]

        # Update it
        update_payload = {
            "rule_name": "Updated Name",
            "description": "This rule has been updated.",
        }
        response = self.client.put(f"/v1/rules/{rule_id}", json=update_payload)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["rule_name"], "Updated Name")
        self.assertEqual(data["description"], "This rule has been updated.")

    def test_delete_rule(self) -> None:
        """Test deleting a rule and ensuring it's no longer retrievable."""
        # Create a rule
        rule_id = self.client.post(
            "/v1/rules",
            json={
                "rule_name": "To Be Deleted",
                "scope": {"level": "GLOBAL"},
                "conditions": {
                    "evaluation_type": "KEYWORD_MATCH",
                    "keywords": ["delete"],
                },
            },
        ).json()["rule_id"]

        # Delete it
        delete_response = self.client.delete(f"/v1/rules/{rule_id}")
        self.assertEqual(delete_response.status_code, 204)

        # Verify it's gone
        get_response = self.client.get(f"/v1/rules/{rule_id}")
        self.assertEqual(get_response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
