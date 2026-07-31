import unittest
from datetime import UTC, datetime
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
    Tag,
)
from backend.pipeline.storage.audio_segment_store import PaginatedAudioSegments
from backend.services.audio_segments.models import (
    AudioClassification,
    AudioSegment,
    TranscriptAnnotation,
    TranscriptAnnotationData,
)

from ..main import app, get_audio_segment_store, get_rules_service


async def skip_auth() -> dict[str, str]:
    """Mock dependency to bypass authentication in tests."""
    return {"sub": "test@example.com", "email": "test@example.com"}


class TestRulesAPI(unittest.TestCase):
    def setUp(self) -> None:
        """Set up a test client and dependency overrides before each test."""
        self.mock_service = AsyncMock()
        self.mock_audio_store = AsyncMock()
        app.dependency_overrides[verify_oidc_token] = skip_auth
        app.dependency_overrides[get_rules_service] = lambda: self.mock_service
        app.dependency_overrides[get_audio_segment_store] = lambda: (
            self.mock_audio_store
        )
        self.client = TestClient(
            app, headers={"X-WD-Actor-Id": "user:google:test@example.com"}
        )

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

    def test_create_rule_with_tags(self) -> None:
        """Test that tags are accepted on create and echoed in the response."""
        payload = {
            "rule_name": "Tagged Rule",
            "is_active": True,
            "scope": {"level": "GLOBAL"},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "keywords": ["flood"],
            },
            "tags": [{"key": "geo_event_type", "value": "flooding"}],
        }
        mock_rule = Rule(
            rule_id="rule_456",
            rule_name="Tagged Rule",
            is_active=True,
            scope=Scope(level=ScopeLevel.GLOBAL),
            conditions=KeywordConditions(
                evaluation_type=EvaluationType.KEYWORD_MATCH,
                keywords=["flood"],
            ),
            tags=[Tag(key="geo_event_type", value="flooding")],
            metadata=RuleMetadata(created_by="test@example.com"),
        )
        self.mock_service.create_rule.return_value = mock_rule

        response = self.client.post("/v1/rules", json=payload)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(
            response.json()["tags"],
            [{"key": "geo_event_type", "value": "flooding"}],
        )
        rule_in = self.mock_service.create_rule.call_args.args[0]
        self.assertEqual(
            rule_in.tags, [Tag(key="geo_event_type", value="flooding")]
        )

    def test_create_rule_with_invalid_tags(self) -> None:
        """Test that malformed tags are rejected with a validation error."""
        payload = {
            "rule_name": "Bad Tags",
            "is_active": True,
            "scope": {"level": "GLOBAL"},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "keywords": ["test"],
            },
            "tags": [{"key": "missing_value"}],
        }

        response = self.client.post("/v1/rules", json=payload)

        self.assertEqual(
            response.status_code, status.HTTP_422_UNPROCESSABLE_CONTENT
        )
        self.mock_service.create_rule.assert_not_called()

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
        self.mock_service.delete_rule.assert_called_once_with(
            rule_id, actor_id="user:google:test@example.com"
        )

    def test_delete_rule_not_found(self) -> None:
        """Test deleting a non-existent rule returns 404."""
        self.mock_service.delete_rule.return_value = False
        response = self.client.delete("/v1/rules/missing")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_dry_run_rule(self) -> None:
        """Test testing a prospective rule via dry-run."""
        payload = {
            "rule": {
                "rule_name": "Prospective Rule",
                "is_active": True,
                "scope": {"level": "GLOBAL"},
                "conditions": {
                    "evaluation_type": "KEYWORD_MATCH",
                    "keywords": ["fire"],
                },
            },
        }

        seg1 = AudioSegment(
            id="12345678-1234-5678-1234-567812345678",
            feed_id="87654321-4321-8765-4321-876543210987",
            classification=AudioClassification.SPEECH,
            start_timestamp=datetime(2023, 1, 1, tzinfo=UTC),
            end_timestamp=datetime(2023, 1, 1, 0, 0, 10, tzinfo=UTC),
            missing_prior_context=False,
            missing_post_context=False,
            created_at=datetime(2023, 1, 1, tzinfo=UTC),
            source_audio_uris=["gs://foo"],
            annotations=[
                TranscriptAnnotation(
                    audio_segment_id="12345678-1234-5678-1234-567812345678",
                    created_at=datetime(2023, 1, 1, tzinfo=UTC),
                    data=TranscriptAnnotationData(
                        text="There is a fire spreading.", errors=[]
                    ),
                )
            ],
        )
        seg2 = AudioSegment(
            id="12345678-1234-5678-1234-567812345679",
            feed_id="87654321-4321-8765-4321-876543210987",
            classification=AudioClassification.SPEECH,
            start_timestamp=datetime(2023, 1, 1, 0, 0, 10, tzinfo=UTC),
            end_timestamp=datetime(2023, 1, 1, 0, 0, 20, tzinfo=UTC),
            missing_prior_context=False,
            missing_post_context=False,
            created_at=datetime(2023, 1, 1, 0, 0, 10, tzinfo=UTC),
            source_audio_uris=["gs://foo"],
            annotations=[
                TranscriptAnnotation(
                    audio_segment_id="12345678-1234-5678-1234-567812345679",
                    created_at=datetime(2023, 1, 1, 0, 0, 10, tzinfo=UTC),
                    data=TranscriptAnnotationData(
                        text="Nothing to see here.", errors=[]
                    ),
                )
            ],
        )
        self.mock_audio_store.list_audio_segments.return_value = (
            PaginatedAudioSegments(segments=[seg1, seg2], next_token=None)
        )

        response = self.client.post("/v1/rules/dry-run", json=payload)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.json()

        self.assertEqual(data["hit_count"], 1)
        self.assertEqual(data["total_evaluated"], 2)
        self.assertEqual(len(data["examples"]), 1)

        example = data["examples"][0]
        self.assertEqual(
            example["audio_segment_id"], "12345678-1234-5678-1234-567812345678"
        )
        self.assertEqual(example["text"], "There is a fire spreading.")
        self.assertEqual(len(example["matched_spans"]), 1)
        self.assertEqual(example["matched_spans"][0]["matched_text"], "fire")


if __name__ == "__main__":
    unittest.main()
