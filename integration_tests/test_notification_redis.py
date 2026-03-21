"""Integration test for notification deduping logic against Redis."""

import base64
import os
import uuid

import pytest
import requests

from backend.pipeline.common.storage.redis_service import RedisService
from backend.pipeline.notification.notification_deduplication import (
    NotificationDeduplication,
)
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)
from integration_tests.test_utils import assert_eventually


@pytest.fixture
def redis_service() -> RedisService:
    """Fixture to instantiate the Redis service."""
    return RedisService()


@pytest.fixture
def dedup(redis_service: RedisService) -> NotificationDeduplication:
    """Fixture to instantiate the NotificationDeduplication class."""
    return NotificationDeduplication(cache=redis_service)


def test_notification_sent(
    redis_service: RedisService, dedup: NotificationDeduplication
) -> None:
    """
    Tests when a message is sent to the notification service, which
    will persist in Redis and pass the message to the mock server.

    We only test this path once, since it will be difficult to test
    that duplicates are filtered out. The test will become brittle
    if we add in an arbitrary timeout to ensure that event doesn't
    happen. We will rely on unit tests to cover this path.
    """
    test_notification_id = f"test-integration-notification-{uuid.uuid4()}"
    test_message = EvaluatedTranscribedAudio(
        transmission_id=test_notification_id,
        transcript="liar liar pants on fire",
        feed_id="test-feed",
        evaluation_decisions=["rule1", "rule2"],
    )
    encoded_data = base64.b64encode(test_message.SerializeToString()).decode("utf-8")
    payload = {"messages": [{"data": encoded_data}]}
    pubsub_host = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
    pubsub_url = (
        f"http://{pubsub_host}/v1/projects/local-project/"
        "topics/rules-evaluation-results-topic:publish"
    )

    try:
        # Publish the message to the topic
        response = requests.post(pubsub_url, json=payload, timeout=10)
        assert response.status_code == 200, f"Failed to publish: {response.text}"

        # Wait for the notification service to process the message and
        # write to Redis. Expect the notification service to process the
        # message and persist for duplicates in Redis.
        assert_eventually(
            lambda: redis_service.client.get(test_notification_id) is not None,
            timeout_sec=5.0,
            error_msg=(
                "Notification service failed to process the message "
                "(or failed to set Redis cache)"
            ),
        )

        # Check that the mock server received the notification
        mock_host = os.environ.get("MOCK_SERVER_HOST", "localhost:8082")
        mock_requests_url = f"http://{mock_host}/"

        def mock_server_received() -> bool:
            try:
                mock_resp = requests.get(mock_requests_url, timeout=5)
            except requests.exceptions.RequestException:
                return False
            else:
                if mock_resp.status_code == 200:
                    requests_data = mock_resp.json()
                    filtered = list(
                        filter(
                            lambda r: r.get("transmissionId") == test_notification_id,
                            requests_data,
                        )
                    )
                    return len(filtered) == 1
                return False

        assert_eventually(
            mock_server_received,
            timeout_sec=10.0,
            error_msg="Mock server did not receive the first notification",
        )

    finally:
        # Clean up the key after the test completes
        redis_service.client.delete(test_notification_id)
