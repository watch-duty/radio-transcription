"""Integration test for notification deduping logic against Redis."""

import base64
import os
import uuid
from collections.abc import Generator

import pytest
import requests

from backend.pipeline.common.storage.redis_service import RedisService
from backend.pipeline.notification.notification_deduplication import (
    NotificationDeduplication,
)
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)
from integration_tests.utils import assert_eventually

FEEDS_API_HOST = os.environ.get("FEEDS_API_HOST", "localhost:8089")


@pytest.fixture
def redis_service() -> RedisService:
    """Fixture to instantiate the Redis service."""
    return RedisService()


@pytest.fixture
def dedup(redis_service: RedisService) -> NotificationDeduplication:
    """Fixture to instantiate the NotificationDeduplication class."""
    return NotificationDeduplication(cache=redis_service)


@pytest.fixture
def test_feed_with_tags() -> Generator[str]:
    """Fixture to create a temporary feed with tags for testing."""
    feed_name = f"integration-test-feed-{uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "source_type": "bcfy_feeds",
        "source_feed_id": str(uuid.uuid4().fields[0]),
        "tags": [{"key": "env", "value": "prod"}],
    }

    url = f"http://{FEEDS_API_HOST}/v1/feeds"
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()

    feed_id = response.json().get("id", "")
    assert feed_id != "", "Feed ID not returned by API"

    try:
        yield feed_id
    finally:
        # Deactivate feed via API
        del_url = f"http://{FEEDS_API_HOST}/v1/feeds/{feed_id}/deactivate"
        del_response = requests.post(del_url, timeout=10)
        del_response.raise_for_status()


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
        segment_id=test_notification_id,
        transcript="liar liar pants on fire",
        feed_id="test-feed",
        evaluation_decisions=["rule1", "rule2"],
    )
    encoded_data = base64.b64encode(test_message.SerializeToString()).decode(
        "utf-8"
    )
    payload = {"messages": [{"data": encoded_data}]}
    pubsub_host = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
    pubsub_url = (
        f"http://{pubsub_host}/v1/projects/local-project/"
        "topics/rules-evaluation-results-topic:publish"
    )

    try:
        # Publish the message to the topic
        response = requests.post(pubsub_url, json=payload, timeout=10)
        assert response.status_code == 200, (
            f"Failed to publish: {response.text}"
        )

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
                            lambda r: (
                                r.get("transmissionId") == test_notification_id
                            ),
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


def test_notification_sent_with_tags(
    redis_service: RedisService, test_feed_with_tags: str
) -> None:
    """Tests that a notification is sent with tags fetched from feeds API."""
    test_notification_id = f"test-integration-tags-{uuid.uuid4()}"
    test_message = EvaluatedTranscribedAudio(
        segment_id=test_notification_id,
        transcript="liar liar pants on fire",
        feed_id=test_feed_with_tags,
        evaluation_decisions=["rule1", "rule2"],
    )
    encoded_data = base64.b64encode(test_message.SerializeToString()).decode(
        "utf-8"
    )
    payload = {"messages": [{"data": encoded_data}]}
    pubsub_host = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
    pubsub_url = (
        f"http://{pubsub_host}/v1/projects/local-project/"
        "topics/rules-evaluation-results-topic:publish"
    )

    try:
        # Publish the message to the topic
        response = requests.post(pubsub_url, json=payload, timeout=10)
        assert response.status_code == 200, (
            f"Failed to publish: {response.text}"
        )

        # Wait for the notification service to process the message and
        # write to Redis.
        assert_eventually(
            lambda: redis_service.client.get(test_notification_id) is not None,
            timeout_sec=5.0,
            error_msg="Notification service failed to process the message",
        )

        # Check that the mock server received the notification with tags
        mock_host = os.environ.get("MOCK_SERVER_HOST", "localhost:8082")
        mock_requests_url = f"http://{mock_host}/"

        def mock_server_received_with_tags() -> bool:
            try:
                mock_resp = requests.get(mock_requests_url, timeout=5)
            except requests.exceptions.RequestException:
                return False
            else:
                if mock_resp.status_code == 200:
                    requests_data = mock_resp.json()
                    filtered = list(
                        filter(
                            lambda r: (
                                r.get("transmissionId") == test_notification_id
                            ),
                            requests_data,
                        )
                    )
                    if len(filtered) == 1:
                        notification = filtered[0]
                        tags = notification.get("tags")
                        return tags == [{"key": "env", "value": "prod"}]
                    return False
                return False

        assert_eventually(
            mock_server_received_with_tags,
            timeout_sec=10.0,
            error_msg="Mock server did not receive the notification with expected tags",
        )

    finally:
        # Clean up the key after the test completes
        redis_service.client.delete(test_notification_id)
