"""Publishes test messages and verifies the end-to-end Rules Evaluation flow."""

import base64
import os
import time
import uuid

import requests

from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio
from integration_tests.feed_utils import create_test_bcfy_feed  # noqa: F401
from integration_tests.test_utils import (
    get_audio_segments_api_url,
    verify_audio_segments_via_api,
    verify_notification_received,
)

PUBSUB_EMULATOR_HOST = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "local-project")
RULES_API_HOST = os.environ.get("RULES_API_HOST", "localhost:8086")
TRANSCRIPTION_TOPIC = os.environ.get(
    "TRANSCRIPTION_TOPIC", "transcription-text-topic"
)
MOCK_SERVER_HOST = os.environ.get("MOCK_SERVER_HOST", "localhost:8082")
FEEDS_API_HOST = os.environ.get("FEEDS_API_HOST", "localhost:8089")


def create_test_rule(test_keyword: str) -> str:
    """Creates a temporary rule for testing with a specific keyword."""
    rule_payload = {
        "rule_name": f"Integration Test Rule - {test_keyword}",
        "description": f"Triggers on {test_keyword} mentions.",
        "is_active": True,
        "scope": {"level": "GLOBAL"},
        "conditions": {
            "evaluation_type": "KEYWORD_MATCH",
            "operator": "ANY",
            "keywords": [test_keyword],
            "case_sensitive": False,
        },
    }

    url = f"http://{RULES_API_HOST}/v1/rules"
    response = requests.post(url, json=rule_payload, timeout=10)
    response.raise_for_status()

    rule_id = response.json().get("rule_id", "")
    assert rule_id != "", "Rule ID not returned by API"
    return rule_id


def publish_test_message(
    segment_id: str, transcript: str, feed_id: str
) -> None:
    """Publishes a test message to the transcription topic."""
    message = TranscribedAudio(
        segment_id=segment_id,
        transcript=transcript,
        source_audio_uris=["chunk1", "chunk2"],
        feed_id=feed_id,
        start_timestamp={"seconds": int(time.time()), "nanos": 0},
        end_timestamp={"seconds": int(time.time()) + 10, "nanos": 0},
    )

    payload = {
        "messages": [
            {
                "data": base64.b64encode(message.SerializeToString()).decode(
                    "utf-8"
                )
            }
        ]
    }

    url = f"http://{PUBSUB_EMULATOR_HOST}/v1/{TRANSCRIPTION_TOPIC}:publish"

    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()


def create_test_audio_segment(segment_id: str, feed_id: str) -> None:
    """Creates a dummy audio segment in the Audio Segments API."""
    segment_data = {
        "id": segment_id,
        "feed_id": feed_id,
        "classification": "SPEECH",
        "start_timestamp": "2026-01-01T10:00:00Z",
        "end_timestamp": "2026-01-01T10:01:00Z",
        "missing_prior_context": False,
        "missing_post_context": False,
        "source_audio_uris": ["gs://bucket/audio1.ogg"],
    }
    url = f"{get_audio_segments_api_url()}/audio_segments"
    response = requests.post(url, json=segment_data, timeout=10)
    response.raise_for_status()


def test_rules_creation_evaluation_publish(
    test_bcfy_feed: tuple[str, str],
) -> None:
    test_uuid = str(uuid.uuid4())[:8]
    unique_keyword = f"evacuation-{test_uuid}"
    unique_trans_id = str(uuid.uuid4())
    unique_transcript = f"Attention: {unique_keyword} is required for Sector 7."

    rule_id = create_test_rule(unique_keyword)
    feed_id, _ = test_bcfy_feed
    create_test_audio_segment(unique_trans_id, feed_id)
    publish_test_message(unique_trans_id, unique_transcript, feed_id)

    # 1. Check Notification (via Mock Server)
    verify_notification_received(unique_trans_id, timeout_sec=70.0)

    # 2. Check Transcript Annotation (via Audio Segments API)
    verify_audio_segments_via_api(
        feed_id=feed_id,
        matcher=lambda segment: (
            segment["id"] == unique_trans_id
            and any(
                ann["type"] == "EVALUATION"
                and rule_id in ann.get("data", {}).get("decisions", [])
                for ann in segment.get("annotations", [])
            )
        ),
        timeout_sec=70.0,
    )
