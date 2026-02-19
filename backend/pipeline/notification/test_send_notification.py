import base64
import json
import os
from unittest import TestCase, main, mock

import requests
from cloudevents.http import CloudEvent
from google.protobuf.json_format import MessageToJson

from backend.pipeline.notification.send_notification import send_notification
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)


class TestSendNotification(TestCase):
    @mock.patch.dict(os.environ, {"ENDPOINT": "https://api.example.com/mock"})
    @mock.patch("backend.pipeline.notification.send_notification.requests.post")
    def test_send_notification(self, mock_post: mock.Mock) -> None:
        mock_response = mock.MagicMock()
        mock_post.return_value = mock_response

        evaluated_payload = EvaluatedTranscribedAudio(transcript="This is a test!")
        payload = MessageToJson(evaluated_payload, indent=None)
        raw_data = base64.b64encode(payload.encode("utf-8")).decode("utf-8")
        event_data = {"message": {"data": raw_data, "messageId": "1234"}}

        attributes = {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/my-project/topics/my-topic",
        }

        cloud_event = CloudEvent(attributes, event_data)
        result = send_notification(cloud_event)
        self.assertIsNone(result)

        expected_url = "https://api.example.com/mock"
        expected_headers = {"Content-Type": "application/json"}
        mock_post.assert_called_once_with(
            expected_url,
            data='{"transcript": "This is a test!"}',
            headers=expected_headers,
            timeout=5,
        )

    @mock.patch.dict(os.environ, {"ENDPOINT": "https://api.example.com/mock"})
    @mock.patch("backend.pipeline.notification.send_notification.requests.post")
    def test_post_error(self, mock_post: mock.Mock) -> None:
        mock_response = mock.MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "Error"
        )
        mock_post.return_value = mock_response

        payload = json.dumps({"transcript": "This is a test!"})
        raw_data = base64.b64encode(payload.encode("utf-8")).decode("utf-8")
        event_data = {"message": {"data": raw_data, "messageId": "1234"}}

        attributes = {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/my-project/topics/my-topic",
        }

        cloud_event = CloudEvent(attributes, event_data)
        self.assertRaises(
            requests.exceptions.RequestException, send_notification, cloud_event
        )

    def test_missing_endpoint_env_var(self) -> None:
        payload = json.dumps({"transcript": "This is a test!"})
        raw_data = base64.b64encode(payload.encode("utf-8")).decode("utf-8")
        event_data = {"message": {"data": raw_data, "messageId": "1234"}}

        attributes = {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/my-project/topics/my-topic",
        }

        cloud_event = CloudEvent(attributes, event_data)
        self.assertRaises(SystemError, send_notification, cloud_event)


if __name__ == "__main__":
    main()
