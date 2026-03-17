import base64
import os
from unittest import TestCase, main, mock

import requests
from cloudevents.http import CloudEvent

from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)

with mock.patch("google.cloud.logging.Client") as mock_client:
    with mock.patch.dict(
        os.environ,
        {
            "NOTIFICATION_ENDPOINT": "https://api.example.com/mock",
            "NOTIFICATION_ENDPOINT_API_KEY": "12345",
        },
    ):
        from backend.pipeline.notification.send_notification import send_notification


class TestSendNotification(TestCase):
    @mock.patch(
        "backend.pipeline.notification.send_notification.notification_deduplication"
    )
    @mock.patch("backend.pipeline.notification.send_notification.requests.post")
    def test_send_notification(
        self, mock_post: mock.Mock, mock_dedupe: mock.Mock
    ) -> None:
        mock_dedupe.process_notification.return_value = False

        mock_response = mock.MagicMock()
        mock_post.return_value = mock_response

        evaluated_payload = EvaluatedTranscribedAudio(
            transcript="This is a test!", transmission_id="1234"
        )
        raw_data = base64.b64encode(evaluated_payload.SerializeToString())
        event_data = {"message": {"data": raw_data, "messageId": "1234"}}

        attributes = {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/my-project/topics/my-topic",
        }

        cloud_event = CloudEvent(attributes, event_data)
        result = send_notification(cloud_event)
        self.assertIsNone(result)

        mock_dedupe.process_notification.assert_called_with("1234")

        expected_url = "https://api.example.com/mock"
        expected_headers = {"Content-Type": "application/json", "X-Api-Key": "12345"}
        mock_post.assert_called_once_with(
            expected_url,
            data='{"transmissionId": "1234", "transcript": "This is a test!"}',
            headers=expected_headers,
            timeout=5,
        )

    @mock.patch(
        "backend.pipeline.notification.send_notification.notification_deduplication"
    )
    @mock.patch("backend.pipeline.notification.send_notification.requests.post")
    def test_duplicate_message(
        self, mock_post: mock.Mock, mock_dedupe: mock.Mock
    ) -> None:
        # Setting this to True indicates a duplicate.
        mock_dedupe.process_notification.return_value = True

        mock_response = mock.MagicMock()
        mock_post.return_value = mock_response

        evaluated_payload = EvaluatedTranscribedAudio(
            transcript="This is a test!", transmission_id="1234"
        )
        raw_data = base64.b64encode(evaluated_payload.SerializeToString())
        event_data = {"message": {"data": raw_data, "messageId": "1234"}}

        attributes = {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/my-project/topics/my-topic",
        }

        cloud_event = CloudEvent(attributes, event_data)
        result = send_notification(cloud_event)
        self.assertIsNone(result)

        mock_dedupe.process_notification.assert_called_with("1234")

        mock_post.assert_not_called()

    @mock.patch(
        "backend.pipeline.notification.send_notification.notification_deduplication"
    )
    @mock.patch("backend.pipeline.notification.send_notification.requests.post")
    def test_post_error(self, mock_post: mock.Mock, mock_dedupe: mock.Mock) -> None:
        mock_dedupe.process_notification.return_value = False

        mock_response = mock.MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "Error"
        )
        mock_post.return_value = mock_response

        evaluated_payload = EvaluatedTranscribedAudio(transcript="This is a test!")
        raw_data = base64.b64encode(evaluated_payload.SerializeToString())
        event_data = {"message": {"data": raw_data, "messageId": "1234"}}

        attributes = {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/my-project/topics/my-topic",
        }

        cloud_event = CloudEvent(attributes, event_data)
        self.assertRaises(
            requests.exceptions.RequestException, send_notification, cloud_event
        )


if __name__ == "__main__":
    main()
