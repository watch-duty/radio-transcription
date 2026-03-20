import base64
from unittest import TestCase, main, mock

from cloudevents.http import CloudEvent

from backend.pipeline.schema_types.alert_notification_pb2 import AlertNotification
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)

with mock.patch("google.cloud.logging.Client") as mock_client:
    from backend.pipeline.notification.send_notification import send_notification


class TestSendNotification(TestCase):
    @mock.patch("backend.pipeline.notification.send_notification.deduplication")
    @mock.patch("backend.pipeline.notification.send_notification.request_handler")
    def test_send_notification(
        self, mock_request_handler: mock.Mock, mock_dedupe: mock.Mock
    ) -> None:
        mock_dedupe.process_notification.return_value = True

        mock_response = mock.MagicMock()
        mock_post.return_value = mock_response
        mock_dedupe.process_notification.return_value = False

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

        mock_request_handler.send_notification.assert_called_once_with(
            AlertNotification(transcript="This is a test!", transmission_id="1234")
        )

    @mock.patch("backend.pipeline.notification.send_notification.deduplication")
    @mock.patch("backend.pipeline.notification.send_notification.request_handler")
    def test_duplicate_message(
        self, mock_request_handler: mock.Mock, mock_dedupe: mock.Mock
    ) -> None:
        # Setting this to False indicates a duplicate.
        mock_dedupe.process_notification.return_value = False

        mock_response = mock.MagicMock()
        mock_post.return_value = mock_response
        # Setting this to True indicates a duplicate.
        mock_dedupe.process_notification.return_value = True

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

        mock_request_handler.send_notification.assert_not_called()


if __name__ == "__main__":
    main()
