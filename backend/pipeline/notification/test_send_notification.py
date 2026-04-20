import base64
import importlib
import os
import sys
from unittest import TestCase, main, mock

from cloudevents.http import CloudEvent

from backend.pipeline.schema_types.alert_notification_pb2 import (
    AlertNotification,
)
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)

with (
    mock.patch("google.cloud.logging.Client"),
    mock.patch.dict(os.environ, {"APP_URL": "https://app.example.com"}),
):
    from backend.pipeline.notification.send_notification import (
        convert_to_notification,
        send_notification,
    )


class TestSendNotification(TestCase):
    def test_missing_app_url_raises_on_import(self) -> None:
        module_name = "backend.pipeline.notification.send_notification"
        original_module = sys.modules.get(module_name)

        try:
            with mock.patch.dict(os.environ, {}, clear=True):
                sys.modules.pop(module_name, None)
                with self.assertRaisesRegex(
                    ValueError,
                    "APP_URL environment variable is not set.",
                ):
                    importlib.import_module(module_name)
        finally:
            if original_module is not None:
                sys.modules[module_name] = original_module

    @mock.patch("backend.pipeline.notification.send_notification.deduplication")
    @mock.patch(
        "backend.pipeline.notification.send_notification.request_handler"
    )
    def test_send_notification(
        self, mock_request_handler: mock.Mock, mock_dedupe: mock.Mock
    ) -> None:
        mock_dedupe.process_notification.return_value = True

        evaluated_payload = EvaluatedTranscribedAudio(
            transcript="This is a test!",
            transmission_id="1234",
            source_audio_uris=["gs://foo/bar.flac"],
        )
        evaluated_payload.start_audio_offset.seconds = 10
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

        expected_notification = AlertNotification(
            transcript="This is a test!",
            transmission_id="1234",
            source_audio_uris=["gs://foo/bar.flac"],
            app_url="https://app.example.com?feedId=&transmissionId=1234&duration=5",
        )
        expected_notification.start_audio_offset.seconds = 10
        mock_request_handler.send_notification.assert_called_once_with(
            expected_notification
        )

    @mock.patch("backend.pipeline.notification.send_notification.deduplication")
    @mock.patch(
        "backend.pipeline.notification.send_notification.request_handler"
    )
    def test_duplicate_message(
        self, mock_request_handler: mock.Mock, mock_dedupe: mock.Mock
    ) -> None:
        # Setting this to False indicates a duplicate.
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

        mock_request_handler.send_notification.assert_not_called()

    def test_convert_to_notification_encodes_epoch_timestamp(self) -> None:
        evaluated_payload = EvaluatedTranscribedAudio(
            feed_id="feed-1",
            transmission_id="tx-1",
        )
        evaluated_payload.start_timestamp.seconds = 1776280988
        evaluated_payload.start_timestamp.nanos = 990000000

        notification = convert_to_notification(evaluated_payload)

        self.assertEqual(
            notification.app_url,
            "https://app.example.com?feedId=feed-1&transmissionId=tx-1"
            "&timestamp=1776280988990&duration=5",
        )


if __name__ == "__main__":
    main()
