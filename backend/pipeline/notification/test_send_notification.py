import base64
import importlib
import os
import sys
from unittest import TestCase, main, mock

from cloudevents.http import CloudEvent

from backend.pipeline.schema_types import EvaluationErrorType
from backend.pipeline.schema_types.alert_notification_pb2 import (
    AlertNotification,
)
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)

with (
    mock.patch("google.cloud.logging.Client"),
    mock.patch.dict(
        os.environ,
        {
            "APP_URL": "https://app.example.com",
            "FEEDS_API_URL": "http://feeds-api",
        },
    ),
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

    @mock.patch("requests.get")
    @mock.patch("backend.pipeline.notification.send_notification.deduplication")
    @mock.patch(
        "backend.pipeline.notification.send_notification.request_handler"
    )
    def test_send_notification(
        self,
        mock_request_handler: mock.Mock,
        mock_dedupe: mock.Mock,
        mock_get: mock.Mock,
    ) -> None:
        mock_dedupe.process_notification.return_value = True

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "tags": [{"key": "env", "value": "prod"}]
        }
        mock_get.return_value = mock_response

        evaluated_payload = EvaluatedTranscribedAudio(
            transcript="This is a test!",
            segment_id="1234",
            source_audio_uris=["gs://foo/bar.flac"],
            feed_name="asdf",
            external_id="ext-id",
        )
        evaluated_payload.start_audio_offset.seconds = 10
        evaluated_payload.end_audio_offset.seconds = 20
        evaluated_payload.start_timestamp.seconds = 1000
        evaluated_payload.end_timestamp.seconds = 2000
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
            segment_id="1234",
            source_audio_uris=["gs://foo/bar.flac"],
            feed_name="asdf",
            external_id="ext-id",
            app_url="https://app.example.com/transcripts?feedId=&segmentId=1234&timestamp=1000000",
        )
        expected_notification.start_audio_offset.seconds = 10
        expected_notification.end_audio_offset.seconds = 20
        expected_notification.start_timestamp.seconds = 1000
        expected_notification.end_timestamp.seconds = 2000

        t = expected_notification.tags.add()
        t.key = "env"
        t.value = "prod"

        mock_request_handler.send_notification.assert_called_once_with(
            expected_notification
        )

    @mock.patch("requests.get")
    @mock.patch("backend.pipeline.notification.send_notification.deduplication")
    @mock.patch(
        "backend.pipeline.notification.send_notification.request_handler"
    )
    def test_send_notification_with_errors(
        self,
        mock_request_handler: mock.Mock,
        mock_dedupe: mock.Mock,
        mock_get: mock.Mock,
    ) -> None:
        mock_dedupe.process_notification.return_value = True

        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"tags": []}
        mock_get.return_value = mock_response

        evaluated_payload = EvaluatedTranscribedAudio(
            transcript="This has errors!",
            segment_id="5678",
            source_audio_uris=["gs://foo/bar.flac"],
            feed_name="asdf",
            external_id="ext-id",
            errors=[EvaluationErrorType.ERROR_RULES_FETCH_FAILED],
        )
        evaluated_payload.start_audio_offset.seconds = 10
        evaluated_payload.end_audio_offset.seconds = 20
        evaluated_payload.start_timestamp.seconds = 1000
        evaluated_payload.end_timestamp.seconds = 2000
        raw_data = base64.b64encode(evaluated_payload.SerializeToString())
        event_data = {"message": {"data": raw_data, "messageId": "5678"}}

        attributes = {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/my-project/topics/my-topic",
        }

        cloud_event = CloudEvent(attributes, event_data)
        result = send_notification(cloud_event)
        self.assertIsNone(result)

        expected_notification = AlertNotification(
            transcript="This has errors!",
            segment_id="5678",
            source_audio_uris=["gs://foo/bar.flac"],
            feed_name="asdf",
            external_id="ext-id",
            app_url="https://app.example.com/transcripts?feedId=&segmentId=5678&timestamp=1000000",
            evaluation_errors=[EvaluationErrorType.ERROR_RULES_FETCH_FAILED],
        )
        expected_notification.start_audio_offset.seconds = 10
        expected_notification.end_audio_offset.seconds = 20
        expected_notification.start_timestamp.seconds = 1000
        expected_notification.end_timestamp.seconds = 2000

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
            transcript="This is a test!", segment_id="1234"
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

    @mock.patch("requests.get")
    @mock.patch(
        "backend.pipeline.notification.send_notification.with_tracer_context"
    )
    @mock.patch("backend.pipeline.notification.send_notification.deduplication")
    @mock.patch(
        "backend.pipeline.notification.send_notification.request_handler"
    )
    def test_send_notification_span(
        self,
        mock_request_handler: mock.Mock,
        mock_dedupe: mock.Mock,
        mock_with_tracer_context: mock.Mock,
        mock_get: mock.Mock,
    ) -> None:
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"tags": []}
        mock_get.return_value = mock_response
        mock_dedupe.process_notification.return_value = True

        evaluated_payload = EvaluatedTranscribedAudio(
            transcript="This is a test!",
            segment_id="1234",
        )
        raw_data = base64.b64encode(evaluated_payload.SerializeToString())
        event_data = {
            "message": {
                "data": raw_data,
                "messageId": "1234",
                "attributes": {"traceparent": "mock-traceparent"},
            }
        }

        attributes = {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/my-project/topics/my-topic",
        }

        cloud_event = CloudEvent(attributes, event_data)
        send_notification(cloud_event)

        mock_with_tracer_context.assert_called_once_with(
            "mock-traceparent",
            "send_notification",
            "backend.pipeline.notification.send_notification",
        )

    def test_convert_to_notification_encodes_epoch_timestamp(self) -> None:
        evaluated_payload = EvaluatedTranscribedAudio(
            feed_id="feed-1",
            segment_id="tx-1",
        )
        evaluated_payload.start_timestamp.seconds = 1776280988
        evaluated_payload.start_timestamp.nanos = 990000000
        evaluated_payload.end_timestamp.seconds = 1776281000
        evaluated_payload.start_audio_offset.seconds = 5
        evaluated_payload.end_audio_offset.seconds = 15

        notification = convert_to_notification(evaluated_payload, None)

        self.assertEqual(
            notification.app_url,
            "https://app.example.com/transcripts?feedId=feed-1&segmentId=tx-1"
            "&timestamp=1776280988990",
        )
        self.assertEqual(notification.start_timestamp.seconds, 1776280988)
        self.assertEqual(notification.start_timestamp.nanos, 990000000)
        self.assertEqual(notification.end_timestamp.seconds, 1776281000)
        self.assertEqual(notification.start_audio_offset.seconds, 5)
        self.assertEqual(notification.end_audio_offset.seconds, 15)


if __name__ == "__main__":
    main()
