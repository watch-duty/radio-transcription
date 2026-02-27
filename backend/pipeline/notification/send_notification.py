import base64
import datetime
import logging
import os

import functions_framework
import google.cloud.logging
import requests
from cloudevents.http.event import CloudEvent
from google.protobuf.json_format import MessageToJson

from backend.pipeline.schema_types.alert_notification_pb2 import AlertNotification
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)

client = google.cloud.logging.Client()
client.setup_logging()
logger = logging.getLogger(__name__)

POST_TIMEOUT_SECONDS = 5


def parse_cloud_event(cloud_event: CloudEvent) -> EvaluatedTranscribedAudio | None:
    pubsub_message = cloud_event.data.get("message", {})
    evaluated_transcribed_audio = EvaluatedTranscribedAudio()
    raw_data = pubsub_message.get("data", "")
    if raw_data:
        decoded_data = base64.b64decode(raw_data)
        evaluated_transcribed_audio.ParseFromString(decoded_data)
        return evaluated_transcribed_audio
    logger.warning("No data provided in CloudEvent")
    return None


def to_iso_string(timestamp: EvaluatedTranscribedAudio.Timestamp) -> str | None:
    if not timestamp or not timestamp.seconds or not timestamp.nanos:
        return None

    dt = datetime.datetime.fromtimestamp(
        timestamp.seconds + (timestamp.nanos / 1e9), tz=datetime.UTC
    )
    return dt.isoformat(timespec="microseconds")


def convert_to_notification(
    evaluated_transcribed_audio: EvaluatedTranscribedAudio,
) -> AlertNotification:
    return AlertNotification(
        file_path=evaluated_transcribed_audio.file_path,
        location=evaluated_transcribed_audio.location,
        feed=evaluated_transcribed_audio.feed,
        audio_id=evaluated_transcribed_audio.audio_id,
        start_timestamp=to_iso_string(evaluated_transcribed_audio.start_timestamp),
        end_timestamp=to_iso_string(evaluated_transcribed_audio.end_timestamp),
        transcript=evaluated_transcribed_audio.transcript,
        evaluation_decisions=evaluated_transcribed_audio.evaluation_decisions,
    )


@functions_framework.cloud_event
def send_notification(cloud_event: CloudEvent) -> None:
    ENDPOINT = os.environ.get("ENDPOINT")

    try:
        if not ENDPOINT:
            msg = "ENDPOINT environment variable not set."
            raise SystemError(msg)

        evaluated_transcribed_audio = parse_cloud_event(cloud_event)
        if not evaluated_transcribed_audio:
            return

        alert_notification = convert_to_notification(evaluated_transcribed_audio)
        request_data = MessageToJson(alert_notification, indent=None)
        logger.info(f"Sending payload: {request_data}")

        # Send POST request to endpoint.
        response = requests.post(
            ENDPOINT,
            data=request_data,
            headers={"Content-Type": "application/json", "X-Api-Key": os.environ.get("API_KEY")},
            timeout=POST_TIMEOUT_SECONDS,
        )

        # Raise an exception for bad status codes
        response.raise_for_status()

        logger.info(f"Message sent successfully! Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logger.exception(f"POST request failed: {e}")
        raise
