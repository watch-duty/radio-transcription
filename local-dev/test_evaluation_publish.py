import base64
import json
import os
import requests
from google.protobuf import text_format

from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio

# For testing payloads sent to the Pub/Sub between the Transcription and Rules Evaluation services.
sample_transcription_message_with_event = TranscribedAudio(
    transmission_id="test-trans-123",
    transcript="evacuation needed immediately",
    source_chunk_ids=["chunk1", "chunk2"],
    feed_id="test-feed",
    start_timestamp={"seconds": 0, "nanos": 0},
    end_timestamp={"seconds": 10, "nanos": 0},
)
sample_transcription_message_no_event = TranscribedAudio(
    transmission_id="test-trans-123",
    transcript="parade at 123 main street",
    source_chunk_ids=["chunk1", "chunk2"],
    feed_id="test-feed",
    start_timestamp={"seconds": 0, "nanos": 0},
    end_timestamp={"seconds": 10, "nanos": 0},
)

# Update this sample to test out different messages
test_message = sample_transcription_message_no_event

payload = {
    "messages": [
        {
            "data": base64.b64encode(test_message.SerializeToString()).decode('utf-8')
        }
    ]
}

PUBSUB_EMULATOR_HOST = os.environ["PUBSUB_EMULATOR_HOST"]
PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]
INPUT_TOPIC = os.environ["TRANSCRIPTION_TOPIC"]

url = f"http://{PUBSUB_EMULATOR_HOST}/v1/projects/{PROJECT_ID}/topics/{INPUT_TOPIC}:publish"

print("====== INPUT ======")
print(f"Publishing to {url}:\n {text_format.MessageToString(test_message)}")
response = requests.post(url, json=payload)
print("====== OUTPUT ======")
print(f"Status: {response.status_code}")
print(f"Response: {response.text}")
