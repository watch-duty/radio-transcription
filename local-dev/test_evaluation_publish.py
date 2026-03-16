import base64
import json
import os
import requests
from google.protobuf.json_format import ParseDict

from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio

with open("local_env.json", "r") as f:
    env_vars = json.load(f)

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

# Update this to test out different messages
message_string = sample_transcription_message_with_event.SerializeToString()

payload = {
    "messages": [
        {
            "data": base64.b64encode(message_string).decode('utf-8')
        }
    ]
}

PUBSUB_EMULATOR_HOST = env_vars["PUBSUB_EMULATOR_HOST"]
PROJECT_ID = env_vars["PROJECT_ID"]
INPUT_TOPIC = env_vars["TRANSCRIPTION_TOPIC"]

url = f"http://{PUBSUB_EMULATOR_HOST}/v1/projects/{PROJECT_ID}/topics/{INPUT_TOPIC}:publish"

print(f"Publishing to {url}:\n {message_string}")
response = requests.post(url, json=payload)
print(f"Status: {response.status_code}")
print(f"Response: {response.text}")
