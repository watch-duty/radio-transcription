import base64
import json
import os
import requests
from google.protobuf.json_format import ParseDict

from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio

# Construct a simple TranscribedAudio payload
transcribed_audio = TranscribedAudio(
    transmission_id="test-trans-123",
    transcript="evacuation needed immediately",
    source_chunk_ids=["chunk1", "chunk2"],
    feed_id="test-feed",
    start_timestamp={"seconds": 0, "nanos": 0},
    end_timestamp={"seconds": 10, "nanos": 0},
)

# Serialize to string
encoded_data = transcribed_audio.SerializeToString()
base64_data = base64.b64encode(encoded_data).decode('utf-8')

# Construct the Pub/Sub REST payload
payload = {
    "messages": [
        {
            "data": base64_data
        }
    ]
}

PUBSUB_EMULATOR_HOST = os.environ.get("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8085")
PROJECT_ID = "local-project"
INPUT_TOPIC = "evaluation-input-topic"

url = f"http://{PUBSUB_EMULATOR_HOST}/v1/projects/{PROJECT_ID}/topics/{INPUT_TOPIC}:publish"

print(f"Publishing to {url}...", flush=True)
response = requests.post(url, json=payload)
print(f"Status: {response.status_code}", flush=True)
print(f"Response: {response.text}", flush=True)
