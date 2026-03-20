from google.cloud import pubsub_v1

from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("automatic-hawk-481415-m9", "evaluated-audio-prod")

# Create a valid message
msg = EvaluatedTranscribedAudio()
msg.transmission_id = "test-session-123"
msg.transcript = "This is a test transcription containing CALIFORNIA and FIRE and POLICE to trigger rules."
# Add timestamps if needed
msg.start_timestamp.seconds = 1625097600
msg.end_timestamp.seconds = 1625097610

# Serialize to binary bits
binary_data = msg.SerializeToString()

# Publish
future = publisher.publish(topic_path, binary_data)
print(f"Published message ID: {future.result()}")
