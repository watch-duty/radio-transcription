import time
import requests
import os
import sys

PUBSUB_EMULATOR_HOST = os.environ.get("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8085")
PROJECT_ID = "local-project"
INPUT_TOPIC = "evaluation-input-topic"
OUTPUT_TOPIC = "evaluation-output-topic"
EVALUATION_ENDPOINT = "http://evaluation:8080/"
NOTIFICATION_ENDPOINT = "http://notification:8081/"

def wait_for_emulator():
    print("Waiting for Pub/Sub emulator...", flush=True)
    for _ in range(30):
        try:
            response = requests.get(f"http://{PUBSUB_EMULATOR_HOST}/")
            if response.status_code == 200:
                print("Pub/Sub emulator is ready.", flush=True)
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)
    print("Timed out waiting for Pub/Sub emulator.", flush=True)
    sys.exit(1)

def create_topic(topic_id):
    url = f"http://{PUBSUB_EMULATOR_HOST}/v1/projects/{PROJECT_ID}/topics/{topic_id}"
    response = requests.put(url, json={})
    if response.status_code in (200, 409): # 200 Created, 409 Already exists
        print(f"Topic '{topic_id}' ready.", flush=True)
    else:
        print(f"Failed to create topic '{topic_id}': {response.text}", flush=True)

def create_push_subscription(subscription_id, topic_id, push_endpoint):
    url = f"http://{PUBSUB_EMULATOR_HOST}/v1/projects/{PROJECT_ID}/subscriptions/{subscription_id}"
    payload = {
        "topic": f"projects/{PROJECT_ID}/topics/{topic_id}",
        "pushConfig": {
            "pushEndpoint": push_endpoint
        }
    }
    response = requests.put(url, json=payload)
    if response.status_code in (200, 409):
        print(f"Subscription '{subscription_id}' ready, pushing to {push_endpoint}.", flush=True)
    else:
        print(f"Failed to create subscription '{subscription_id}': {response.text}", flush=True)

if __name__ == "__main__":
    wait_for_emulator()
    
    # Create evaluation input topic and subscription
    create_topic(INPUT_TOPIC)
    create_push_subscription("evaluation-sub", INPUT_TOPIC, EVALUATION_ENDPOINT)
    
    # Create evaluation output topic and notification subscription
    create_topic(OUTPUT_TOPIC)
    create_push_subscription("notification-sub", OUTPUT_TOPIC, NOTIFICATION_ENDPOINT)
    
    print("Pub/Sub initialization complete.", flush=True)
