import time
import requests
import os
import sys
import json

with open("local_env.json", "r") as f:
    env_vars = json.load(f)

PUBSUB_EMULATOR_HOST = env_vars["PUBSUB_EMULATOR_HOST"]
PROJECT_ID = env_vars["PROJECT_ID"]
PUBSUB_ENDPOINT = f"http://{PUBSUB_EMULATOR_HOST}/v1/projects/{PROJECT_ID}"

# Startup loop for emulator
def wait_for_emulator():
    print("Waiting for Pub/Sub emulator...")
    for _ in range(30):
        try:
            response = requests.get(f"http://{PUBSUB_EMULATOR_HOST}/")
            if response.status_code == 200:
                print("Pub/Sub emulator is ready.")
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)
    print("Timed out waiting for Pub/Sub emulator.")
    sys.exit(1)

# Creates the topics for the pipeline. Ignores pre-existing topics. 
def create_topic(topic_id):
    url = f"{PUBSUB_ENDPOINT}/topics/{topic_id}"
    response = requests.put(url, json={})
    # 200 Created, 409 Already exists
    if response.status_code in (200, 409):
        print(f"Topic '{topic_id}' ready.")
    else:
        print(f"Failed to create topic '{topic_id}': {response.text}")

# Set up subscription from service to the given topic. Ignores pre-existing subscriptions. 
def create_push_subscription(subscription_id, topic_id, push_endpoint):
    url = f"{PUBSUB_ENDPOINT}/subscriptions/{subscription_id}"
    payload = {
        "topic": f"projects/{PROJECT_ID}/topics/{topic_id}",
        "pushConfig": {
            "pushEndpoint": push_endpoint
        }
    }
    response = requests.put(url, json=payload)
    if response.status_code in (200, 409):
        print(f"Subscription '{subscription_id}' ready, pushing to {push_endpoint}.")
    else:
        print(f"Failed to create subscription '{subscription_id}': {response.text}")

if __name__ == "__main__":
    wait_for_emulator()
    # WIP endpoints

    # Set up Pub/Sub between Capturer and Normalizer in Audio Ingestion Service
    STAGING_TOPIC = env_vars['STAGING_TOPIC']
    create_topic(STAGING_TOPIC)
    create_push_subscription("normalizer-sub", STAGING_TOPIC, f"http://{env_vars['NORMALIZER_SERVICE_HOST']}/")
    
    # Pub/Sub between Audio Ingestion and Transcription Services
    CANONICAL_TOPIC = env_vars['CANONICAL_TOPIC']
    create_push_subscription("transcription-sub", CANONICAL_TOPIC, f"http://{env_vars['TRANSCRIPTION_SERVICE_HOST']}/")

    # Pub/Sub between Transcription and Rules Evaluation Services
    TRANSCRIPTION_TOPIC = env_vars['TRANSCRIPTION_TOPIC']
    create_topic(TRANSCRIPTION_TOPIC)
    create_push_subscription("rules-evaluation-sub", TRANSCRIPTION_TOPIC, f"http://{env_vars['RULES_EVALUATION_SERVICE_HOST']}/")
    
    # Pub/Sub between Rules Evaluation and Notification Services
    RULES_EVALUATION_RESULTS_TOPIC = env_vars['RULES_EVALUATION_RESULTS_TOPIC']
    create_topic(RULES_EVALUATION_RESULTS_TOPIC)
    create_push_subscription("notification-sub", RULES_EVALUATION_RESULTS_TOPIC, f"http://{env_vars['NOTIFICATION_SERVICE_HOST']}/")
    
    print("Pub/Sub initialization complete.")
