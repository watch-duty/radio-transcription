import base64
import json
import logging
import os

import functions_framework
import requests
from cloudevents.http.event import CloudEvent

logger = logging.getLogger(__name__)

POST_TIMEOUT_SECONDS = 5
ENDPOINT = os.environ.get("ENDPOINT")

@functions_framework.cloud_event
def send_notification(cloud_event: CloudEvent) -> None:
    try:
        if not ENDPOINT:
            return "Error: Endpoint is not configured.", 500

        # Pub/Sub data is base64 encoded inside the 'data' field.
        # cloud_event.data['message']['data'] contains the actual payload.
        base64_data = cloud_event.data["message"]["data"]
        decoded_string = base64.b64decode(base64_data).decode("utf-8")
        payload = json.loads(decoded_string)

        # Send POST request to endpoint.
        response = requests.post(
            ENDPOINT, 
            data=json.dumps(payload), 
            headers={
                "Content-Type": "application/json"
            },
            timeout=POST_TIMEOUT_SECONDS
        )

        # Raise an exception for bad status codes
        response.raise_for_status()

        logger.info(f"Message sent successfully! Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logger.exception(f"Request failed: {e}")
        return f"Error: Request to external service failed. {e}", 500
