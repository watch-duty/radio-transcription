import base64
import functions_framework
import json
import os
import requests

@functions_framework.cloud_event
def send_notification(cloud_event):
    try:
        ENDPOINT = os.environ.get('ENDPOINT')

        if not ENDPOINT:
            return f"Error: Endpoint is not configured.", 500

        # Pub/Sub data is base64 encoded inside the 'data' field.
        # cloud_event.data['message']['data'] contains the actual payload.
        base64_data = cloud_event.data["message"]["data"]
        decoded_string = base64.b64decode(base64_data).decode('utf-8')
        payload = json.loads(decoded_string)

        # Send POST request to endpoint.
        response = requests.post(
            ENDPOINT, 
            data=json.dumps(payload), 
            headers={
                "Content-Type": "application/json"
            }
        )
        
        # Raise an exception for bad status codes
        response.raise_for_status()

        print(f"Message sent successfully! Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return f"Error: Request to external service failed. {e}", 500
