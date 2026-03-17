import os
from logging import Logger

from google.protobuf.json_format import MessageToJson
from urllib3 import PoolManager, Retry
from urllib3.exceptions import MaxRetryError

from backend.pipeline.schema_types.alert_notification_pb2 import AlertNotification

NOTIFICATION_ENDPOINT = os.environ.get("NOTIFICATION_ENDPOINT", "")
NOTIFICATION_ENDPOINT_API_KEY = os.environ.get("NOTIFICATION_ENDPOINT_API_KEY", "")


class RequestHandler:
    """
    Class which manages incoming notification requests.
    """

    def __init__(self, logger: Logger) -> None:
        self.logger = logger
        self.retry_strategy = Retry(
            total=3,
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=frozenset(["POST"]),
        )

    def send_notification(self, notification: AlertNotification) -> None:
        """
        Sends a notification an endpoint with a POST request.
        """
        request_data = MessageToJson(notification, indent=None)
        self.logger.info(f"Sending payload: {request_data}")

        http = PoolManager(retries=self.retry_strategy)

        try:
            response = http.request(
                "POST",
                NOTIFICATION_ENDPOINT,
                body=request_data,
                headers={
                    "Content-Type": "application/json",
                    "X-Api-Key": NOTIFICATION_ENDPOINT_API_KEY,
                },
            )
            self.logger.info(f"Status code: ${response.status}")
            self.logger.info(f"Response body: ${response.data.decode('utf-8')}")
        except MaxRetryError as e:
            self.logger.exception(
                f"Maximum number of retries exceeded for request: {e}"
            )
            raise
        except Exception as e:
            self.logger.exception(
                f"Error occurred while sending POST request to endpoint: {e}"
            )
            raise
