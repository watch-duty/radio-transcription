import os
from logging import Logger

from google.protobuf.json_format import MessageToJson
from urllib3 import PoolManager, Retry
from urllib3.exceptions import HTTPError, MaxRetryError

from backend.pipeline.common.exceptions import NonRetryableError
from backend.pipeline.schema_types.alert_notification_pb2 import (
    AlertNotification,
)

NOTIFICATION_ENDPOINT = os.environ.get("NOTIFICATION_ENDPOINT", "")
NOTIFICATION_ENDPOINT_API_KEY = os.environ.get(
    "NOTIFICATION_ENDPOINT_API_KEY", ""
)
DEFAULT_CONNECTION_POOL_SIZE = 10


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
        self.http = PoolManager(
            maxsize=DEFAULT_CONNECTION_POOL_SIZE, retries=self.retry_strategy
        )

    def send_notification(self, notification: AlertNotification) -> None:
        """
        Makes a POST request to an endpoint with the provided `notification`.
        """
        request_data = MessageToJson(
            notification,
            always_print_fields_with_no_presence=True,
            indent=None,
        )
        self.logger.info(f"Sending payload: {request_data}")

        try:
            response = self.http.request(
                "POST",
                NOTIFICATION_ENDPOINT,
                body=request_data,
                headers={
                    "Content-Type": "application/json",
                    "X-Api-Key": NOTIFICATION_ENDPOINT_API_KEY,
                },
            )
        except MaxRetryError as e:
            self.logger.exception(
                f"Maximum number of retries exceeded for request: {e}"
            )
            raise
        except HTTPError as e:
            self.logger.exception(
                f"Error occurred while sending POST request to endpoint: {e}"
            )
            raise

        self.logger.info(f"Status code: {response.status}")

        response_body = response.data.decode("utf-8")
        self.logger.info(f"Response body: {response_body}")

        if 400 <= response.status < 500:
            err_msg = (
                f"Client error from API: {response.status} - {response_body}"
            )
            raise NonRetryableError(err_msg)
        if response.status >= 500:
            err_msg = (
                f"Server error from API: {response.status} - {response_body}"
            )
            raise HTTPError(err_msg)
