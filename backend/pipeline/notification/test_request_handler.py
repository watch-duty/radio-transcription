import os
from logging import Logger
from unittest import TestCase, main, mock

from urllib3.connectionpool import ConnectionPool
from urllib3.exceptions import MaxRetryError

with mock.patch.dict(
    os.environ,
    {
        "NOTIFICATION_ENDPOINT": "https://api.example.com/mock",
        "NOTIFICATION_ENDPOINT_API_KEY": "test-key-123",
    },
):
    from backend.pipeline.notification.request_handler import RequestHandler
from backend.pipeline.schema_types.alert_notification_pb2 import AlertNotification


class TestRequestHandler(TestCase):
    def setUp(self) -> None:
        self.mock_logger = mock.MagicMock(spec=Logger)
        self.handler = RequestHandler(self.mock_logger)

    @mock.patch("backend.pipeline.notification.request_handler.PoolManager")
    def test_success(self, mock_pool_manager: mock.Mock) -> None:
        mock_http = mock_pool_manager.return_value
        mock_response = mock.MagicMock()
        mock_response.status = 200
        mock_response.data = b'{"success": true}'
        mock_http.request.return_value = mock_response

        notification = AlertNotification(transmission_id="123")
        self.handler.send_notification(notification)

        mock_pool_manager.assert_called_once_with(retries=self.handler.retry_strategy)
        mock_http.request.assert_called_once_with(
            "POST",
            "https://api.example.com/mock",
            body='{"transmissionId": "123"}',
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": "test-key-123",
            },
        )

    @mock.patch("backend.pipeline.notification.request_handler.PoolManager")
    def test_max_retry_error(self, mock_pool_manager: mock.Mock) -> None:
        mock_http = mock_pool_manager.return_value
        mock_http.request.side_effect = [
            MaxRetryError(
                pool=ConnectionPool(host="127.0.0.1"),
                url="https://api.test.com/notify",
                reason=Exception(),
            ),
        ]

        notification = AlertNotification(transmission_id="123")

        with self.assertRaises(MaxRetryError):
            self.handler.send_notification(notification)


if __name__ == "__main__":
    main()
