from __future__ import annotations

import unittest
from unittest import mock

from backend.pipeline.common.clients.pubsub_client import PubSubClient


class TestPubSubClient(unittest.IsolatedAsyncioTestCase):
    """Tests for lazy init and cleanup behavior of PubSubClient."""

    @mock.patch("backend.pipeline.common.clients.pubsub_client.pubsub_v1")
    async def test_get_publisher_is_lazy_and_reused(
        self,
        mock_pubsub_v1: mock.MagicMock,
    ) -> None:
        """Publisher options/client are created once and then reused."""
        client = PubSubClient()

        mock_options = mock.MagicMock()
        mock_publisher = mock.MagicMock()
        mock_pubsub_v1.types.PublisherOptions.return_value = mock_options
        mock_pubsub_v1.PublisherClient.return_value = mock_publisher

        first = client.get_publisher()
        second = client.get_publisher()

        self.assertIs(first, second)
        self.assertIs(first, mock_publisher)
        mock_pubsub_v1.types.PublisherOptions.assert_called_once_with(
            enable_message_ordering=True,
        )
        mock_pubsub_v1.PublisherClient.assert_called_once_with(
            publisher_options=mock_options,
        )

    @mock.patch("backend.pipeline.common.clients.pubsub_client.asyncio.to_thread")
    @mock.patch("backend.pipeline.common.clients.pubsub_client.pubsub_v1")
    async def test_close_stops_initialized_publisher(
        self,
        mock_pubsub_v1: mock.MagicMock,
        mock_to_thread: mock.AsyncMock,
    ) -> None:
        """close() calls stop in a worker thread and clears state."""
        client = PubSubClient()

        mock_options = mock.MagicMock()
        mock_publisher = mock.MagicMock()
        mock_pubsub_v1.types.PublisherOptions.return_value = mock_options
        mock_pubsub_v1.PublisherClient.return_value = mock_publisher

        client.get_publisher()
        await client.close()

        mock_to_thread.assert_awaited_once_with(mock_publisher.stop)
        self.assertIsNone(client._publisher)

    @mock.patch("backend.pipeline.common.clients.pubsub_client.asyncio.to_thread")
    async def test_close_is_noop_when_not_initialized(
        self,
        mock_to_thread: mock.AsyncMock,
    ) -> None:
        """close() does not call stop when publisher was never created."""
        client = PubSubClient()

        await client.close()

        mock_to_thread.assert_not_awaited()
        self.assertIsNone(client._publisher)
