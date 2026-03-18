from __future__ import annotations

import unittest
from unittest import mock

from backend.pipeline.common.clients import gcs_client


class TestGcsClient(unittest.IsolatedAsyncioTestCase):
    """Tests for lazy init and cleanup behavior of GcsClient."""

    @mock.patch("backend.pipeline.common.clients.gcs_client.Storage")
    @mock.patch("backend.pipeline.common.clients.gcs_client.aiohttp.ClientSession")
    async def test_get_storage_is_lazy_and_reused(
        self,
        mock_client_session: mock.MagicMock,
        mock_storage_cls: mock.MagicMock,
    ) -> None:
        """Storage and session are created once and reused."""
        client = gcs_client.GcsClient()

        mock_session = mock.MagicMock()
        mock_storage = mock.MagicMock()
        mock_client_session.return_value = mock_session
        mock_storage_cls.return_value = mock_storage

        first = client.get_storage()
        second = client.get_storage()

        self.assertIs(first, second)
        self.assertIs(first, mock_storage)
        mock_client_session.assert_called_once_with()
        mock_storage_cls.assert_called_once_with(session=mock_session)

    @mock.patch("backend.pipeline.common.clients.gcs_client.Storage")
    @mock.patch("backend.pipeline.common.clients.gcs_client.aiohttp.ClientSession")
    async def test_close_closes_storage_and_session(
        self,
        mock_client_session: mock.MagicMock,
        mock_storage_cls: mock.MagicMock,
    ) -> None:
        """close() releases initialized resources and clears references."""
        client = gcs_client.GcsClient()

        mock_session = mock.AsyncMock()
        mock_storage = mock.AsyncMock()
        mock_client_session.return_value = mock_session
        mock_storage_cls.return_value = mock_storage

        client.get_storage()
        await client.close()

        mock_storage.close.assert_awaited_once_with()
        mock_session.close.assert_awaited_once_with()
        self.assertIsNone(client._storage)
        self.assertIsNone(client._session)

    async def test_close_is_noop_when_not_initialized(self) -> None:
        """close() does not fail when get_storage() was never called."""
        client = gcs_client.GcsClient()

        await client.close()

        self.assertIsNone(client._storage)
        self.assertIsNone(client._session)
