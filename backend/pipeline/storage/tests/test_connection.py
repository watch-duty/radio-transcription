from __future__ import annotations

import unittest
from unittest import mock

from backend.pipeline.storage import connection


class TestCreatePool(unittest.IsolatedAsyncioTestCase):
    """Tests for create_pool."""

    @mock.patch(
        "backend.pipeline.storage.connection.asyncpg.create_pool",
        new_callable=mock.AsyncMock,
    )
    async def test_create_pool_defaults(
        self,
        mock_create_pool: mock.AsyncMock,
    ) -> None:
        """Test create_pool with default arguments."""
        mock_pool = mock.AsyncMock()
        mock_create_pool.return_value = mock_pool

        result = await connection.create_pool(
            host="10.0.0.1",
            user="my-user",
            db_name="my-db",
        )

        mock_create_pool.assert_called_once_with(
            host="10.0.0.1",
            port=5432,
            user="my-user",
            password="",
            database="my-db",
            min_size=10,
            max_size=10,
        )
        self.assertEqual(result, mock_pool)

    @mock.patch(
        "backend.pipeline.storage.connection.asyncpg.create_pool",
        new_callable=mock.AsyncMock,
    )
    async def test_create_pool_custom_args(
        self,
        mock_create_pool: mock.AsyncMock,
    ) -> None:
        """Test create_pool with custom arguments."""
        mock_pool = mock.AsyncMock()
        mock_create_pool.return_value = mock_pool

        result = await connection.create_pool(
            host="10.0.0.2",
            user="my-user",
            db_name="my-db",
            password="secret",
            port=5433,
            min_size=5,
            max_size=20,
        )

        mock_create_pool.assert_called_once_with(
            host="10.0.0.2",
            port=5433,
            user="my-user",
            password="secret",
            database="my-db",
            min_size=5,
            max_size=20,
        )
        self.assertEqual(result, mock_pool)

    @mock.patch(
        "backend.pipeline.storage.connection.asyncpg.create_pool",
        new_callable=mock.AsyncMock,
    )
    async def test_create_pool_with_timeouts(
        self,
        mock_create_pool: mock.AsyncMock,
    ) -> None:
        """Test create_pool forwards command_timeout and timeout."""
        mock_pool = mock.AsyncMock()
        mock_create_pool.return_value = mock_pool

        result = await connection.create_pool(
            host="10.0.0.1",
            user="my-user",
            db_name="my-db",
            command_timeout=30.0,
            timeout=10.0,
        )

        mock_create_pool.assert_called_once_with(
            host="10.0.0.1",
            port=5432,
            user="my-user",
            password="",
            database="my-db",
            min_size=10,
            max_size=10,
            command_timeout=30.0,
            timeout=10.0,
        )
        self.assertEqual(result, mock_pool)


class TestClosePool(unittest.IsolatedAsyncioTestCase):
    """Tests for close_pool."""

    async def test_close_pool(self) -> None:
        """Test that close_pool calls pool.close()."""
        mock_pool = mock.AsyncMock()

        await connection.close_pool(mock_pool)

        mock_pool.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
