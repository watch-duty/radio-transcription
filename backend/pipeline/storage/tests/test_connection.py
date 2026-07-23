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
            port=6432,
            user="my-user",
            password="",
            database="my-db",
            min_size=8,
            max_size=8,
            statement_cache_size=0,
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
            statement_cache_size=0,
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
            port=6432,
            user="my-user",
            password="",
            database="my-db",
            min_size=8,
            max_size=8,
            statement_cache_size=0,
            command_timeout=30.0,
            timeout=10.0,
        )
        self.assertEqual(result, mock_pool)

    @mock.patch(
        "backend.pipeline.storage.connection.asyncpg.create_pool",
        new_callable=mock.AsyncMock,
    )
    async def test_create_pool_timeout_error(
        self,
        mock_create_pool: mock.AsyncMock,
    ) -> None:
        """Test create_pool handles TimeoutError with custom message."""
        mock_create_pool.side_effect = TimeoutError("asyncpg timeout")

        with self.assertRaises(TimeoutError) as context:
            await connection.create_pool(
                host="10.0.0.1",
                user="my-user",
                db_name="my-db",
                timeout=10.0,
            )

        error_message = str(context.exception)
        self.assertIn("Failed to connect to AlloyDB", error_message)
        self.assertIn("10.0.0.1:6432", error_message)
        self.assertIn("10.0s", error_message)
        self.assertIn("AlloyDB Auth Proxy", error_message)

    @mock.patch(
        "backend.pipeline.storage.connection.asyncpg.create_pool",
        new_callable=mock.AsyncMock,
    )
    async def test_create_pool_connection_error(
        self,
        mock_create_pool: mock.AsyncMock,
    ) -> None:
        """Test create_pool handles generic Exception as ConnectionError."""
        original_error = RuntimeError("Invalid credentials")
        mock_create_pool.side_effect = original_error

        with self.assertRaises(ConnectionError) as context:
            await connection.create_pool(
                host="10.0.0.1",
                user="my-user",
                db_name="my-db",
            )

        error_message = str(context.exception)
        self.assertIn("Failed to connect to AlloyDB", error_message)
        self.assertIn("Invalid credentials", error_message)
        self.assertIn("10.0.0.1:6432", error_message)


class TestClosePool(unittest.IsolatedAsyncioTestCase):
    """Tests for close_pool."""

    async def test_close_pool(self) -> None:
        """Test that close_pool calls pool.close()."""
        mock_pool = mock.AsyncMock()

        await connection.close_pool(mock_pool)

        mock_pool.close.assert_awaited_once()


class TestFetchWithTimeoutBudget(unittest.IsolatedAsyncioTestCase):
    """Tests for one bounded pool checkout, query, and release lifecycle."""

    async def test_release_failure_does_not_mask_query_failure(self) -> None:
        query_failure = RuntimeError("query failed")
        db_connection = mock.AsyncMock()
        db_connection.fetch.side_effect = query_failure
        pool = mock.MagicMock()
        pool.acquire = mock.AsyncMock(return_value=db_connection)
        pool.release = mock.AsyncMock(
            side_effect=TimeoutError("release timed out")
        )

        with (
            mock.patch.object(
                connection.time,
                "monotonic",
                side_effect=(100.0, 101.0, 105.0, 118.0),
            ),
            self.assertRaisesRegex(RuntimeError, "query failed") as raised,
        ):
            await connection.fetch_with_timeout_budget(
                pool,
                "SELECT 1",
                timeout_sec=18.0,
            )

        self.assertIs(raised.exception, query_failure)


class TestCreatePoolWithRetry(unittest.IsolatedAsyncioTestCase):
    """Tests for create_pool_with_retry."""

    @mock.patch(
        "backend.pipeline.storage.connection.create_pool_from_settings",
        new_callable=mock.AsyncMock,
    )
    @mock.patch("asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_succeeds_on_first_attempt(
        self,
        mock_sleep: mock.AsyncMock,
        mock_create: mock.AsyncMock,
    ) -> None:
        """No retries when connection succeeds immediately."""
        mock_pool = mock.AsyncMock()
        mock_create.return_value = mock_pool

        result = await connection.create_pool_with_retry()

        mock_create.assert_awaited_once()
        mock_sleep.assert_not_awaited()
        self.assertEqual(result, mock_pool)

    @mock.patch(
        "backend.pipeline.storage.connection.create_pool_from_settings",
        new_callable=mock.AsyncMock,
    )
    @mock.patch("asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_retries_and_succeeds(
        self,
        mock_sleep: mock.AsyncMock,
        mock_create: mock.AsyncMock,
    ) -> None:
        """Succeeds after two transient TimeoutErrors."""
        mock_pool = mock.AsyncMock()
        mock_create.side_effect = [
            TimeoutError("timeout"),
            TimeoutError("timeout"),
            mock_pool,
        ]

        result = await connection.create_pool_with_retry()

        self.assertEqual(mock_create.await_count, 3)
        self.assertEqual(mock_sleep.await_count, 2)
        self.assertEqual(result, mock_pool)

    @mock.patch(
        "backend.pipeline.storage.connection.create_pool_from_settings",
        new_callable=mock.AsyncMock,
    )
    @mock.patch("asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_reraises_after_max_attempts(
        self,
        mock_sleep: mock.AsyncMock,
        mock_create: mock.AsyncMock,
    ) -> None:
        """Re-raises the original exception after all 5 attempts are exhausted."""
        mock_create.side_effect = ConnectionError("pooler unavailable")

        with self.assertRaises(ConnectionError, msg="pooler unavailable"):
            await connection.create_pool_with_retry()

        self.assertEqual(mock_create.await_count, 5)
        self.assertEqual(mock_sleep.await_count, 4)

    @mock.patch(
        "backend.pipeline.storage.connection.create_pool_from_settings",
        new_callable=mock.AsyncMock,
    )
    @mock.patch("asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_retries_on_os_error(
        self,
        mock_sleep: mock.AsyncMock,
        mock_create: mock.AsyncMock,
    ) -> None:
        """OSError (e.g. network unreachable) is also retried."""
        mock_pool = mock.AsyncMock()
        mock_create.side_effect = [OSError("network unreachable"), mock_pool]

        result = await connection.create_pool_with_retry()

        self.assertEqual(mock_create.await_count, 2)
        self.assertEqual(result, mock_pool)


if __name__ == "__main__":
    unittest.main()
