"""Unit tests for the centralized test schema migration helper."""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from backend.pipeline.common.test_schema_helper import (
    async_apply_test_schema,
    sync_apply_test_schema,
)


class TestSchemaHelper(unittest.IsolatedAsyncioTestCase):
    @patch("backend.pipeline.common.test_schema_helper._SQL_DIR")
    async def test_async_apply_test_schema(
        self, mock_sql_dir: MagicMock
    ) -> None:
        mock_file_normal = MagicMock()
        mock_file_normal.name = "001_normal.sql"
        mock_file_normal.read_text.return_value = "CREATE TABLE a (id UUID);"

        mock_file_autocommit = MagicMock()
        mock_file_autocommit.name = "027_autocommit.sql"
        mock_file_autocommit.read_text.return_value = "-- AUTOCOMMIT\nALTER TYPE a ADD VALUE 'B';\nALTER TYPE a ADD VALUE 'C';"

        mock_file_disabled = MagicMock()
        mock_file_disabled.name = "005_disabled.sql"
        mock_file_disabled.read_text.return_value = (
            "-- Disabled: dropped in 018.\n--\n-- CREATE INDEX idx ON a (id);\n"
        )

        mock_file_disabled_autocommit = MagicMock()
        mock_file_disabled_autocommit.name = "039_disabled_autocommit.sql"
        mock_file_disabled_autocommit.read_text.return_value = "-- AUTOCOMMIT\n-- Disabled.\n-- CREATE INDEX CONCURRENTLY idx ON a (id);\n"

        mock_sql_dir.glob.return_value = [
            mock_file_normal,
            mock_file_autocommit,
            mock_file_disabled,
            mock_file_disabled_autocommit,
        ]

        mock_conn = AsyncMock()
        await async_apply_test_schema(mock_conn)

        # Verified normal files are executed as a single block
        mock_conn.execute.assert_any_call("CREATE TABLE a (id UUID);")
        # Verified autocommit files are split by semicolon and executed individually
        mock_conn.execute.assert_any_call(
            "-- AUTOCOMMIT\nALTER TYPE a ADD VALUE 'B'"
        )
        mock_conn.execute.assert_any_call("ALTER TYPE a ADD VALUE 'C'")
        # Comment-only files and chunks are skipped: asyncpg cannot execute
        # an empty query.
        self.assertEqual(mock_conn.execute.await_count, 3)

    @patch("backend.pipeline.common.test_schema_helper._SQL_DIR")
    def test_sync_apply_test_schema(self, mock_sql_dir: MagicMock) -> None:
        mock_file_normal = MagicMock()
        mock_file_normal.name = "001_normal.sql"
        mock_file_normal.read_text.return_value = "CREATE TABLE a (id UUID);"

        mock_file_autocommit = MagicMock()
        mock_file_autocommit.name = "027_autocommit.sql"
        mock_file_autocommit.read_text.return_value = "-- AUTOCOMMIT\nALTER TYPE a ADD VALUE 'B';\nALTER TYPE a ADD VALUE 'C';"

        mock_file_disabled = MagicMock()
        mock_file_disabled.name = "005_disabled.sql"
        mock_file_disabled.read_text.return_value = (
            "-- Disabled: dropped in 018.\n--\n-- CREATE INDEX idx ON a (id);\n"
        )

        mock_sql_dir.glob.return_value = [
            mock_file_normal,
            mock_file_autocommit,
            mock_file_disabled,
        ]

        mock_conn = MagicMock()
        sync_apply_test_schema(mock_conn)

        mock_conn.execute.assert_any_call(b"CREATE TABLE a (id UUID);")
        mock_conn.execute.assert_any_call(
            b"-- AUTOCOMMIT\nALTER TYPE a ADD VALUE 'B'"
        )
        mock_conn.execute.assert_any_call(b"ALTER TYPE a ADD VALUE 'C'")
        self.assertEqual(mock_conn.execute.call_count, 3)
