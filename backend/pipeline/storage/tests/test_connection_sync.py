"""Tests for the sync connect_db function."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from backend.pipeline.storage.settings import AlloyDBSettings
from backend.pipeline.storage.sync_connection import connect_db


class TestConnectDb:
    def test_uses_settings_values(self) -> None:
        settings = AlloyDBSettings(
            host="10.0.0.1",
            port=5432,
            user="testuser",
            password="testpass",
            db_name="testdb",
        )
        with patch(
            "backend.pipeline.storage.sync_connection.psycopg"
        ) as mock_psycopg:
            mock_psycopg.connect.return_value = MagicMock()
            connect_db(settings)

            mock_psycopg.connect.assert_called_once()
            call_kwargs = mock_psycopg.connect.call_args.kwargs
            assert call_kwargs["host"] == "10.0.0.1"
            assert call_kwargs["port"] == 5432
            assert call_kwargs["user"] == "testuser"
            assert call_kwargs["password"] == "testpass"
            assert call_kwargs["dbname"] == "testdb"
            assert call_kwargs["autocommit"] is True

    def test_defaults_to_alloydb_settings(self) -> None:
        with (
            patch(
                "backend.pipeline.storage.sync_connection.psycopg"
            ) as mock_psycopg,
            patch(
                "backend.pipeline.storage.sync_connection.AlloyDBSettings"
            ) as mock_settings_cls,
        ):
            mock_settings = MagicMock()
            mock_settings.host = "default-host"
            mock_settings.port = 6432
            mock_settings.user = "postgres"
            mock_settings.password = ""
            mock_settings.db_name = "postgres"
            mock_settings_cls.return_value = mock_settings
            mock_psycopg.connect.return_value = MagicMock()

            connect_db()

            mock_settings_cls.assert_called_once()
            call_kwargs = mock_psycopg.connect.call_args.kwargs
            assert call_kwargs["host"] == "default-host"
