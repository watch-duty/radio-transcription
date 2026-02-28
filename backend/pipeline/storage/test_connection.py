from __future__ import annotations

import unittest
from unittest import mock

from google.cloud.alloydb.connector import IPTypes

from backend.pipeline.storage import connection


class TestConnection(unittest.TestCase):
    """Tests for connection.py."""

    def setUp(self) -> None:
        """Clear global connector state before each test."""
        connection.close_connector()

    def tearDown(self) -> None:
        """Clear global connector state after each test."""
        connection.close_connector()

    @mock.patch("backend.pipeline.storage.connection.Connector")
    def test_create_connection_defaults(self, mock_connector_cls: mock.MagicMock) -> None:
        """Test create_connection with default arguments."""
        mock_connector = mock.MagicMock()
        mock_connector_cls.return_value = mock_connector

        conn = connection.create_connection(
            project_id="my-project",
            region="us-central1",
            cluster_name="my-cluster",
            instance_name="my-instance",
            user="my-user",
            db_name="my-db",
        )

        mock_connector_cls.assert_called_once()
        expected_uri = "projects/my-project/locations/us-central1/clusters/my-cluster/instances/my-instance"
        mock_connector.connect.assert_called_once_with(
            expected_uri,
            "psycopg3",
            user="my-user",
            password="",
            db="my-db",
            ip_type=IPTypes.PRIVATE,
        )
        self.assertEqual(conn, mock_connector.connect.return_value)

    @mock.patch("backend.pipeline.storage.connection.Connector")
    def test_create_connection_custom_args(self, mock_connector_cls: mock.MagicMock) -> None:
        """Test create_connection with custom password and ip_type."""
        mock_connector = mock.MagicMock()
        mock_connector_cls.return_value = mock_connector

        conn = connection.create_connection(
            project_id="my-project",
            region="us-central1",
            cluster_name="my-cluster",
            instance_name="my-instance",
            user="my-user",
            db_name="my-db",
            password="my-password",
            ip_type=IPTypes.PUBLIC,
        )

        expected_uri = "projects/my-project/locations/us-central1/clusters/my-cluster/instances/my-instance"
        mock_connector.connect.assert_called_once_with(
            expected_uri,
            "psycopg3",
            user="my-user",
            password="my-password",
            db="my-db",
            ip_type=IPTypes.PUBLIC,
        )
        self.assertEqual(conn, mock_connector.connect.return_value)

    @mock.patch("backend.pipeline.storage.connection.Connector")
    def test_reuses_global_connector(self, mock_connector_cls: mock.MagicMock) -> None:
        """Test that subsequent calls reuse the global connector."""
        mock_connector = mock.MagicMock()
        mock_connector_cls.return_value = mock_connector

        connection.create_connection(
            project_id="my-project",
            region="us-central1",
            cluster_name="my-cluster",
            instance_name="my-instance",
            user="my-user",
            db_name="my-db",
        )

        connection.create_connection(
            project_id="my-project",
            region="us-central1",
            cluster_name="my-cluster",
            instance_name="my-instance",
            user="other-user",
            db_name="my-db",
        )

        # Connector class should only be instantiated once
        mock_connector_cls.assert_called_once()
        # But connect is called twice
        self.assertEqual(mock_connector.connect.call_count, 2)

    @mock.patch("backend.pipeline.storage.connection.Connector")
    def test_close_connector(self, mock_connector_cls: mock.MagicMock) -> None:
        """Test close_connector closes and unsets the global connector."""
        mock_connector = mock.MagicMock()
        mock_connector_cls.return_value = mock_connector

        connection.create_connection(
            project_id="my-project",
            region="us-central1",
            cluster_name="my-cluster",
            instance_name="my-instance",
            user="my-user",
            db_name="my-db",
        )

        # Confirm connector was kept
        self.assertIsNotNone(connection._connector)

        connection.close_connector()

        mock_connector.close.assert_called_once()
        self.assertIsNone(connection._connector)


if __name__ == "__main__":
    unittest.main()
