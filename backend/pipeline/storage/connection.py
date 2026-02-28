from __future__ import annotations

import atexit
import threading
from typing import TYPE_CHECKING

from google.cloud.alloydb.connector import Connector, IPTypes

if TYPE_CHECKING:
    import psycopg

# Maintain a global connector to avoid leaking background threads
# on repeated calls to create_connection.
_connector: Connector | None = None
_connector_lock = threading.Lock()


def close_connector() -> None:
    """Close the global AlloyDB Connector if it exists."""
    global _connector  # noqa: PLW0603
    with _connector_lock:
        if _connector is not None:
            _connector.close()
            _connector = None


atexit.register(close_connector)


def create_connection(  # noqa: PLR0913
    project_id: str,
    region: str,
    cluster_name: str,
    instance_name: str,
    user: str,
    db_name: str,
    password: str | None = None,
    ip_type: IPTypes | str = IPTypes.PRIVATE,
) -> psycopg.Connection:
    """
    Create a psycopg connection to the AlloyDB instance using the AlloyDB Connector.

    This manages the secure TLS tunnel and handles IAM-based authentication.

    Args:
        project_id: GCP Project ID.
        region: GCP Region (e.g., "us-central1").
        cluster_name: AlloyDB cluster name.
        instance_name: AlloyDB instance name.
        user: Database username.
        db_name: Target database name (e.g., "postgres").
        password: Database password. Optional if using IAM authentication.
        ip_type: Type of IP to connect to (default: IPTypes.PRIVATE).

    Returns:
        A psycopg connection to the AlloyDB instance.

    """
    global _connector  # noqa: PLW0603
    if _connector is None:
        with _connector_lock:
            if _connector is None:
                _connector = Connector()

    instance_uri = (
        f"projects/{project_id}/"
        f"locations/{region}/"
        f"clusters/{cluster_name}/"
        f"instances/{instance_name}"
    )

    # The connector requires a string password, even if empty for IAM authentication
    pw = password or ""

    conn: psycopg.Connection = _connector.connect(
        instance_uri,
        "psycopg3",
        user=user,
        password=pw,
        db=db_name,
        ip_type=ip_type,
    )
    return conn
