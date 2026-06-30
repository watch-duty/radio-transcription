from __future__ import annotations

from pathlib import Path


def test_feed_change_webhook_package_has_no_database_coupling() -> None:
    package_dir = Path(__file__).resolve().parents[1]
    source = "\n".join(
        path.read_text() for path in sorted(package_dir.glob("*.py"))
    )

    forbidden = (
        "backend.pipeline.storage",
        "asyncpg",
        "psycopg",
        "AlloyDB",
        "connection_pool",
    )

    for value in forbidden:
        assert value not in source
