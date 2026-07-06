from __future__ import annotations

from pathlib import Path


def test_feed_change_webhook_package_has_no_database_coupling() -> None:
    package_dir = Path(__file__).resolve().parents[1]
    source_files = (
        path
        for path in package_dir.rglob("*.py")
        if "tests" not in path.relative_to(package_dir).parts
    )
    source = "\n".join(path.read_text() for path in sorted(source_files))

    forbidden = (
        "backend.pipeline.storage",
        "asyncpg",
        "psycopg",
        "AlloyDB",
        "connection_pool",
    )

    for value in forbidden:
        assert value not in source
