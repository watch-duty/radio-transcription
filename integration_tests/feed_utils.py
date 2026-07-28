import asyncio
import datetime
import os
import uuid
from collections.abc import Generator
from typing import Any

import asyncpg
import pytest
import requests
from google.cloud import storage

FEEDS_API_HOST = os.environ.get("FEEDS_API_HOST", "localhost:8089")
_TEST_ACTOR_HEADERS = {"X-WD-Actor-Id": "user:google:e2e-admin@example.com"}


_CONN_KWARGS = {
    "host": os.environ.get("ALLOYDB_HOST", "postgres"),
    "port": int(os.environ.get("ALLOYDB_PORT", "5432")),
    "user": os.environ.get("ALLOYDB_USER", "postgres"),
    "password": os.environ.get("ALLOYDB_PASSWORD", "postgres"),
    "database": os.environ.get("ALLOYDB_DB", "postgres"),
}


async def _update_feed_bookmark(
    feed_id: str, bookmark_time: datetime.datetime | None
) -> None:
    conn = await asyncpg.connect(**_CONN_KWARGS)
    await conn.execute(
        "UPDATE feeds SET last_bookmark_time = $1 WHERE id = $2::uuid",
        bookmark_time,
        feed_id,
    )
    await conn.close()


async def _activate_bcfy_calls_sid_membership(
    feed_id: str,
    sid: str,
    group_id: str,
) -> None:
    """Activate one current-contract Calls child beneath a durable SID lease."""
    conn = await asyncpg.connect(**_CONN_KWARGS)
    try:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE public.feed_properties
                SET bcfy_calls_sid = $2,
                    bcfy_calls_group_id = $3,
                    bcfy_calls_is_trunked = TRUE
                WHERE feed_id = $1::uuid
                """,
                feed_id,
                sid,
                group_id,
            )
            await conn.execute(
                """
                UPDATE public.feeds
                SET status = 'active'::public.feed_status,
                    worker_id = NULL,
                    last_heartbeat = NULL,
                    unclaimed_since = NULL
                WHERE id = $1::uuid
                """,
                feed_id,
            )
            await conn.execute(
                """
                INSERT INTO public.ingestion_leases (
                    source_type,
                    lease_key,
                    status
                ) VALUES (
                    'bcfy_calls',
                    $1,
                    'unclaimed'::public.feed_status
                )
                """,
                sid,
            )
    finally:
        await conn.close()


async def _deactivate_bcfy_calls_sid_lease(sid: str) -> None:
    """Fence the temporary parent lease before deactivating its child Feed."""
    conn = await asyncpg.connect(**_CONN_KWARGS)
    try:
        await conn.execute(
            """
            UPDATE public.ingestion_leases
            SET status = 'deactivated'::public.feed_status,
                worker_id = NULL,
                last_heartbeat = NULL
            WHERE source_type = 'bcfy_calls'
              AND lease_key = $1
            """,
            sid,
        )
    finally:
        await conn.close()


def _create_and_cleanup_feed(
    payload: dict[str, Any],
) -> Generator[tuple[str, str]]:
    """Helper to create a feed via API and clean up after test."""
    url = f"http://{FEEDS_API_HOST}/v1/feeds"
    response = requests.post(
        url,
        json=payload,
        headers=_TEST_ACTOR_HEADERS,
        timeout=10,
    )
    response.raise_for_status()

    feed_id = response.json().get("id", "")
    if not feed_id:
        msg = "Feed ID not returned by API"
        raise ValueError(msg)

    try:
        yield feed_id, payload["name"]
    finally:

        async def _cleanup_db() -> None:
            conn = await asyncpg.connect(**_CONN_KWARGS)
            await conn.execute(
                "DELETE FROM audio_segments WHERE feed_id = $1::uuid", feed_id
            )
            await conn.execute(
                "DELETE FROM transcripts WHERE feed_id = $1::uuid", feed_id
            )
            await conn.close()

        asyncio.run(_cleanup_db())

        # Delete feed via API
        del_url = f"http://{FEEDS_API_HOST}/v1/feeds/{feed_id}/deactivate"
        del_response = requests.post(
            del_url,
            headers=_TEST_ACTOR_HEADERS,
            timeout=10,
        )
        del_response.raise_for_status()


@pytest.fixture(name="test_bcfy_feed")
def create_test_bcfy_feed() -> Generator[tuple[str, str]]:
    """Fixture to create a temporary BCFY feed for testing.

    Yields:
        tuple[str, str]: A tuple containing (feed_id, source_feed_id).
    """
    feed_name = f"integration-test-feed-{uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "source_type": "bcfy_feeds",
        "source_feed_id": str(uuid.uuid4().fields[0]),
    }
    yield from _create_and_cleanup_feed(payload)


@pytest.fixture(name="test_sid_polling_feed")
def create_test_sid_polling_feed() -> Generator[tuple[str, str]]:
    """Create a temporary Calls Feed beneath its current SID authority.

    Yields:
        tuple[str, str]: A tuple containing (feed_id, feed_name).
    """
    sid = str(uuid.uuid4().fields[0])
    group_id = "2912"
    feed_name = f"integration-test-polling-feed-{uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "source_type": "bcfy_calls",
        "source_feed_id": f"{sid}-{group_id}",
    }
    gen = _create_and_cleanup_feed(payload)
    feed_id, _ = next(gen)
    lease_created = False
    try:
        asyncio.run(
            _activate_bcfy_calls_sid_membership(
                feed_id,
                sid,
                group_id,
            )
        )
        lease_created = True
        yield feed_id, feed_name
    finally:
        if lease_created:
            asyncio.run(_deactivate_bcfy_calls_sid_lease(sid))
        try:
            next(gen)
        except StopIteration:
            pass


@pytest.fixture(name="test_echo_feed")
def create_test_echo_feed() -> Generator[tuple[str, str]]:
    """Fixture to create a temporary echo feed for testing.

    Yields:
        tuple[str, str]: A tuple containing (feed_id, source_feed_id).
    """
    feed_name = f"integration-test-echo-feed-{uuid.uuid4()}"
    source_feed_id = f"src-{uuid.uuid4()}"
    bucket_name = os.environ.get(
        "ECHO_RECORDINGS_BUCKET", "echo-recordings-test"
    )

    # Create a dummy keep file to simulate GCS directory presence
    # before creating the feed
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(f"{source_feed_id}/.keep")
    blob.upload_from_string("")

    payload = {
        "name": feed_name,
        "source_type": "echo",
        "source_feed_id": source_feed_id,
    }
    gen = _create_and_cleanup_feed(payload)
    feed_id, _ = next(gen)
    try:
        yield feed_id, payload["source_feed_id"]
    finally:
        try:
            next(gen)
        except StopIteration:
            pass
        # Clean up GCS keep file and dummy folder
        try:
            blob.delete()
        except Exception:  # noqa: S110
            pass


@pytest.fixture(name="test_fire_notifications_feed")
def create_test_fire_notifications_feed() -> Generator[tuple[str, str]]:
    """Fixture to create a temporary fire notifications feed for testing.

    Yields:
        tuple[str, str]: A tuple containing (feed_id, source_feed_id).
    """
    feed_name = f"integration-test-fn-feed-{uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "source_type": "fire_notifications",
        "source_feed_id": "RECORDINGS/SAN-JOSE-DISP",
        "tags": [{"key": "system/timezone", "value": "America/Los_Angeles"}],
    }
    gen = _create_and_cleanup_feed(payload)
    feed_id, _ = next(gen)

    # Reset last_bookmark_time to NULL so older files are processed
    # with the new timestamp
    asyncio.run(_update_feed_bookmark(feed_id, None))

    try:
        yield feed_id, payload["source_feed_id"]
    finally:
        try:
            next(gen)
        except StopIteration:
            pass
