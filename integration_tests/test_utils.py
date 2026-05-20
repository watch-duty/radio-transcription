"""Common testing utilities for integration tests."""

import asyncio
import logging
import os
import time
from collections.abc import Callable

import asyncpg
import pytest


def assert_eventually(
    condition_func: Callable[[], bool],
    timeout_sec: float = 5.0,
    error_msg: str = "Condition not met within timeout",
) -> None:
    """Repeatedly polls a condition function until it returns True."""
    end_time = time.time() + timeout_sec
    while time.time() < end_time:
        if condition_func():
            return
        time.sleep(0.5)
    pytest.fail(error_msg)


logger = logging.getLogger(__name__)


def _get_db_conn_kwargs():
    return {
        "host": os.environ["ALLOYDB_HOST"],
        "port": int(os.environ["ALLOYDB_PORT"]),
        "user": os.environ["ALLOYDB_USER"],
        "password": os.environ["ALLOYDB_PASSWORD"],
        "database": os.environ["ALLOYDB_DB"],
    }


def verify_transcript_in_db(feed_id: str, timeout_sec: float = 180.0) -> bool:
    """Polls the database until a transcript for the given feed_id appears."""

    async def _check_db():
        conn = await asyncpg.connect(**_get_db_conn_kwargs())
        row = await conn.fetchrow(
            "SELECT * FROM transcripts WHERE feed_id = $1::uuid", feed_id
        )
        await conn.close()
        return row is not None

    def condition():
        try:
            return asyncio.run(_check_db())
        except Exception as e:
            logger.warning(f"DB check failed: {e}")
            return False

    logger.info(f"Waiting for transcript in DB for feed {feed_id}...")

    assert_eventually(
        condition,
        timeout_sec=timeout_sec,
        error_msg="Transcript not found in DB",
    )
    return True
