from __future__ import annotations

import asyncio
import contextlib
import datetime
import os
import unittest
import uuid
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import asyncpg
import docker
import requests as sync_requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.postgres import PostgresContainer

from backend.pipeline.common import gcp_helper
from backend.pipeline.common.clients import gcs_client
from backend.pipeline.ingestion.collectors.openmhz._types import CallEvent
from backend.pipeline.ingestion.collectors.openmhz.collector import (
    openmhz_collector,
)
from backend.pipeline.ingestion.collectors.tests.conftest import (
    _default_resources,
    _require_captured_chunk,
)
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
    FeedStore,
    LeasedFeed,
    SourceType,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

_CLAIM: dict[SourceType, int] = {SourceType.OPENMHZ: 1}

_REPO_ROOT = Path(__file__).resolve().parents[5]
_SQL_DIR = (
    _REPO_ROOT / "terraform" / "modules" / "alloydb" / "sql" / "ingestion"
)

_FAKE_GCS_PORT = 4443
_TEST_BUCKET = "test-audio-bucket"


_COL_MOD = "backend.pipeline.ingestion.collectors.openmhz.collector"


def _docker_available() -> bool:
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


def _make_m4a_bytes() -> bytes:
    """Generate a valid 1-second silent m4a file using pydub."""
    return b"dummy m4a audio"


def _make_call(
    call_id: str = "abc",
    length_sec: int = 5,
) -> CallEvent:
    return CallEvent(
        id=call_id,
        talkgroup_num=34480,
        url=f"https://media2.openmhz.com/media/wmata/34480/wmata-34480-{call_id}.m4a",
        time=datetime.datetime(2026, 4, 2, 22, 57, 20, tzinfo=datetime.UTC),
        length_sec=length_sec,
        freq=490962500,
        src_list=[],
        short_name="wmata",
        emergency=False,
    )


@contextlib.asynccontextmanager
async def _mock_transport(
    calls: list[CallEvent],
) -> AsyncIterator[AsyncIterator[CallEvent]]:
    """Fake transport that yields scripted CallEvent objects."""

    async def _events() -> AsyncIterator[CallEvent]:
        for c in calls:
            yield c

    yield _events()


@unittest.skipUnless(_docker_available(), "Docker is not available")
class TestOpenmhzCollectorIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for openmhz_collector with real GCS and DB."""

    db_container: PostgresContainer
    gcs_container: DockerContainer

    @classmethod
    def setUpClass(cls) -> None:
        """Start AlloyDB Omni + Fake GCS Server containers and apply schema."""
        # --- AlloyDB Omni ---
        cls.db_container = PostgresContainer(
            image="google/alloydbomni:15",
            username="postgres",
            password="postgres",
            dbname="postgres",
            driver=None,
        )
        cls.db_container.start()
        cls._db_host = cls.db_container.get_container_host_ip()
        cls._db_port = int(cls.db_container.get_exposed_port(5432))

        async def _setup_schema() -> None:
            conn = await asyncpg.connect(
                host=cls._db_host,
                port=cls._db_port,
                user="postgres",
                password="postgres",
                database="postgres",
            )
            for sql_file in sorted(_SQL_DIR.glob("*.sql")):
                if "pg_cron" in sql_file.name:
                    continue  # pg_cron extension is production-only (AlloyDB flag)
                await conn.execute(sql_file.read_text())
            await conn.close()

        asyncio.run(_setup_schema())

        # --- Fake GCS Server ---
        cls.gcs_container = (
            DockerContainer("fsouza/fake-gcs-server")
            .with_exposed_ports(_FAKE_GCS_PORT)
            .with_command(f"-scheme http -port {_FAKE_GCS_PORT}")
            .waiting_for(LogMessageWaitStrategy("server started at"))
        )
        cls.gcs_container.start()

        cls._gcs_host = cls.gcs_container.get_container_host_ip()
        cls._gcs_port = int(cls.gcs_container.get_exposed_port(_FAKE_GCS_PORT))
        cls._gcs_url = f"http://{cls._gcs_host}:{cls._gcs_port}"

        # Create test bucket
        resp = sync_requests.post(
            f"{cls._gcs_url}/storage/v1/b",
            json={"name": _TEST_BUCKET},
            timeout=10,
        )
        resp.raise_for_status()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.gcs_container.stop()
        cls.db_container.stop()

    async def asyncSetUp(self) -> None:
        self.pool = await asyncpg.create_pool(
            host=self._db_host,
            port=self._db_port,
            user="postgres",
            password="postgres",
            database="postgres",
            min_size=2,
            max_size=5,
        )
        await self.pool.execute("TRUNCATE feeds CASCADE")
        self.store = FeedStore(self.pool)
        self.worker_id = uuid.uuid4()

        os.environ["STORAGE_EMULATOR_HOST"] = self._gcs_url
        self.gcs_client = gcs_client.GcsClient()

    async def asyncTearDown(self) -> None:
        await self.gcs_client.close()
        os.environ.pop("STORAGE_EMULATOR_HOST", None)
        await self.pool.close()

    # -- Helpers ----------------------------------------------------------

    async def _insert_feed(
        self,
        name: str,
        *,
        source_feed_id: str | None = "wmata",
    ) -> uuid.UUID:
        """Insert an unclaimed openmhz feed, optionally with properties."""
        feed_id = await self.pool.fetchval(
            "INSERT INTO feeds (name, source_type, status)"
            " VALUES ($1, 'openmhz', 'unclaimed')"
            " RETURNING id",
            name,
        )
        if source_feed_id is not None:
            await self.pool.execute(
                "INSERT INTO feed_properties"
                " (feed_id, source_feed_id, source_type)"
                " VALUES ($1::uuid, $2, $3)",
                str(feed_id),
                source_feed_id,
                "openmhz",
            )
        return feed_id

    async def _get_feed_row(self, feed_id: uuid.UUID) -> dict:
        row = await self.pool.fetchrow(
            "SELECT status, failure_count, worker_id,"
            " last_processed_filename"
            " FROM feeds WHERE id = $1::uuid",
            str(feed_id),
        )
        if row is None:
            msg = "Expected a feed row"
            raise AssertionError(msg)
        return dict(row)

    async def _download_gcs_object(self, object_name: str) -> bytes:
        import aiohttp  # noqa: PLC0415

        url = (
            f"{self._gcs_url}/download/storage/v1/b/"
            f"{_TEST_BUCKET}/o/{object_name}?alt=media"
        )
        async with (
            aiohttp.ClientSession() as session,
            session.get(url) as resp,
        ):
            resp.raise_for_status()
            return await resp.read()

    async def _lease_feed(self, name: str) -> LeasedFeed:
        """Insert and acquire the feed via the production claim path."""
        await self._insert_feed(name)
        leased = await self.store.acquire_feeds_batch(self.worker_id, _CLAIM)
        if not leased:
            msg = (
                "Expected a LeasedFeed from acquire_feeds_batch, got empty list"
            )
            raise AssertionError(msg)
        return leased[0]

    # -- Tests ------------------------------------------------------------

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a", new_callable=AsyncMock)
    async def test_capture_upload_and_bookmark(
        self,
        mock_download: AsyncMock,
        mock_transport,
    ) -> None:
        """Happy path: lease -> capture 1 call -> upload to GCS -> bookmark."""
        feed = await self._lease_feed("integration-feed")

        call = _make_call(call_id="c1", length_sec=5)
        mock_transport.side_effect = lambda *a, **kw: _mock_transport([call])
        mock_download.return_value = _make_m4a_bytes()

        shutdown = asyncio.Event()
        chunks_uploaded = []
        async for event in openmhz_collector(
            feed,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            chunk = _require_captured_chunk(event)
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client,
                chunk.audio_bytes,
                feed,
                _TEST_BUCKET,
                len(chunks_uploaded),
            )
            ok = await self.store.update_feed_progress(
                feed["id"],
                self.worker_id,
                gcs_path,
                feed["fencing_token"],
                chunk.chunk_start_time,
            )
            self.assertTrue(ok)
            chunks_uploaded.append((chunk.audio_bytes, gcs_path))
            shutdown.set()

        # Assert: exactly 1 chunk uploaded
        self.assertEqual(len(chunks_uploaded), 1)

        # Assert: GCS object matches downloaded M4A
        _, gcs_path = chunks_uploaded[0]
        object_name = gcs_path.replace(f"gs://{_TEST_BUCKET}/", "")
        downloaded = await self._download_gcs_object(object_name)
        self.assertEqual(downloaded, _make_m4a_bytes())

        # Assert: DB bookmark updated
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_path)
        self.assertEqual(row["failure_count"], 0)

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a", new_callable=AsyncMock)
    async def test_multiple_calls_uploaded_to_gcs(
        self,
        mock_download: AsyncMock,
        mock_transport,
    ) -> None:
        """3 calls captured and uploaded to GCS."""
        feed = await self._lease_feed("multi-call-feed")

        calls = [
            _make_call(call_id="c1"),
            _make_call(call_id="c2"),
            _make_call(call_id="c3"),
        ]
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(calls)
        mock_download.return_value = _make_m4a_bytes()

        shutdown = asyncio.Event()
        gcs_paths = []
        seq = 0
        async for event in openmhz_collector(
            feed,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            chunk = _require_captured_chunk(event)
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client, chunk.audio_bytes, feed, _TEST_BUCKET, seq
            )
            await self.store.update_feed_progress(
                feed["id"],
                self.worker_id,
                gcs_path,
                feed["fencing_token"],
                chunk.chunk_start_time,
            )
            gcs_paths.append(gcs_path)
            seq += 1
            if seq == 3:
                shutdown.set()

        self.assertEqual(len(gcs_paths), 3)

        for path in gcs_paths:
            object_name = path.replace(f"gs://{_TEST_BUCKET}/", "")
            downloaded = await self._download_gcs_object(object_name)
            self.assertEqual(downloaded, _make_m4a_bytes())

        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_paths[-1])

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a", new_callable=AsyncMock)
    async def test_shutdown_stops_before_next_session(
        self,
        mock_download: AsyncMock,
        mock_transport,
    ) -> None:
        """Shutdown after 1st session: no reconnect, only session-1 calls uploaded."""
        feed = await self._lease_feed("shutdown-feed")

        # First session yields 1 call; second session would yield more
        # but shutdown fires between sessions so it's never entered.
        session_num = 0

        def _transport_factory(*a, **kw):
            nonlocal session_num
            session_num += 1
            if session_num == 1:
                return _mock_transport([_make_call(call_id="c1")])
            return _mock_transport(
                [
                    _make_call(call_id="c2"),
                    _make_call(call_id="c3"),
                ]
            )

        mock_transport.side_effect = _transport_factory
        mock_download.return_value = _make_m4a_bytes()

        shutdown = asyncio.Event()
        gcs_paths = []
        seq = 0
        async for event in openmhz_collector(
            feed,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            chunk = _require_captured_chunk(event)
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client, chunk.audio_bytes, feed, _TEST_BUCKET, seq
            )
            await self.store.update_feed_progress(
                feed["id"],
                self.worker_id,
                gcs_path,
                feed["fencing_token"],
                chunk.chunk_start_time,
            )
            gcs_paths.append(gcs_path)
            seq += 1
            shutdown.set()  # fires between sessions

        self.assertEqual(len(gcs_paths), 1)

        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_paths[0])

    @patch(f"{_COL_MOD}.websocket_transport")
    @patch(f"{_COL_MOD}._download_m4a", new_callable=AsyncMock)
    async def test_zero_length_call_skipped(
        self,
        mock_download: AsyncMock,
        mock_transport,
    ) -> None:
        """Zero-length call skipped, next call processed."""
        feed = await self._lease_feed("zero-len-feed")

        calls = [
            _make_call(call_id="zero", length_sec=0),
            _make_call(call_id="normal", length_sec=5),
        ]
        mock_transport.side_effect = lambda *a, **kw: _mock_transport(calls)
        mock_download.return_value = _make_m4a_bytes()

        shutdown = asyncio.Event()
        gcs_paths = []
        async for event in openmhz_collector(
            feed,
            shutdown,
            "https://api.openmhz.com/",
            _default_resources(),
        ):
            chunk = _require_captured_chunk(event)
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client, chunk.audio_bytes, feed, _TEST_BUCKET, 0
            )
            gcs_paths.append(gcs_path)
            shutdown.set()

        self.assertEqual(len(gcs_paths), 1)
        mock_download.assert_called_once()

    async def test_missing_source_feed_id_raises_without_side_effects(
        self,
    ) -> None:
        """Feed without feed_properties -> typed failure, no GCS upload."""
        # Insert a valid feed
        feed_id = await self._insert_feed("no-id-feed")
        leased = await self.store.acquire_feeds_batch(self.worker_id, _CLAIM)
        if not leased:
            msg = (
                "Expected a LeasedFeed from acquire_feeds_batch, got empty list"
            )
            raise AssertionError(msg)
        feed = leased[0]

        # Mock missing source_feed_id on the loaded feed object
        feed["source_feed_id"] = None

        shutdown = asyncio.Event()
        with self.assertRaises(FeedFailure) as ctx:
            async for _ in openmhz_collector(
                feed,
                shutdown,
                "https://api.openmhz.com/",
                _default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(ctx.exception), "missing_source_feed_id")

        row = await self._get_feed_row(feed_id)
        self.assertEqual(row["status"], "active")
        self.assertIsNone(row["last_processed_filename"])


if __name__ == "__main__":
    unittest.main()
