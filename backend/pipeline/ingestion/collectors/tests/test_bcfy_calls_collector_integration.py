from __future__ import annotations

import asyncio
import io
import os
import unittest
import uuid
from unittest.mock import AsyncMock, patch

import asyncpg
import docker
import numpy as np
import requests as sync_requests
import soundfile as sf
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.postgres import PostgresContainer

from backend.pipeline.common import gcp_helper
from backend.pipeline.common.clients import gcs_client
from backend.pipeline.common.test_schema_helper import async_apply_test_schema
from backend.pipeline.ingestion.collectors.bcfy_calls import provider
from backend.pipeline.ingestion.collectors.bcfy_calls.bcfy_calls_collector import (
    capture_bcfy_calls,
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

_CLAIM: dict[SourceType, int] = {SourceType.BCFY_CALLS: 1}

_FAKE_GCS_PORT = 4443
_TEST_BUCKET = "test-audio-bucket"
_FLAC_MAGIC = b"fLaC"
_RUNTIME_ACTOR_ID = "service_account:gcp:123456789012345678901"

_PROVIDER_MOD = "backend.pipeline.ingestion.collectors.bcfy_calls.provider"


def _docker_available() -> bool:
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


def _make_flac_bytes() -> bytes:
    """Generate a valid 1-second silent FLAC file using pydub."""
    segment = np.zeros(((1000) * 16), dtype=np.int16)
    buf = io.BytesIO()

    sf.write(buf, segment, 16000, format="FLAC")
    return buf.getvalue()


@unittest.skipUnless(_docker_available(), "Docker is not available")
class TestBcfyCallsCollectorIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for capture_bcfy_calls with real GCS and DB."""

    db_container: PostgresContainer
    gcs_container: DockerContainer

    @classmethod
    def setUpClass(cls) -> None:
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
            await async_apply_test_schema(conn)
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
        provider._reset_jwt_cache_for_tests()
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
        self.gcs = gcs_client.GcsClient()

    async def asyncTearDown(self) -> None:
        await self.gcs.close()
        os.environ.pop("STORAGE_EMULATOR_HOST", None)
        await self.pool.close()
        provider._reset_jwt_cache_for_tests()

    # -- Helpers ----------------------------------------------------------

    async def _insert_feed(
        self,
        name: str,
        *,
        source_feed_id: str | None = "12345",
    ) -> uuid.UUID:
        feed_id = await self.pool.fetchval(
            "INSERT INTO feeds (name, source_type, status)"
            " VALUES ($1, 'bcfy_calls', 'unclaimed')"
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
                "bcfy_calls",
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

    @patch(f"{_PROVIDER_MOD}._get_jwt_token")
    @patch(f"{_PROVIDER_MOD}._fetch_calls", new_callable=AsyncMock)
    @patch(f"{_PROVIDER_MOD}._download_audio", new_callable=AsyncMock)
    @patch(
        f"{_PROVIDER_MOD}.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_capture_upload_and_bookmark(
        self,
        mock_sleep: AsyncMock,
        mock_dl: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt,
    ) -> None:
        """Happy path: lease -> capture 1 call -> upload to GCS -> bookmark."""
        feed = await self._lease_feed("integration-feed")
        mock_jwt.return_value = "token"
        mock_dl.return_value = _make_flac_bytes()

        fetch_calls = 0

        async def _fetch_side_effect(*args, **kwargs):
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls == 1:
                return {
                    "calls": [
                        {
                            "url": "http://audio/1.mp3",
                            "start_ts": 1000,
                            "end_ts": 1005,
                        },
                    ],
                    "lastPos": 1005,
                }
            return None

        mock_fetch.side_effect = _fetch_side_effect
        mock_sleep.return_value = False

        shutdown = asyncio.Event()
        chunks_uploaded = []
        async for event in capture_bcfy_calls(
            feed,
            shutdown,
            "http://api.example.com/",
            _default_resources(),
        ):
            chunk = _require_captured_chunk(event)
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs,
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
                actor_id=_RUNTIME_ACTOR_ID,
            )
            self.assertTrue(ok)
            chunks_uploaded.append(gcs_path)
            shutdown.set()

        self.assertEqual(len(chunks_uploaded), 1)

        # GCS object is valid FLAC
        object_name = chunks_uploaded[0].replace(f"gs://{_TEST_BUCKET}/", "")
        downloaded = await self._download_gcs_object(object_name)
        self.assertEqual(downloaded[:4], _FLAC_MAGIC)

        # DB bookmark updated
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], chunks_uploaded[0])
        self.assertEqual(row["failure_count"], 0)

    @patch(f"{_PROVIDER_MOD}._get_jwt_token")
    @patch(f"{_PROVIDER_MOD}._fetch_calls", new_callable=AsyncMock)
    @patch(f"{_PROVIDER_MOD}._download_audio", new_callable=AsyncMock)
    @patch(
        f"{_PROVIDER_MOD}.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_multiple_calls_uploaded_to_gcs(
        self,
        mock_sleep: AsyncMock,
        mock_dl: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt,
    ) -> None:
        """3 calls captured and uploaded to GCS."""
        feed = await self._lease_feed("multi-call-feed")
        mock_jwt.return_value = "token"
        mock_dl.return_value = _make_flac_bytes()

        fetch_calls = 0

        async def _fetch_side_effect(*args, **kwargs):
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls == 1:
                return {
                    "calls": [
                        {
                            "url": "http://audio/1.mp3",
                            "start_ts": 1000,
                            "end_ts": 1005,
                        },
                        {
                            "url": "http://audio/2.mp3",
                            "start_ts": 1005,
                            "end_ts": 1010,
                        },
                        {
                            "url": "http://audio/3.mp3",
                            "start_ts": 1010,
                            "end_ts": 1015,
                        },
                    ],
                    "lastPos": 1015,
                }
            return None

        mock_fetch.side_effect = _fetch_side_effect
        mock_sleep.return_value = False

        shutdown = asyncio.Event()
        gcs_paths = []
        seq = 0
        async for event in capture_bcfy_calls(
            feed,
            shutdown,
            "http://api.example.com/",
            _default_resources(),
        ):
            chunk = _require_captured_chunk(event)
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs, chunk.audio_bytes, feed, _TEST_BUCKET, seq
            )
            await self.store.update_feed_progress(
                feed["id"],
                self.worker_id,
                gcs_path,
                feed["fencing_token"],
                chunk.chunk_start_time,
                actor_id=_RUNTIME_ACTOR_ID,
            )
            gcs_paths.append(gcs_path)
            seq += 1
            if seq == 3:
                shutdown.set()

        self.assertEqual(len(gcs_paths), 3)
        for path in gcs_paths:
            object_name = path.replace(f"gs://{_TEST_BUCKET}/", "")
            downloaded = await self._download_gcs_object(object_name)
            self.assertEqual(downloaded[:4], _FLAC_MAGIC)

        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_paths[-1])

    @patch(f"{_PROVIDER_MOD}._get_jwt_token")
    @patch(f"{_PROVIDER_MOD}._fetch_calls", new_callable=AsyncMock)
    @patch(f"{_PROVIDER_MOD}._download_audio", new_callable=AsyncMock)
    @patch(
        f"{_PROVIDER_MOD}.control_flow.sleep_or_cancel",
        new_callable=AsyncMock,
    )
    async def test_session_id_set_on_chunks(
        self,
        mock_sleep: AsyncMock,
        mock_dl: AsyncMock,
        mock_fetch: AsyncMock,
        mock_jwt,
    ) -> None:
        """All chunks have session_id set and consistent."""
        feed = await self._lease_feed("session-feed")
        mock_jwt.return_value = "token"
        mock_dl.return_value = _make_flac_bytes()

        fetch_calls = 0

        async def _fetch_side_effect(*args, **kwargs):
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls == 1:
                return {
                    "calls": [
                        {
                            "url": "http://audio/1.mp3",
                            "start_ts": 1000,
                            "end_ts": 1005,
                        },
                        {
                            "url": "http://audio/2.mp3",
                            "start_ts": 1005,
                            "end_ts": 1010,
                        },
                    ],
                    "lastPos": 1010,
                }
            return None

        mock_fetch.side_effect = _fetch_side_effect
        mock_sleep.return_value = False

        shutdown = asyncio.Event()
        chunks = []
        async for event in capture_bcfy_calls(
            feed,
            shutdown,
            "http://api.example.com/",
            _default_resources(),
        ):
            chunk = _require_captured_chunk(event)
            chunks.append(chunk)
            if len(chunks) == 2:
                shutdown.set()

        self.assertEqual(len(chunks), 2)
        for chunk in chunks:
            self.assertIsNotNone(chunk.session_id)
        self.assertEqual(chunks[0].session_id, chunks[1].session_id)

    async def test_missing_source_feed_id_raises_without_side_effects(
        self,
    ) -> None:
        """Feed without source_feed_id -> typed failure, no GCS/DB writes."""
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
            async for _ in capture_bcfy_calls(
                feed,
                shutdown,
                "http://api.example.com/",
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
