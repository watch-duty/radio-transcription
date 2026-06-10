from __future__ import annotations

import asyncio
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import docker
import requests as sync_requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.postgres import PostgresContainer

from backend.pipeline.common import gcp_helper
from backend.pipeline.common.clients import gcs_client
from backend.pipeline.ingestion.collectors.icecast import icecast_collector
from backend.pipeline.ingestion.collectors.tests.conftest import (
    _default_resources,
)
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
    FeedStore,
    LeasedFeed,
    SourceType,
)

_CLAIM: dict[SourceType, int] = {SourceType.BCFY_FEEDS: 1}

MOCK_ENV_VARS = {
    "BROADCASTIFY_USERNAME": "test_user",
    "BROADCASTIFY_PASSWORD": "test_pass",
}

_REPO_ROOT = Path(__file__).resolve().parents[5]
_SQL_DIR = (
    _REPO_ROOT / "terraform" / "modules" / "alloydb" / "sql" / "ingestion"
)

_FAKE_GCS_PORT = 4443
_TEST_BUCKET = "test-audio-bucket"

# Audio constants
_FLAC_MAGIC = b"fLaC"


def _docker_available() -> bool:
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


@unittest.skipUnless(_docker_available(), "Docker is not available")
class TestIcecastCollectorIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for icecast_collector with real GCS and DB."""

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
                content = sql_file.read_text()
                if "ALTER TYPE" in content:
                    lines = [
                        line
                        for line in content.splitlines()
                        if not line.strip().startswith("ALTER TYPE")
                    ]
                    content = "\n".join(lines)
                await conn.execute(content)
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

        # Create test bucket via fake-gcs-server JSON API
        resp = sync_requests.post(
            f"{cls._gcs_url}/storage/v1/b",
            json={"name": _TEST_BUCKET},
        )
        resp.raise_for_status()

    @classmethod
    def tearDownClass(cls) -> None:
        """Stop containers."""
        cls.gcs_container.stop()
        cls.db_container.stop()

    async def asyncSetUp(self) -> None:
        """Create pool, truncate feeds, and configure a GCS client for fake server."""
        # DB setup
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

        # Broadcastify env vars for _build_auth_header()
        os.environ.update(MOCK_ENV_VARS)
        # Point Storage at fake-gcs-server via emulator host and use an explicit
        # client instance, since gcp_helper no longer exposes singleton helpers.
        os.environ["STORAGE_EMULATOR_HOST"] = self._gcs_url
        self.gcs_client = gcs_client.GcsClient()

    async def asyncTearDown(self) -> None:
        """Close GCS client, remove env var, and close pool."""
        await self.gcs_client.close()
        os.environ.pop("STORAGE_EMULATOR_HOST", None)
        for key in MOCK_ENV_VARS:
            os.environ.pop(key, None)
        await self.pool.close()

    # -- Helpers ----------------------------------------------------------

    async def _insert_feed(
        self,
        name: str,
        *,
        source_feed_id: str | None = "123",
    ) -> uuid.UUID:
        """Insert an unclaimed feed row, optionally with feed properties."""
        feed_id = await self.pool.fetchval(
            "INSERT INTO feeds (name, source_type, status)"
            " VALUES ($1, 'bcfy_feeds', 'unclaimed')"
            " RETURNING id",
            name,
        )
        if source_feed_id is not None:
            await self.pool.execute(
                "INSERT INTO feed_properties (feed_id, source_feed_id)"
                " VALUES ($1::uuid, $2)",
                str(feed_id),
                source_feed_id,
            )
        return feed_id

    async def _get_feed_row(self, feed_id: uuid.UUID) -> dict:
        """Read back feed status fields from the database."""
        row = await self.pool.fetchrow(
            "SELECT status, failure_count, worker_id,"
            " last_processed_filename, last_bookmark_time"
            " FROM feeds WHERE id = $1::uuid",
            str(feed_id),
        )
        if row is None:
            msg = "Expected a feed row"
            raise AssertionError(msg)
        return dict(row)

    def _mock_create_ffmpeg(
        self,
        segments: list[bytes],
        *,
        exit_code: int = 0,
        wait_delay: float = 0.01,
        wait_exception: Exception | None = None,
    ):
        """Create side effect for _create_ffmpeg_process that writes segment files."""

        async def _factory(
            _url: str, segment_pattern: str, _auth: str = ""
        ) -> AsyncMock:
            segment_dir = Path(segment_pattern).parent
            for index, segment in enumerate(segments):
                (segment_dir / f"chunk_{index:06d}.flac").write_bytes(segment)

            mock_proc = AsyncMock()
            mock_proc.pid = 12345
            mock_proc.returncode = None
            mock_proc.terminate = MagicMock()
            mock_proc.stderr.readline = AsyncMock(return_value=b"")

            async def _wait_impl() -> int:
                await asyncio.sleep(wait_delay)
                if wait_exception is not None:
                    raise wait_exception
                mock_proc.returncode = exit_code
                return exit_code

            mock_proc.wait = AsyncMock(side_effect=_wait_impl)
            return mock_proc

        return _factory

    async def _download_gcs_object(self, object_name: str) -> bytes:
        """Download an object from the fake GCS server via HTTP."""
        import aiohttp  # noqa: PLC0415

        url = f"{self._gcs_url}/download/storage/v1/b/{_TEST_BUCKET}/o/{object_name}?alt=media"
        async with aiohttp.ClientSession() as session, session.get(url) as resp:
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

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_capture_upload_and_bookmark(
        self, mock_create_ffmpeg
    ) -> None:
        """Happy path: lease -> capture 1 chunk -> upload to GCS -> bookmark in DB."""
        # Arrange: insert feed, lease it
        feed = await self._lease_feed("integration-feed")

        # Mock ffmpeg: one finalized FLAC segment
        flac_chunk = _FLAC_MAGIC + b"\x80" * 1024
        mock_create_ffmpeg.side_effect = self._mock_create_ffmpeg([flac_chunk])

        # Act: capture -> upload -> bookmark
        shutdown = asyncio.Event()
        chunks_uploaded = []
        last_chunk_ts = None
        async for capture_chunk in icecast_collector.capture_icecast_stream(
            feed,
            shutdown,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        ):
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client,
                capture_chunk.audio_bytes,
                feed,
                _TEST_BUCKET,
                len(chunks_uploaded),
            )
            ok = await self.store.update_feed_progress(
                feed["id"],
                self.worker_id,
                gcs_path,
                feed["fencing_token"],
                capture_chunk.chunk_end_time,
            )
            self.assertTrue(ok)
            last_chunk_ts = capture_chunk.chunk_end_time
            chunks_uploaded.append((capture_chunk.audio_bytes, gcs_path))

        # Assert: exactly 1 chunk uploaded
        self.assertEqual(len(chunks_uploaded), 1)

        # Assert: GCS object content matches yielded segment bytes
        yielded_chunk, gcs_path = chunks_uploaded[0]
        # gcs_path is "gs://bucket/source_type/feed_id/timestamp_seq.wav"
        object_name = gcs_path.replace(f"gs://{_TEST_BUCKET}/", "")
        downloaded = await self._download_gcs_object(object_name)
        self.assertEqual(downloaded, yielded_chunk)
        self.assertEqual(downloaded[:4], _FLAC_MAGIC)

        # Assert: DB bookmark updated
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_path)
        self.assertEqual(row["last_bookmark_time"], last_chunk_ts)
        self.assertEqual(row["failure_count"], 0)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_multiple_chunks_uploaded_to_gcs(
        self, mock_create_ffmpeg
    ) -> None:
        """3 segments captured and uploaded to GCS."""
        feed = await self._lease_feed("multi-chunk-feed")

        # Mock ffmpeg: three finalized FLAC segments
        segments = [
            _FLAC_MAGIC + b"\x42" * 120,
            _FLAC_MAGIC + b"\x24" * 140,
            _FLAC_MAGIC + b"\x66" * 160,
        ]
        mock_create_ffmpeg.side_effect = self._mock_create_ffmpeg(segments)

        shutdown = asyncio.Event()
        gcs_paths = []
        chunk_timestamps = []
        seq = 0
        async for capture_chunk in icecast_collector.capture_icecast_stream(
            feed,
            shutdown,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        ):
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client,
                capture_chunk.audio_bytes,
                feed,
                _TEST_BUCKET,
                seq,
            )
            await self.store.update_feed_progress(
                feed["id"],
                self.worker_id,
                gcs_path,
                feed["fencing_token"],
                capture_chunk.chunk_end_time,
            )
            gcs_paths.append(gcs_path)
            chunk_timestamps.append(capture_chunk.chunk_end_time)
            seq += 1

        # Assert: 3 distinct uploads
        self.assertEqual(len(gcs_paths), 3)

        # Assert: each GCS object is downloadable and looks like FLAC bytes
        for i, path in enumerate(gcs_paths):
            object_name = path.replace(f"gs://{_TEST_BUCKET}/", "")
            downloaded = await self._download_gcs_object(object_name)
            self.assertEqual(downloaded, segments[i])
            self.assertEqual(downloaded[:4], _FLAC_MAGIC)

        # Assert: DB bookmark points to last uploaded path and timestamp
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_paths[-1])
        self.assertEqual(row["last_bookmark_time"], chunk_timestamps[-1])

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_shutdown_stops_capture_after_partial_upload(
        self,
        mock_create_ffmpeg,
    ) -> None:
        """Shutdown after 1st chunk: generator stops, only 1 GCS object exists."""
        feed = await self._lease_feed("shutdown-feed")

        # Mock ffmpeg: 3 finalized segments; shutdown is checked between yields
        segments = [
            _FLAC_MAGIC + b"\xab" * 90,
            _FLAC_MAGIC + b"\xcd" * 100,
            _FLAC_MAGIC + b"\xef" * 110,
        ]
        mock_create_ffmpeg.side_effect = self._mock_create_ffmpeg(segments)

        shutdown = asyncio.Event()
        gcs_paths = []
        last_chunk_ts = None
        seq = 0
        async for capture_chunk in icecast_collector.capture_icecast_stream(
            feed,
            shutdown,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        ):
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client,
                capture_chunk.audio_bytes,
                feed,
                _TEST_BUCKET,
                seq,
            )
            await self.store.update_feed_progress(
                feed["id"],
                self.worker_id,
                gcs_path,
                feed["fencing_token"],
                capture_chunk.chunk_end_time,
            )
            gcs_paths.append(gcs_path)
            last_chunk_ts = capture_chunk.chunk_end_time
            seq += 1
            # Signal shutdown after first chunk
            shutdown.set()

        # Assert: only 1 chunk made it through
        self.assertEqual(len(gcs_paths), 1)

        # Assert: DB bookmark reflects single upload
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_paths[0])
        self.assertEqual(row["last_bookmark_time"], last_chunk_ts)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_reports_failure_in_db(
        self,
        mock_create_ffmpeg,
    ) -> None:
        """Ffmpeg exit code 1 -> RuntimeError -> feed status = 'failing'."""
        feed = await self._lease_feed("error-feed")

        # Mock ffmpeg: no segments and non-zero exit
        mock_create_ffmpeg.side_effect = self._mock_create_ffmpeg(
            [], exit_code=1
        )

        shutdown = asyncio.Event()
        with patch(
            "backend.pipeline.ingestion.collectors.icecast.icecast_collector._probe_stream_once",
            new_callable=AsyncMock,
            return_value=None,
        ):
            with self.assertRaises(FeedFailure) as ctx:
                async for _chunk in icecast_collector.capture_icecast_stream(
                    feed,
                    shutdown,
                    url_base="https://mock.example.com/",
                    resources=_default_resources(),
                ):
                    pass  # Should not yield any chunks

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(str(ctx.exception), "ffmpeg_exit_1")

        # Simulate what CollectorRuntime._process_feed does on exception
        await self.store.report_feed_failure(
            feed["id"],
            self.worker_id,
            feed["fencing_token"],
            reason=str(ctx.exception),
            status_reason=ctx.exception.status_reason,
        )

        # Assert: feed transitioned to 'failing'
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["status"], "failing")
        self.assertEqual(row["failure_count"], 1)
        self.assertIsNone(row["worker_id"])

    @patch(
        "backend.pipeline.ingestion.collectors.icecast.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_flac_segment_bytes_roundtrip(
        self,
        mock_create_ffmpeg,
    ) -> None:
        """Upload FLAC segment bytes to GCS and verify exact roundtrip content."""
        feed = await self._lease_feed("flac-roundtrip-feed")

        expected_segment = _FLAC_MAGIC + bytes(range(64)) * 8
        mock_create_ffmpeg.side_effect = self._mock_create_ffmpeg(
            [expected_segment]
        )

        shutdown = asyncio.Event()
        gcs_path = None
        async for capture_chunk in icecast_collector.capture_icecast_stream(
            feed,
            shutdown,
            url_base="https://mock.example.com/",
            resources=_default_resources(),
        ):
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client,
                capture_chunk.audio_bytes,
                feed,
                _TEST_BUCKET,
                0,
            )
            break  # Only need first chunk

        if gcs_path is None:
            msg = "Expected a GCS path, got None"
            raise AssertionError(msg)
        object_name = gcs_path.replace(f"gs://{_TEST_BUCKET}/", "")
        downloaded = await self._download_gcs_object(object_name)

        self.assertEqual(downloaded, expected_segment)
        self.assertEqual(downloaded[:4], _FLAC_MAGIC)

    async def test_missing_source_feed_id_raises_without_side_effects(
        self,
    ) -> None:
        """Feed without icecast properties -> typed failure, no GCS upload."""
        # Insert a valid feed
        feed_id = await self._insert_feed("no-url-feed")
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
            async for _chunk in icecast_collector.capture_icecast_stream(
                feed,
                shutdown,
                url_base="https://mock.example.com/",
                resources=_default_resources(),
            ):
                pass

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        )
        self.assertEqual(str(ctx.exception), "missing_source_feed_id")

        # Assert: DB state unchanged (still active, no bookmark)
        row = await self._get_feed_row(feed_id)
        self.assertEqual(row["status"], "active")
        self.assertIsNone(row["last_processed_filename"])
        self.assertIsNone(row["last_bookmark_time"])


if __name__ == "__main__":
    unittest.main()
