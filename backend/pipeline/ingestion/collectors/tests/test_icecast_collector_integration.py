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
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from backend.pipeline.common.clients import gcs_client
from backend.pipeline.storage.feed_store import FeedStore

# Must patch env vars BEFORE importing icecast_collector (module-level sys.exit guard)
MOCK_ENV_VARS = {
    "BROADCASTIFY_USERNAME": "test_user",
    "BROADCASTIFY_PASSWORD": "test_pass",
}

with (
    patch.dict(os.environ, MOCK_ENV_VARS, clear=False),
    patch("google.cloud.pubsub_v1.PublisherClient"),
):
    from backend.pipeline.ingestion.collectors import icecast_collector

from backend.pipeline.common import gcp_helper  # noqa: E402

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
                await conn.execute(sql_file.read_text())
            await conn.close()

        asyncio.run(_setup_schema())

        # --- Fake GCS Server ---
        cls.gcs_container = (
            DockerContainer("fsouza/fake-gcs-server")
            .with_exposed_ports(_FAKE_GCS_PORT)
            .with_command(f"-scheme http -port {_FAKE_GCS_PORT}")
        )
        cls.gcs_container.start()
        wait_for_logs(cls.gcs_container, "server started at")

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

        # Point Storage at fake-gcs-server via emulator host and use an explicit
        # client instance, since gcp_helper no longer exposes singleton helpers.
        os.environ["STORAGE_EMULATOR_HOST"] = self._gcs_url
        self.gcs_client = gcs_client.GcsClient()

    async def asyncTearDown(self) -> None:
        """Close GCS client, remove env var, and close pool."""
        await self.gcs_client.close()
        os.environ.pop("STORAGE_EMULATOR_HOST", None)
        await self.pool.close()

    # -- Helpers ----------------------------------------------------------

    async def _insert_feed(
        self,
        name: str,
        *,
        source_feed_id: str | None = "123",
        external_id: str = "test-external-id",
    ) -> uuid.UUID:
        """Insert an unclaimed feed row, optionally with icecast properties."""
        feed_id = await self.pool.fetchval(
            "INSERT INTO feeds (name, source_type, source_type_enum, status)"
            " VALUES ($1, 'bcfy_feeds', 1, 'unclaimed')"
            " RETURNING id",
            name,
        )
        if source_feed_id is not None:
            await self.pool.execute(
                "INSERT INTO feed_properties (feed_id, source_feed_id, external_id)"
                " VALUES ($1::uuid, $2, $3)",
                str(feed_id),
                source_feed_id,
                external_id,
            )
        return feed_id

    async def _get_feed_row(self, feed_id: uuid.UUID) -> dict:
        """Read back feed status fields from the database."""
        row = await self.pool.fetchrow(
            "SELECT status, failure_count, worker_id, last_processed_filename"
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

        async def _factory(_url: str, segment_pattern: str) -> AsyncMock:
            segment_dir = Path(segment_pattern).parent
            for index, segment in enumerate(segments):
                (segment_dir / f"chunk_{index:06d}.flac").write_bytes(segment)

            mock_proc = AsyncMock()
            mock_proc.pid = 12345
            mock_proc.returncode = None
            mock_proc.terminate = MagicMock()

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

    # -- Tests ------------------------------------------------------------

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_capture_upload_and_bookmark(
        self, mock_create_ffmpeg
    ) -> None:
        """Happy path: lease -> capture 1 chunk -> upload to GCS -> bookmark in DB."""
        # Arrange: insert feed, lease it
        await self._insert_feed("integration-feed")
        feed = await self.store.lease_feed(self.worker_id)
        if feed is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)

        # Mock ffmpeg: one finalized FLAC segment
        flac_chunk = _FLAC_MAGIC + b"\x80" * 1024
        mock_create_ffmpeg.side_effect = self._mock_create_ffmpeg([flac_chunk])

        # Act: capture -> upload -> bookmark
        shutdown = asyncio.Event()
        chunks_uploaded = []
        async for flac_chunk, _ts in icecast_collector.capture_icecast_stream(
            feed, shutdown
        ):
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client,
                flac_chunk,
                feed,
                _TEST_BUCKET,
                len(chunks_uploaded),
            )
            ok = await self.store.update_feed_progress(
                feed["id"],
                self.worker_id,
                gcs_path,
                feed["fencing_token"],
            )
            self.assertTrue(ok)
            chunks_uploaded.append((flac_chunk, gcs_path))

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
        self.assertEqual(row["failure_count"], 0)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_multiple_chunks_uploaded_to_gcs(
        self, mock_create_ffmpeg
    ) -> None:
        """3 segments captured and uploaded to GCS."""
        await self._insert_feed("multi-chunk-feed")
        feed = await self.store.lease_feed(self.worker_id)
        if feed is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)

        # Mock ffmpeg: three finalized FLAC segments
        segments = [
            _FLAC_MAGIC + b"\x42" * 120,
            _FLAC_MAGIC + b"\x24" * 140,
            _FLAC_MAGIC + b"\x66" * 160,
        ]
        mock_create_ffmpeg.side_effect = self._mock_create_ffmpeg(segments)

        shutdown = asyncio.Event()
        gcs_paths = []
        seq = 0
        async for flac_chunk, _ts in icecast_collector.capture_icecast_stream(
            feed, shutdown
        ):
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client, flac_chunk, feed, _TEST_BUCKET, seq
            )
            await self.store.update_feed_progress(
                feed["id"],
                self.worker_id,
                gcs_path,
                feed["fencing_token"],
            )
            gcs_paths.append(gcs_path)
            seq += 1

        # Assert: 3 distinct uploads
        self.assertEqual(len(gcs_paths), 3)

        # Assert: each GCS object is downloadable and looks like FLAC bytes
        for i, path in enumerate(gcs_paths):
            object_name = path.replace(f"gs://{_TEST_BUCKET}/", "")
            downloaded = await self._download_gcs_object(object_name)
            self.assertEqual(downloaded, segments[i])
            self.assertEqual(downloaded[:4], _FLAC_MAGIC)

        # Assert: DB bookmark points to last uploaded path
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_paths[-1])

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_shutdown_stops_capture_after_partial_upload(
        self,
        mock_create_ffmpeg,
    ) -> None:
        """Shutdown after 1st chunk: generator stops, only 1 GCS object exists."""
        await self._insert_feed("shutdown-feed")
        feed = await self.store.lease_feed(self.worker_id)
        if feed is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)

        # Mock ffmpeg: 3 finalized segments; shutdown is checked between yields
        segments = [
            _FLAC_MAGIC + b"\xab" * 90,
            _FLAC_MAGIC + b"\xcd" * 100,
            _FLAC_MAGIC + b"\xef" * 110,
        ]
        mock_create_ffmpeg.side_effect = self._mock_create_ffmpeg(segments)

        shutdown = asyncio.Event()
        gcs_paths = []
        seq = 0
        async for flac_chunk, _ts in icecast_collector.capture_icecast_stream(
            feed, shutdown
        ):
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client, flac_chunk, feed, _TEST_BUCKET, seq
            )
            await self.store.update_feed_progress(
                feed["id"],
                self.worker_id,
                gcs_path,
                feed["fencing_token"],
            )
            gcs_paths.append(gcs_path)
            seq += 1
            # Signal shutdown after first chunk
            shutdown.set()

        # Assert: only 1 chunk made it through
        self.assertEqual(len(gcs_paths), 1)

        # Assert: DB bookmark reflects single upload
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_paths[0])

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_ffmpeg_error_reports_failure_in_db(
        self,
        mock_create_ffmpeg,
    ) -> None:
        """Ffmpeg exit code 1 -> RuntimeError -> feed status = 'failing'."""
        await self._insert_feed("error-feed")
        feed = await self.store.lease_feed(self.worker_id)
        if feed is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)

        # Mock ffmpeg: no segments and non-zero exit
        mock_create_ffmpeg.side_effect = self._mock_create_ffmpeg(
            [], exit_code=1
        )

        shutdown = asyncio.Event()
        with self.assertRaises(RuntimeError) as ctx:
            async for _chunk in icecast_collector.capture_icecast_stream(
                feed,
                shutdown,
            ):
                pass  # Should not yield any chunks

        self.assertIn("ffmpeg exited with code 1", str(ctx.exception))

        # Simulate what NormalizerRuntime._process_feed does on exception
        await self.store.report_feed_failure(
            feed["id"], self.worker_id, feed["fencing_token"]
        )

        # Assert: feed transitioned to 'failing'
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["status"], "failing")
        self.assertEqual(row["failure_count"], 1)
        self.assertIsNone(row["worker_id"])

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_flac_segment_bytes_roundtrip(
        self,
        mock_create_ffmpeg,
    ) -> None:
        """Upload FLAC segment bytes to GCS and verify exact roundtrip content."""
        await self._insert_feed("flac-roundtrip-feed")
        feed = await self.store.lease_feed(self.worker_id)
        if feed is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)

        expected_segment = _FLAC_MAGIC + bytes(range(64)) * 8
        mock_create_ffmpeg.side_effect = self._mock_create_ffmpeg(
            [expected_segment]
        )

        shutdown = asyncio.Event()
        gcs_path = None
        async for flac_chunk, _ts in icecast_collector.capture_icecast_stream(
            feed, shutdown
        ):
            gcs_path = await gcp_helper.upload_staged_audio(
                self.gcs_client,
                flac_chunk,
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
        """Feed without icecast properties -> ValueError, no GCS upload."""
        # Insert feed WITHOUT source_feed_id (no feed_properties row)
        await self._insert_feed("no-url-feed", source_feed_id=None)
        feed = await self.store.lease_feed(self.worker_id)
        if feed is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)
        self.assertIsNone(feed["source_feed_id"])

        shutdown = asyncio.Event()
        with self.assertRaises(ValueError) as ctx:
            async for _chunk in icecast_collector.capture_icecast_stream(
                feed,
                shutdown,
            ):
                pass

        self.assertIn("missing source_feed_id", str(ctx.exception))

        # Assert: DB state unchanged (still active, no bookmark)
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["status"], "active")
        self.assertIsNone(row["last_processed_filename"])


if __name__ == "__main__":
    unittest.main()
