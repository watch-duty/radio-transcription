from __future__ import annotations

import asyncio
import os
import struct
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import asyncpg
import docker
import requests as sync_requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from backend.pipeline.storage.feed_store import FeedStore

# Must patch env vars BEFORE importing icecast_collector (module-level sys.exit guard)
MOCK_ENV_VARS = {
    "BROADCASTIFY_USERNAME": "test_user",
    "BROADCASTIFY_PASSWORD": "test_pass",
}

with patch.dict(os.environ, MOCK_ENV_VARS, clear=False):
    from backend.pipeline.ingestion.collectors import icecast_collector

from backend.pipeline.ingestion import gcs

_REPO_ROOT = Path(__file__).resolve().parents[5]
_SQL_DIR = _REPO_ROOT / "terraform" / "modules" / "alloydb" / "sql" / "ingestion"

_FAKE_GCS_PORT = 4443
_TEST_BUCKET = "test-audio-bucket"

# Audio constants (mirrors icecast_collector.py)
_BYTES_PER_CHUNK = 480_000
_WAV_HEADER_SIZE = 44


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
        """Create pool, truncate feeds, configure GCS singleton for fake server."""
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

        # GCS setup: reset singleton and point at fake server
        # gcs._get_storage() creates Storage(session=s) without api_root.
        # Storage.__init__ calls init_api_root(None) which checks
        # STORAGE_EMULATOR_HOST env var. When set, _api_is_dev=True
        # (disables SSL, _headers() returns {} skipping auth).
        gcs._session = None
        gcs._storage = None
        os.environ["STORAGE_EMULATOR_HOST"] = self._gcs_url

    async def asyncTearDown(self) -> None:
        """Close GCS client, remove env var, close pool."""
        await gcs.close_client()
        os.environ.pop("STORAGE_EMULATOR_HOST", None)
        await self.pool.close()

    # -- Helpers ----------------------------------------------------------

    async def _insert_feed(
        self,
        name: str,
        *,
        stream_url: str | None = "http://example.com/stream",
    ) -> uuid.UUID:
        """Insert an unclaimed feed row, optionally with icecast properties."""
        feed_id = await self.pool.fetchval(
            "INSERT INTO feeds (name, source_type, status)"
            " VALUES ($1, 'bcfy_feeds', 'unclaimed')"
            " RETURNING id",
            name,
        )
        if stream_url is not None:
            await self.pool.execute(
                "INSERT INTO feed_properties_icecast (feed_id, stream_url)"
                " VALUES ($1::uuid, $2)",
                str(feed_id),
                stream_url,
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

    def _mock_ffmpeg_process(
        self,
        pcm_data_reads: list[bytes],
        exit_code: int = 0,
    ) -> AsyncMock:
        """Create a mock ffmpeg process yielding given data reads then EOF."""
        mock_proc = AsyncMock()
        mock_proc.pid = 12345
        mock_proc.returncode = None
        mock_proc.stdout.read.side_effect = [*pcm_data_reads, b""]

        def _set_returncode(*_args):
            mock_proc.returncode = exit_code
            return exit_code

        mock_proc.wait = AsyncMock(side_effect=_set_returncode)
        return mock_proc

    async def _download_gcs_object(self, object_name: str) -> bytes:
        """Download an object from the fake GCS server via HTTP."""
        import aiohttp

        url = f"{self._gcs_url}/download/storage/v1/b/{_TEST_BUCKET}/o/{object_name}?alt=media"
        async with aiohttp.ClientSession() as session, session.get(url) as resp:
            resp.raise_for_status()
            return await resp.read()

    def _parse_wav_header(self, wav_data: bytes) -> dict:
        """Parse the 44-byte WAV header and return field dict."""
        self.assertTrue(len(wav_data) >= _WAV_HEADER_SIZE)
        riff = wav_data[0:4]
        file_size = struct.unpack_from("<I", wav_data, 4)[0]
        wave = wav_data[8:12]
        fmt = wav_data[12:16]
        (
            subchunk1_size,
            audio_format,
            num_channels,
            sample_rate,
            byte_rate,
            block_align,
            bits_per_sample,
        ) = struct.unpack_from("<IHHIIHH", wav_data, 16)
        data_tag = wav_data[36:40]
        data_size = struct.unpack_from("<I", wav_data, 40)[0]
        return {
            "riff": riff,
            "file_size": file_size,
            "wave": wave,
            "fmt": fmt,
            "subchunk1_size": subchunk1_size,
            "audio_format": audio_format,
            "num_channels": num_channels,
            "sample_rate": sample_rate,
            "byte_rate": byte_rate,
            "block_align": block_align,
            "bits_per_sample": bits_per_sample,
            "data_tag": data_tag,
            "data_size": data_size,
        }

    # -- Tests ------------------------------------------------------------

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_capture_upload_and_bookmark(self, mock_create_ffmpeg) -> None:
        """Happy path: lease -> capture 1 chunk -> upload to GCS -> bookmark in DB."""
        # Arrange: insert feed, lease it
        await self._insert_feed("integration-feed")
        feed = await self.store.lease_feed(self.worker_id)
        self.assertIsNotNone(feed)

        # Mock ffmpeg: return exactly 1 chunk of PCM data then EOF
        pcm_data = b"\x80" * _BYTES_PER_CHUNK
        mock_proc = self._mock_ffmpeg_process([pcm_data])
        mock_create_ffmpeg.return_value = mock_proc

        # Act: capture -> upload -> bookmark
        shutdown = asyncio.Event()
        chunks_uploaded = []
        async for wav_chunk in icecast_collector.capture_icecast_stream(feed, shutdown):
            gcs_path = await gcs.upload_audio(
                wav_chunk, feed, _TEST_BUCKET, len(chunks_uploaded),
            )
            ok = await self.store.update_feed_progress(
                feed["id"], self.worker_id, gcs_path,
            )
            self.assertTrue(ok)
            chunks_uploaded.append((wav_chunk, gcs_path))

        # Assert: exactly 1 chunk uploaded
        self.assertEqual(len(chunks_uploaded), 1)

        # Assert: GCS object content matches yielded WAV
        wav_chunk, gcs_path = chunks_uploaded[0]
        # gcs_path is "gs://bucket/source_type/feed_id/timestamp_seq.wav"
        object_name = gcs_path.replace(f"gs://{_TEST_BUCKET}/", "")
        downloaded = await self._download_gcs_object(object_name)
        self.assertEqual(downloaded, wav_chunk)

        # Assert: DB bookmark updated
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_path)
        self.assertEqual(row["failure_count"], 0)

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_multiple_chunks_uploaded_to_gcs(self, mock_create_ffmpeg) -> None:
        """3 chunks captured, all uploaded to GCS with correct WAV data."""
        await self._insert_feed("multi-chunk-feed")
        feed = await self.store.lease_feed(self.worker_id)
        self.assertIsNotNone(feed)

        # Mock ffmpeg: return 3 chunks worth of data in one read then EOF
        pcm_data = b"\x42" * (_BYTES_PER_CHUNK * 3)
        mock_proc = self._mock_ffmpeg_process([pcm_data])
        mock_create_ffmpeg.return_value = mock_proc

        shutdown = asyncio.Event()
        gcs_paths = []
        seq = 0
        async for wav_chunk in icecast_collector.capture_icecast_stream(feed, shutdown):
            gcs_path = await gcs.upload_audio(wav_chunk, feed, _TEST_BUCKET, seq)
            await self.store.update_feed_progress(
                feed["id"], self.worker_id, gcs_path,
            )
            gcs_paths.append(gcs_path)
            seq += 1

        # Assert: 3 distinct uploads
        self.assertEqual(len(gcs_paths), 3)

        # Assert: each GCS object is downloadable and is WAV-sized
        for path in gcs_paths:
            object_name = path.replace(f"gs://{_TEST_BUCKET}/", "")
            downloaded = await self._download_gcs_object(object_name)
            self.assertEqual(len(downloaded), _BYTES_PER_CHUNK + _WAV_HEADER_SIZE)

        # Assert: DB bookmark points to last uploaded path
        row = await self._get_feed_row(feed["id"])
        self.assertEqual(row["last_processed_filename"], gcs_paths[-1])

    @patch(
        "backend.pipeline.ingestion.collectors.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_shutdown_stops_capture_after_partial_upload(
        self, mock_create_ffmpeg,
    ) -> None:
        """Shutdown after 1st chunk: generator stops, only 1 GCS object exists."""
        await self._insert_feed("shutdown-feed")
        feed = await self.store.lease_feed(self.worker_id)
        self.assertIsNotNone(feed)

        # Mock ffmpeg: 3 separate reads (shutdown checked between reads)
        chunk = b"\xAB" * _BYTES_PER_CHUNK
        mock_proc = self._mock_ffmpeg_process([chunk, chunk, chunk])
        mock_create_ffmpeg.return_value = mock_proc

        shutdown = asyncio.Event()
        gcs_paths = []
        seq = 0
        async for wav_chunk in icecast_collector.capture_icecast_stream(feed, shutdown):
            gcs_path = await gcs.upload_audio(wav_chunk, feed, _TEST_BUCKET, seq)
            await self.store.update_feed_progress(
                feed["id"], self.worker_id, gcs_path,
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
