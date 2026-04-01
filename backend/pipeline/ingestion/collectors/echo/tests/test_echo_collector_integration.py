"""Integration tests for the Echo audio ingestion Cloud Function.

Uses testcontainers for AlloyDB Omni and fake-gcs-server to verify
the full flow: MP3 upload -> feed resolution -> FLAC conversion ->
canonical bucket write -> Pub/Sub publish -> lifecycle update.
"""

from __future__ import annotations

import asyncio
import io
import os
import unittest
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

if TYPE_CHECKING:
    import uuid

import shutil

import asyncpg
import docker
import requests as sync_requests
from google.cloud import storage
from pydub import AudioSegment
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from backend.pipeline.ingestion.collectors.echo import main as echo_main

_REPO_ROOT = Path(__file__).resolve().parents[6]
_SQL_DIR = (
    _REPO_ROOT / "terraform" / "modules" / "alloydb" / "sql" / "ingestion"
)

_FAKE_GCS_PORT = 4443
_ECHO_BUCKET = "wd-echo-recordings-test"
_CANONICAL_BUCKET = "ingestion-canonical-test"
_RAW_AUDIO_TOPIC = "projects/test/topics/raw-audio-test"
_FLAC_MAGIC = b"fLaC"


def _docker_available() -> bool:
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _make_mp3_bytes(
    *, sample_rate: int = 8000, duration_ms: int = 500
) -> bytes:
    audio = AudioSegment.silent(duration=duration_ms, frame_rate=sample_rate)
    buf = io.BytesIO()
    audio.export(buf, format="mp3")
    return buf.getvalue()


@unittest.skipUnless(_docker_available(), "Docker is not available")
@unittest.skipUnless(_ffmpeg_available(), "ffmpeg is not available")
class TestEchoCollectorIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for echo ingestion with real GCS and DB."""

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

        # Create test buckets
        for bucket_name in (_ECHO_BUCKET, _CANONICAL_BUCKET):
            resp = sync_requests.post(
                f"{cls._gcs_url}/storage/v1/b",
                json={"name": bucket_name},
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

        # Point GCS client at fake server
        os.environ["STORAGE_EMULATOR_HOST"] = self._gcs_url
        self.gcs = storage.Client(project="test")

        # Mock publisher to capture published messages
        self.mock_publisher = MagicMock()
        self.published_future = MagicMock()
        self.published_future.result = MagicMock(return_value="msg-id")
        self.mock_publisher.publish.return_value = self.published_future

    async def asyncTearDown(self) -> None:
        os.environ.pop("STORAGE_EMULATOR_HOST", None)
        await self.pool.close()

    # -- Helpers ----------------------------------------------------------

    async def _insert_echo_feed(
        self, channel_name: str, *, status: str = "active"
    ) -> uuid.UUID:
        feed_id = await self.pool.fetchval(
            "INSERT INTO feeds (name, source_type, status)"
            " VALUES ($1, 'echo', $2::feed_status)"
            " RETURNING id",
            channel_name,
            status,
        )
        await self.pool.execute(
            "INSERT INTO feed_properties_echo (feed_id, channel_name)"
            " VALUES ($1, $2)",
            feed_id,
            channel_name,
        )
        return feed_id

    async def _get_feed_row(self, feed_id: uuid.UUID) -> dict:
        row = await self.pool.fetchrow(
            "SELECT status::text, failure_count FROM feeds WHERE id = $1",
            feed_id,
        )
        return dict(row)

    def _upload_mp3(self, name: str) -> bytes:
        mp3_bytes = _make_mp3_bytes()
        blob = self.gcs.bucket(_ECHO_BUCKET).blob(name)
        blob.upload_from_string(mp3_bytes, content_type="audio/mpeg")
        return mp3_bytes

    def _make_cloud_event(self, name: str) -> MagicMock:
        event = MagicMock()
        event.data = {"name": name, "bucket": _ECHO_BUCKET}
        return event

    def _download_canonical(self, path: str) -> bytes:
        resp = sync_requests.get(
            f"{self._gcs_url}/storage/v1/b/{_CANONICAL_BUCKET}/o/{path.replace('/', '%2F')}?alt=media",
        )
        resp.raise_for_status()
        return resp.content

    async def _run_handler(self, event: MagicMock) -> None:
        """Run _handle with real GCS + DB, mocked publisher."""
        with (
            patch.object(echo_main, "gcs_client", self.gcs),
            patch.object(echo_main, "publisher", self.mock_publisher),
            patch.object(echo_main, "RAW_AUDIO_TOPIC", _RAW_AUDIO_TOPIC),
            patch.object(echo_main, "CANONICAL_BUCKET", _CANONICAL_BUCKET),
            patch.object(
                echo_main,
                "_get_pool",
                new_callable=AsyncMock,
                return_value=self.pool,
            ),
        ):
            await echo_main._handle(event)

    # -- Tests ------------------------------------------------------------

    async def test_successful_mp3_to_flac_pipeline(self) -> None:
        """Upload MP3 -> resolve feed -> convert FLAC -> upload -> publish."""
        channel = "fire-ca_almaden"
        feed_id = await self._insert_echo_feed(channel)
        name = f"{channel}/20260326/fire_20260326_143022.mp3"
        self._upload_mp3(name)

        await self._run_handler(self._make_cloud_event(name))

        # Verify FLAC in canonical bucket
        flac_path = f"echo/{feed_id}/20260326/fire_20260326_143022.flac"
        flac_bytes = self._download_canonical(flac_path)
        self.assertTrue(flac_bytes[:4] == _FLAC_MAGIC)

        # Verify FLAC is 16kHz mono 16-bit
        audio = AudioSegment.from_file(io.BytesIO(flac_bytes), format="flac")
        self.assertEqual(audio.frame_rate, 16000)
        self.assertEqual(audio.channels, 1)
        self.assertEqual(audio.sample_width, 2)

        # Verify AudioChunk published with correct attributes
        self.mock_publisher.publish.assert_called_once()
        call_kwargs = self.mock_publisher.publish.call_args
        self.assertEqual(call_kwargs.kwargs["feed_id"], str(feed_id))
        self.assertEqual(call_kwargs.kwargs["ordering_key"], str(feed_id))
        self.assertEqual(call_kwargs.kwargs["source_type"], "echo")
        self.assertEqual(
            call_kwargs.kwargs["chunk_uri"],
            f"gs://{_CANONICAL_BUCKET}/{flac_path}",
        )

    async def test_unknown_channel_skips_silently(self) -> None:
        """MP3 from unregistered channel -> no GCS write, no publish."""
        name = "unknown-channel/20260326/unknown_20260326_143022.mp3"
        self._upload_mp3(name)

        await self._run_handler(self._make_cloud_event(name))

        self.mock_publisher.publish.assert_not_called()

    async def test_quarantined_feed_skips(self) -> None:
        """Quarantined feed -> skip processing."""
        channel = "quarantined-ch"
        await self._insert_echo_feed(channel, status="quarantined")
        name = f"{channel}/20260326/quarantined_20260326_143022.mp3"
        self._upload_mp3(name)

        await self._run_handler(self._make_cloud_event(name))

        self.mock_publisher.publish.assert_not_called()

    async def test_malformed_filename_records_failure(self) -> None:
        """Malformed filename -> failure recorded in DB."""
        channel = "bad-ch"
        feed_id = await self._insert_echo_feed(channel)
        name = f"{channel}/20260326/badname.mp3"
        self._upload_mp3(name)

        with self.assertRaises(ValueError, msg="Cannot parse timestamp"):
            await self._run_handler(self._make_cloud_event(name))

        row = await self._get_feed_row(feed_id)
        self.assertEqual(row["failure_count"], 1)
        self.assertEqual(row["status"], "failing")

    async def test_failure_increments_to_quarantine(self) -> None:
        """5 consecutive failures -> feed quarantined."""
        channel = "failing-ch"
        feed_id = await self._insert_echo_feed(channel)

        # Pre-set failure_count to 4 (one below threshold)
        await self.pool.execute(
            "UPDATE feeds SET failure_count = 4, status = 'failing'"
            " WHERE id = $1",
            feed_id,
        )

        name = f"{channel}/20260326/badname.mp3"
        self._upload_mp3(name)

        with self.assertRaises(ValueError):
            await self._run_handler(self._make_cloud_event(name))

        row = await self._get_feed_row(feed_id)
        self.assertEqual(row["failure_count"], 5)
        self.assertEqual(row["status"], "quarantined")

    async def test_success_resets_failure_count(self) -> None:
        """Successful processing resets failure_count to 0."""
        channel = "recovering-ch"
        feed_id = await self._insert_echo_feed(channel)

        # Pre-set a previous failure
        await self.pool.execute(
            "UPDATE feeds SET failure_count = 2, status = 'failing'"
            " WHERE id = $1",
            feed_id,
        )

        name = f"{channel}/20260326/recovering_20260326_143022.mp3"
        self._upload_mp3(name)

        await self._run_handler(self._make_cloud_event(name))

        row = await self._get_feed_row(feed_id)
        self.assertEqual(row["failure_count"], 0)
        self.assertEqual(row["status"], "active")

    async def test_non_mp3_file_ignored(self) -> None:
        """Non-MP3 file -> return immediately, no DB lookup."""
        name = "some-channel/20260326/notes.txt"
        await self._run_handler(self._make_cloud_event(name))
        self.mock_publisher.publish.assert_not_called()

    async def test_idempotent_overwrite(self) -> None:
        """Same file processed twice -> second invocation is a no-op (no duplicate publish)."""
        channel = "idempotent-ch"
        feed_id = await self._insert_echo_feed(channel)
        name = f"{channel}/20260326/idempotent_20260326_143022.mp3"
        self._upload_mp3(name)

        await self._run_handler(self._make_cloud_event(name))
        await self._run_handler(self._make_cloud_event(name))

        # Verify FLAC still valid
        flac_path = f"echo/{feed_id}/20260326/idempotent_20260326_143022.flac"
        flac_bytes = self._download_canonical(flac_path)
        self.assertTrue(flac_bytes[:4] == _FLAC_MAGIC)

        # if_generation_match=0 causes the second upload to raise PreconditionFailed,
        # so we return early and publish only once — preventing duplicate AudioChunks.
        self.assertEqual(self.mock_publisher.publish.call_count, 1)
