"""Integration tests for the Echo audio ingestion handler.

Uses testcontainers for AlloyDB Omni and fake-gcs-server to verify
the full flow: MP3 upload -> feed resolution -> FLAC conversion ->
canonical bucket write -> Pub/Sub publish -> lifecycle update.
"""

from __future__ import annotations

import os
import shutil
import unittest
from functools import partial
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock, patch

if TYPE_CHECKING:
    import uuid

import docker
import psycopg
import requests as sync_requests
from google.cloud import storage
from psycopg.rows import dict_row
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.postgres import PostgresContainer

from backend.pipeline.common.test_schema_helper import sync_apply_test_schema
from backend.pipeline.ingestion.collectors.echo import main as echo_main
from backend.pipeline.schema_types.segmented_audio_pb2 import SegmentedAudio
from backend.pipeline.storage.settings import AlloyDBSettings
from backend.pipeline.storage.sync_connection import connect_db
from backend.pipeline.storage.sync_feed_store import SyncFeedStore

_FAKE_GCS_PORT = 4443
_ECHO_BUCKET = "wd-echo-recordings-test"
_STAGING_BUCKET = "ingestion-staging-test"
_SEGMENTED_PUBSUB_TOPIC_PATH = "projects/test/topics/segmented-audio-test"


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
    return b"dummy mp3 audio"


@unittest.skipUnless(_docker_available(), "Docker is not available")
@unittest.skipUnless(_ffmpeg_available(), "ffmpeg is not available")
class TestEchoCollectorIntegration(unittest.TestCase):
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

        # Apply schema using psycopg (sync)
        with psycopg.connect(
            host=cls._db_host,
            port=cls._db_port,
            user="postgres",
            password="postgres",
            dbname="postgres",
            autocommit=True,
        ) as conn:
            sync_apply_test_schema(conn)

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

        # Create test buckets
        for bucket_name in (_ECHO_BUCKET, _STAGING_BUCKET):
            resp = sync_requests.post(
                f"{cls._gcs_url}/storage/v1/b",
                json={"name": bucket_name},
            )
            resp.raise_for_status()

        # Shared DB settings for handler connections
        cls._db_settings = AlloyDBSettings(
            host=cls._db_host,
            port=cls._db_port,
            user="postgres",
            password="postgres",
            db_name="postgres",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.gcs_container.stop()
        cls.db_container.stop()

    def setUp(self) -> None:
        self.conn: psycopg.Connection[dict[str, Any]] = cast(
            "psycopg.Connection[dict[str, Any]]",
            psycopg.connect(
                host=self._db_host,
                port=self._db_port,
                user="postgres",
                password="postgres",
                dbname="postgres",
                autocommit=True,
                row_factory=cast("Any", dict_row),
            ),
        )
        self.conn.execute(
            "TRUNCATE feed_audit_events, feeds CASCADE"
        )

        # Point GCS client at fake server
        os.environ["STORAGE_EMULATOR_HOST"] = self._gcs_url
        self.gcs = storage.Client(project="test")

        # Mock publisher to capture published messages
        self.mock_publisher = MagicMock()
        self.mock_publisher.publish.return_value.result.return_value = "msg-id"

    def tearDown(self) -> None:
        os.environ.pop("STORAGE_EMULATOR_HOST", None)
        self.conn.close()

    # -- Helpers ----------------------------------------------------------

    def _insert_echo_feed(
        self, channel_name: str, *, status: str = "active"
    ) -> uuid.UUID:
        row = self.conn.execute(
            "INSERT INTO feeds (name, source_type, status, created_at)"
            " VALUES (%s, 'echo', %s::feed_status, '2026-01-01 00:00:00+00'::timestamptz)"
            " RETURNING id",
            (channel_name, status),
        ).fetchone()
        assert row is not None
        feed_id = row["id"]
        self.conn.execute(
            "INSERT INTO feed_properties (feed_id, source_type, source_feed_id)"
            " VALUES (%s, 'echo', %s)",
            (feed_id, channel_name),
        )
        return feed_id

    def _get_feed_row(self, feed_id: uuid.UUID) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT status::text, failure_count, status_reason, retry_after,"
            " status_reason_detail, quarantine_reason FROM feeds WHERE id = %s",
            (feed_id,),
        ).fetchone()
        assert row is not None
        return row

    def _get_audit_rows(self, feed_id: uuid.UUID) -> list[dict[str, Any]]:
        cursor = self.conn.execute(
            "SELECT action, actor_id, feed_revision, before_values,"
            " after_values, metadata FROM feed_audit_events"
            " WHERE feed_id = %s ORDER BY feed_revision",
            (feed_id,),
        )
        return list(cursor.fetchall())

    def _upload_mp3(self, name: str) -> bytes:
        mp3_bytes = _make_mp3_bytes()
        blob = self.gcs.bucket(_ECHO_BUCKET).blob(name)
        blob.upload_from_string(mp3_bytes, content_type="audio/mpeg")
        return mp3_bytes

    def _make_cloud_event(self, name: str) -> MagicMock:
        event = MagicMock()
        event.data = {"name": name, "bucket": _ECHO_BUCKET}
        return event

    def _download_staged(self, path: str) -> bytes:
        resp = sync_requests.get(
            f"{self._gcs_url}/storage/v1/b/{_STAGING_BUCKET}/o/{path.replace('/', '%2F')}?alt=media",
        )
        resp.raise_for_status()
        return resp.content

    def _run_handler(
        self,
        event: MagicMock,
        *,
        duration_side_effect: Exception | None = None,
    ) -> None:
        """Run _handle with real GCS + DB, mocked publisher."""
        mock_pubsub = MagicMock()
        mock_pubsub.get_publisher.return_value = self.mock_publisher

        store = SyncFeedStore(partial(connect_db, self._db_settings))
        duration_mock = MagicMock(return_value=15000)
        if duration_side_effect is not None:
            duration_mock.side_effect = duration_side_effect

        with (
            patch.object(echo_main, "gcs_client", self.gcs),
            patch.object(echo_main, "pubsub_client", mock_pubsub),
            patch.object(echo_main, "feed_store", store),
            patch.object(
                echo_main,
                "SEGMENTED_PUBSUB_TOPIC_PATH",
                _SEGMENTED_PUBSUB_TOPIC_PATH,
            ),
            patch.object(echo_main, "STAGING_BUCKET", _STAGING_BUCKET),
            patch.object(echo_main, "get_audio_duration", duration_mock),
        ):
            echo_main._handle(event)

    # -- Tests ------------------------------------------------------------

    def test_successful_mp3_pipeline(self) -> None:
        """Upload MP3 -> resolve feed -> upload -> publish."""
        channel = "fire-ca_almaden"
        feed_id = self._insert_echo_feed(channel)
        name = f"{channel}/20260326/fire_20260326_143022.mp3"
        uploaded_bytes = self._upload_mp3(name)

        self._run_handler(self._make_cloud_event(name))

        # Verify MP3 in staging bucket
        mp3_path = f"echo/{feed_id}/20260326/fire_20260326_143022.mp3"
        downloaded_bytes = self._download_staged(mp3_path)
        self.assertEqual(downloaded_bytes, uploaded_bytes)

        # Verify AudioChunk published with correct attributes
        self.mock_publisher.publish.assert_called_once()
        publish_args, call_kwargs = self.mock_publisher.publish.call_args
        self.assertEqual(call_kwargs["source_type"], "echo")
        chunk = SegmentedAudio()
        chunk.ParseFromString(publish_args[1])
        self.assertEqual(chunk.feed_id, str(feed_id))
        self.assertEqual(self._get_audit_rows(feed_id), [])

    def test_unknown_channel_skips_silently(self) -> None:
        """MP3 from unregistered channel -> no GCS write, no publish."""
        name = "unknown-channel/20260326/unknown_20260326_143022.mp3"
        self._upload_mp3(name)

        self._run_handler(self._make_cloud_event(name))

        self.mock_publisher.publish.assert_not_called()

    def test_deactivated_feed_drops_event_without_audit(self) -> None:
        """Deactivated feed -> drop event (return 200), no storage mutation."""
        channel = "deactivated-ch"
        feed_id = self._insert_echo_feed(channel, status="deactivated")
        name = f"{channel}/20260326/deactivated_20260326_143022.mp3"
        self._upload_mp3(name)

        self._run_handler(self._make_cloud_event(name))

        row = self._get_feed_row(feed_id)
        self.assertEqual(row["status"], "deactivated")
        self.assertEqual(row["failure_count"], 0)
        self.assertEqual(self._get_audit_rows(feed_id), [])
        self.mock_publisher.publish.assert_not_called()

    def test_quarantined_feed_drops_event(self) -> None:
        """Quarantined feed -> drop event (return 200), no publish."""
        channel = "quarantined-ch"
        feed_id = self._insert_echo_feed(channel, status="quarantined")
        name = f"{channel}/20260326/quarantined_20260326_143022.mp3"
        self._upload_mp3(name)

        self._run_handler(self._make_cloud_event(name))

        row = self._get_feed_row(feed_id)
        self.assertEqual(row["status"], "quarantined")
        self.assertEqual(self._get_audit_rows(feed_id), [])
        self.mock_publisher.publish.assert_not_called()

    def test_malformed_filename_skips_gracefully(self) -> None:
        """Malformed filename -> log warning, return 200, no failure recorded."""
        channel = "bad-ch"
        feed_id = self._insert_echo_feed(channel)
        name = f"{channel}/20260326/badname.mp3"
        self._upload_mp3(name)

        self._run_handler(self._make_cloud_event(name))

        row = self._get_feed_row(feed_id)
        self.assertEqual(row["failure_count"], 0)
        self.assertEqual(row["status"], "active")
        self.mock_publisher.publish.assert_not_called()

    def test_pipeline_failure_does_not_quarantine(self) -> None:
        """Pipeline failures are visible but do not consume feed budget."""
        channel = "failing-ch"
        feed_id = self._insert_echo_feed(channel)

        # Pre-set failure_count to 4 (one below threshold)
        self.conn.execute(
            "UPDATE feeds SET failure_count = 4, status = 'failing',"
            " status_reason = 'source_unreachable',"
            " status_reason_detail = 'offline'"
            " WHERE id = %s",
            (feed_id,),
        )

        name = f"{channel}/20260326/failing_20260326_143022.mp3"
        self._upload_mp3(name)

        # Simulate an infrastructure failure (Pub/Sub outage)
        self.mock_publisher.publish.return_value.result.side_effect = Exception(
            "Pub/Sub unavailable"
        )

        self._run_handler(self._make_cloud_event(name))

        row = self._get_feed_row(feed_id)
        self.assertEqual(row["failure_count"], 0)
        self.assertEqual(row["status"], "failing")
        self.assertEqual(row["status_reason"], "system_pipeline_error")
        self.assertIsNone(row["retry_after"])
        self.assertIsNone(row["quarantine_reason"])
        events = self._get_audit_rows(feed_id)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["action"], "feed.failure_reported")
        self.assertEqual(
            events[0]["actor_id"],
            echo_main.ECHO_INGESTION_ACTOR_ID,
        )
        self.assertEqual(events[0]["metadata"], {})
        self.assertEqual(events[0]["before_values"]["status"], "failing")
        self.assertEqual(events[0]["before_values"]["failure_count"], 4)
        self.assertEqual(
            events[0]["before_values"]["status_reason"],
            "source_unreachable",
        )
        self.assertEqual(events[0]["after_values"]["status"], "failing")
        self.assertEqual(
            events[0]["after_values"]["status_reason"],
            "system_pipeline_error",
        )

    def test_duration_failure_does_not_block_later_valid_object(self) -> None:
        """One bad Echo object cannot quarantine or block later objects."""
        channel = "duration-failing-ch"
        feed_id = self._insert_echo_feed(channel)

        self.conn.execute(
            "UPDATE feeds SET failure_count = 4, status = 'failing',"
            " status_reason = 'source_unreachable',"
            " status_reason_detail = 'offline'"
            " WHERE id = %s",
            (feed_id,),
        )

        bad_name = f"{channel}/20260326/bad_20260326_143022.mp3"
        good_name = f"{channel}/20260326/good_20260326_143122.mp3"
        self._upload_mp3(bad_name)
        self._upload_mp3(good_name)

        self._run_handler(
            self._make_cloud_event(bad_name),
            duration_side_effect=ValueError("bad mp3"),
        )

        bad_row = self._get_feed_row(feed_id)
        self.assertEqual(bad_row["failure_count"], 0)
        self.assertEqual(bad_row["status"], "failing")
        self.assertEqual(bad_row["status_reason"], "system_collector_error")
        self.mock_publisher.publish.assert_not_called()

        self._run_handler(self._make_cloud_event(good_name))

        good_row = self._get_feed_row(feed_id)
        self.assertEqual(good_row["failure_count"], 0)
        self.assertEqual(good_row["status"], "active")
        self.assertIsNone(good_row["status_reason"])
        self.mock_publisher.publish.assert_called_once()
        events = self._get_audit_rows(feed_id)
        self.assertEqual(
            [event["action"] for event in events],
            [
                "feed.failure_reported",
                "feed.recovered",
            ],
        )
        self.assertEqual(events[0]["metadata"], {})
        self.assertEqual(
            events[0]["before_values"]["status_reason"],
            "source_unreachable",
        )
        self.assertEqual(
            events[-1]["actor_id"],
            echo_main.ECHO_INGESTION_ACTOR_ID,
        )
        self.assertEqual(events[-1]["metadata"], {})
        self.assertEqual(events[-1]["before_values"]["status"], "failing")
        self.assertEqual(
            events[-1]["before_values"]["status_reason"],
            "system_collector_error",
        )
        self.assertEqual(events[-1]["before_values"]["status"], "failing")
        self.assertEqual(events[-1]["after_values"]["status"], "active")

    def test_success_resets_failure_count(self) -> None:
        """Successful processing resets failure_count to 0."""
        channel = "recovering-ch"
        feed_id = self._insert_echo_feed(channel)

        # Pre-set a previous failure
        self.conn.execute(
            "UPDATE feeds SET failure_count = 2, status = 'failing',"
            " status_reason = 'source_unreachable',"
            " status_reason_detail = 'offline'"
            " WHERE id = %s",
            (feed_id,),
        )

        name = f"{channel}/20260326/recovering_20260326_143022.mp3"
        self._upload_mp3(name)

        self._run_handler(self._make_cloud_event(name))

        row = self._get_feed_row(feed_id)
        self.assertEqual(row["failure_count"], 0)
        self.assertEqual(row["status"], "active")
        events = self._get_audit_rows(feed_id)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["action"], "feed.recovered")
        self.assertEqual(
            events[0]["actor_id"],
            echo_main.ECHO_INGESTION_ACTOR_ID,
        )
        self.assertEqual(events[0]["metadata"], {})
        self.assertEqual(events[0]["before_values"]["status"], "failing")
        self.assertEqual(events[0]["before_values"]["failure_count"], 2)
        self.assertEqual(
            events[0]["before_values"]["status_reason"],
            "source_unreachable",
        )
        self.assertEqual(events[0]["before_values"]["status"], "failing")
        self.assertEqual(events[0]["after_values"]["status"], "active")

    def test_non_mp3_file_ignored(self) -> None:
        """Non-MP3 file -> return immediately, no DB lookup."""
        name = "some-channel/20260326/notes.txt"
        self._run_handler(self._make_cloud_event(name))
        self.mock_publisher.publish.assert_not_called()

    def test_idempotent_redelivery(self) -> None:
        """Same file processed twice -> both invocations publish."""
        channel = "idempotent-ch"
        feed_id = self._insert_echo_feed(channel)
        name = f"{channel}/20260326/idempotent_20260326_143022.mp3"
        self._upload_mp3(name)

        self._run_handler(self._make_cloud_event(name))
        self._run_handler(self._make_cloud_event(name))

        # Verify MP3 still valid
        mp3_path = f"echo/{feed_id}/20260326/idempotent_20260326_143022.mp3"
        downloaded_bytes = self._download_staged(mp3_path)
        self.assertEqual(downloaded_bytes, _make_mp3_bytes())

        # Both invocations publish — second upload skipped via
        # if_generation_match=0 but we always proceed to publish.
        self.assertEqual(self.mock_publisher.publish.call_count, 2)
