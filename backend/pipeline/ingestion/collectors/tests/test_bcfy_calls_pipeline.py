"""Tests for the minimal SID-owned Broadcastify Calls Feed pipeline."""

from __future__ import annotations

import asyncio
import datetime
import types
import typing
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion import grant_control, models
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    bcfy_calls_collector,
    pipeline,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_NOW = datetime.datetime(2026, 7, 19, 12, 0, tzinfo=datetime.UTC)


def _grant() -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key="123",
        owner_worker_id=_OWNER_ID,
        fencing_token=7,
    )


def _member() -> ingestion_lease_store.LeaseMember:
    return ingestion_lease_store.LeaseMember(
        identity=ingestion_lease_store.LeaseMemberIdentity(
            feed_id=_FEED_ID,
            source_type=feed_store.SourceType.BCFY_CALLS,
            source_feed_id="123-456",
        ),
        name="Dispatch",
        last_bookmark_time=None,
        retry_after=None,
    )


def _work(index: int) -> pipeline.CallWork:
    return pipeline.CallWork(
        payload={
            "ts": int(_NOW.timestamp()) + index,
            "start_ts": int(_NOW.timestamp()) + index,
            "end_ts": int(_NOW.timestamp()) + index + 1,
        },
        audio_url=f"https://audio.example/{index}.mp3",
    )


def _chunk(index: int) -> models.CapturedChunk:
    start = _NOW + datetime.timedelta(seconds=index)
    return models.CapturedChunk(
        audio_bytes=f"audio-{index}".encode(),
        chunk_start_time=start,
        chunk_end_time=start + datetime.timedelta(seconds=1),
        session_id="session-1",
        receipt_time=_NOW,
        mime_type=models.AudioMimeType.MPEG,
        resume_position=start,
        external_audio_segment_id=(f"https://audio.example/{index}.mp3"),
    )


def _batch(*works: pipeline.CallWork) -> pipeline.FeedBatch:
    return pipeline.FeedBatch(
        grant=_grant(),
        member=_member(),
        session_id="session-1",
        starting_sequence=10,
        calls=tuple(works),
    )


def _settings() -> types.SimpleNamespace:
    return types.SimpleNamespace(
        audio_staging_bucket="audio-staging",
        gcs_upload_max_retries=0,
        gcs_upload_retry_base_delay_sec=0.0,
        gcs_upload_retry_max_delay_sec=0.0,
        pubsub_publish_max_retries=0,
        pubsub_publish_retry_base_delay_sec=0.0,
        pubsub_publish_retry_max_delay_sec=0.0,
    )


def _committed(
    disposition: ingestion_lease_store.ChildDisposition = (
        ingestion_lease_store.ChildDisposition.COMMITTED
    ),
) -> ingestion_lease_store.BatchCommitted:
    return ingestion_lease_store.BatchCommitted(
        children=(
            ingestion_lease_store.ChildMutationResult(
                feed_id=_FEED_ID,
                disposition=disposition,
            ),
        ),
    )


class _Store:
    def __init__(
        self,
        *results: (
            ingestion_lease_store.BatchCommitted
            | ingestion_lease_store.GrantRejected
            | BaseException
        ),
    ) -> None:
        self._results = list(results)
        self.calls: list[
            tuple[
                ingestion_lease_store.LeaseGrant,
                ingestion_lease_store.ChildMutationBatch,
                str,
            ]
        ] = []

    async def commit_child_mutations(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        batch: ingestion_lease_store.ChildMutationBatch,
        *,
        actor_id: str,
    ) -> (
        ingestion_lease_store.BatchCommitted
        | ingestion_lease_store.GrantRejected
    ):
        self.calls.append((grant, batch, actor_id))
        result = self._results.pop(0) if self._results else _committed()
        if isinstance(result, BaseException):
            raise result
        return result


def _executor(store: _Store) -> pipeline.BcfyCallsFeedBatchExecutor:
    return pipeline.BcfyCallsFeedBatchExecutor(
        calls_provider=mock.MagicMock(),
        lease_store=typing.cast(
            "ingestion_lease_store.IngestionLeaseStore",
            store,
        ),
        gcs_client=mock.MagicMock(),
        pubsub_client=mock.MagicMock(),
        settings=typing.cast(
            "pipeline.settings.CollectorSettings",
            _settings(),
        ),
        topic_path="projects/test/topics/segmented",
        actor_id="service_account:gcp:test",
    )


class TestFeedBatchExecution(unittest.IsolatedAsyncioTestCase):
    @mock.patch.object(
        bcfy_calls_collector,
        "_create_chunk_from_call",
        new_callable=mock.AsyncMock,
    )
    @mock.patch.object(
        pipeline.gcp_helper,
        "upload_staged_audio",
        new_callable=mock.AsyncMock,
    )
    @mock.patch.object(
        pipeline.gcp_helper,
        "publish_audio_chunk",
        new_callable=mock.AsyncMock,
    )
    async def test_calls_run_sequentially_in_physical_order(
        self,
        publish: mock.AsyncMock,
        upload: mock.AsyncMock,
        create_chunk: mock.AsyncMock,
    ) -> None:
        events: list[str] = []
        store = _Store(_committed(), _committed())

        async def create(*args: object, **kwargs: object) -> object:
            del args, kwargs
            index = create_chunk.await_count - 1
            events.append(f"download-{index}")
            return bcfy_calls_collector._CallChunkResult(chunk=_chunk(index))

        async def stage(*args: object, **kwargs: object) -> str:
            del kwargs
            sequence = args[4]
            events.append(f"upload-{sequence}")
            return f"gs://audio/{sequence}.mp3"

        async def send(*args: object, **kwargs: object) -> str:
            del kwargs
            gcs_uri = typing.cast("str", args[4])
            events.append(f"publish-{gcs_uri.rsplit('/', 1)[-1]}")
            return "message-id"

        create_chunk.side_effect = create
        upload.side_effect = stage
        publish.side_effect = send

        result = await _executor(store).execute(_batch(_work(0), _work(1)))

        self.assertEqual(
            events,
            [
                "download-0",
                "upload-10",
                "publish-10.mp3",
                "download-1",
                "upload-11",
                "publish-11.mp3",
            ],
        )
        self.assertEqual(result.attempted_count, 2)
        self.assertEqual(result.published_count, 2)
        self.assertEqual(result.next_sequence, 12)
        self.assertEqual(
            result.committed_urls,
            (
                "https://audio.example/0.mp3",
                "https://audio.example/1.mp3",
            ),
        )
        self.assertIsNone(result.terminal)
        self.assertEqual(len(store.calls), 2)
        for grant, batch, actor_id in store.calls:
            self.assertEqual(grant, _grant())
            self.assertEqual(actor_id, "service_account:gcp:test")
            self.assertIsInstance(
                batch.lease_effect,
                ingestion_lease_store.NoLeaseEffect,
            )
            self.assertEqual(len(batch.mutations), 1)
            self.assertIsInstance(
                batch.mutations[0],
                ingestion_lease_store.AdmittedAudioProgress,
            )

    @mock.patch.object(
        bcfy_calls_collector,
        "_create_chunk_from_call",
        new_callable=mock.AsyncMock,
    )
    @mock.patch.object(
        pipeline.gcp_helper,
        "upload_staged_audio",
        new_callable=mock.AsyncMock,
        return_value="gs://audio/11.mp3",
    )
    @mock.patch.object(
        pipeline.gcp_helper,
        "publish_audio_chunk",
        new_callable=mock.AsyncMock,
        return_value="message-id",
    )
    async def test_media_failure_continues_and_success_suppresses_promotion(
        self,
        _publish: mock.AsyncMock,
        _upload: mock.AsyncMock,
        create_chunk: mock.AsyncMock,
    ) -> None:
        create_chunk.side_effect = (
            bcfy_calls_collector._CallChunkResult(
                failure=failure_classification.ItemFailure(
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                    "missing_audio",
                )
            ),
            bcfy_calls_collector._CallChunkResult(chunk=_chunk(1)),
        )

        result = await _executor(_Store(_committed())).execute(
            _batch(_work(0), _work(1))
        )

        self.assertEqual(result.attempted_count, 2)
        self.assertEqual(result.published_count, 1)
        self.assertEqual(result.next_sequence, 12)
        self.assertEqual(
            result.committed_urls,
            ("https://audio.example/1.mp3",),
        )
        self.assertIsNone(result.terminal)

    @mock.patch.object(
        bcfy_calls_collector,
        "_create_chunk_from_call",
        new_callable=mock.AsyncMock,
    )
    async def test_all_media_failures_promote_shared_item_failure(
        self,
        create_chunk: mock.AsyncMock,
    ) -> None:
        failure = failure_classification.ItemFailure(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "missing_audio",
        )
        create_chunk.side_effect = (
            bcfy_calls_collector._CallChunkResult(failure=failure),
            bcfy_calls_collector._CallChunkResult(failure=failure),
        )
        store = _Store()

        result = await _executor(store).execute(_batch(_work(0), _work(1)))

        self.assertEqual(result.attempted_count, 2)
        self.assertEqual(result.published_count, 0)
        self.assertEqual(result.next_sequence, 12)
        self.assertEqual(result.committed_urls, ())
        self.assertEqual(result.terminal, failure)
        self.assertEqual(store.calls, [])

    @mock.patch.object(
        bcfy_calls_collector,
        "_create_chunk_from_call",
        new_callable=mock.AsyncMock,
        return_value=bcfy_calls_collector._CallChunkResult(chunk=_chunk(0)),
    )
    @mock.patch.object(
        pipeline.gcp_helper,
        "upload_staged_audio",
        new_callable=mock.AsyncMock,
        side_effect=OSError("gcs unavailable"),
    )
    async def test_direct_upload_failure_stops_the_batch(
        self,
        _upload: mock.AsyncMock,
        _create_chunk: mock.AsyncMock,
    ) -> None:
        result = await _executor(_Store()).execute(_batch(_work(0), _work(1)))

        self.assertEqual(result.attempted_count, 1)
        self.assertEqual(result.next_sequence, 11)
        self.assertEqual(
            result.terminal,
            failure_classification.ItemFailure(
                feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                "gcs_upload_failed",
            ),
        )

    @mock.patch.object(
        bcfy_calls_collector,
        "_create_chunk_from_call",
        new_callable=mock.AsyncMock,
        return_value=bcfy_calls_collector._CallChunkResult(chunk=_chunk(0)),
    )
    @mock.patch.object(
        pipeline.gcp_helper,
        "upload_staged_audio",
        new_callable=mock.AsyncMock,
        return_value="gs://audio/10.mp3",
    )
    async def test_grant_and_member_rejections_stop_without_failure(
        self,
        _upload: mock.AsyncMock,
        _create_chunk: mock.AsyncMock,
    ) -> None:
        rejection = ingestion_lease_store.GrantRejected(
            ingestion_lease_store.GrantRejectionReason.FENCE_MISMATCH
        )
        cases = (
            (rejection, rejection),
            (
                _committed(ingestion_lease_store.ChildDisposition.REJECTED),
                None,
            ),
        )

        for index, (commit, terminal) in enumerate(cases):
            with self.subTest(index=index):
                result = await _executor(_Store(commit)).execute(
                    _batch(_work(0))
                )

                self.assertEqual(result.committed_urls, ())
                self.assertEqual(result.terminal, terminal)

    @mock.patch.object(
        bcfy_calls_collector,
        "_create_chunk_from_call",
        new_callable=mock.AsyncMock,
        return_value=bcfy_calls_collector._CallChunkResult(chunk=_chunk(0)),
    )
    @mock.patch.object(
        pipeline.gcp_helper,
        "upload_staged_audio",
        new_callable=mock.AsyncMock,
        return_value="gs://audio/10.mp3",
    )
    async def test_commit_exception_and_cancellation_are_outcome_unknown(
        self,
        _upload: mock.AsyncMock,
        _create_chunk: mock.AsyncMock,
    ) -> None:
        for error in (OSError("db unavailable"), asyncio.CancelledError()):
            with self.subTest(error=type(error).__name__):
                with self.assertRaisesRegex(
                    grant_control.GrantControlIntegrityError,
                    "outcome is unknown",
                ):
                    await _executor(_Store(error)).execute(_batch(_work(0)))

    @mock.patch.object(
        bcfy_calls_collector,
        "_create_chunk_from_call",
        new_callable=mock.AsyncMock,
        return_value=bcfy_calls_collector._CallChunkResult(chunk=_chunk(0)),
    )
    @mock.patch.object(
        pipeline.gcp_helper,
        "upload_staged_audio",
        new_callable=mock.AsyncMock,
        return_value="gs://audio/10.mp3",
    )
    @mock.patch.object(
        pipeline.gcp_helper,
        "publish_audio_chunk",
        new_callable=mock.AsyncMock,
        side_effect=RuntimeError("schema validation failed"),
    )
    async def test_publish_failure_keeps_committed_url_and_stops_batch(
        self,
        _publish: mock.AsyncMock,
        _upload: mock.AsyncMock,
        _create_chunk: mock.AsyncMock,
    ) -> None:
        result = await _executor(_Store(_committed())).execute(
            _batch(_work(0), _work(1))
        )

        self.assertEqual(
            result.committed_urls,
            ("https://audio.example/0.mp3",),
        )
        self.assertEqual(result.attempted_count, 1)
        self.assertIsInstance(
            result.terminal,
            failure_classification.ItemFailure,
        )
        assert isinstance(
            result.terminal,
            failure_classification.ItemFailure,
        )
        self.assertIs(
            result.terminal.status_reason,
            feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
        )
        self.assertIn("schema validation", result.terminal.reason.lower())

    @mock.patch.object(
        bcfy_calls_collector,
        "_create_chunk_from_call",
        new_callable=mock.AsyncMock,
        return_value=bcfy_calls_collector._CallChunkResult(chunk=_chunk(0)),
    )
    @mock.patch.object(
        pipeline.gcp_helper,
        "upload_staged_audio",
        new_callable=mock.AsyncMock,
        return_value="gs://audio/10.mp3",
    )
    async def test_cancellation_waits_for_postcommit_publish_settlement(
        self,
        _upload: mock.AsyncMock,
        _create_chunk: mock.AsyncMock,
    ) -> None:
        publish_started = asyncio.Event()
        allow_publish = asyncio.Event()
        publish_settled = asyncio.Event()

        async def publish(*args: object, **kwargs: object) -> str:
            del args, kwargs
            publish_started.set()
            await allow_publish.wait()
            publish_settled.set()
            return "message-id"

        with mock.patch.object(
            pipeline.gcp_helper,
            "publish_audio_chunk",
            side_effect=publish,
        ):
            task = asyncio.create_task(
                _executor(_Store(_committed())).execute(_batch(_work(0)))
            )
            await publish_started.wait()
            task.cancel()
            await asyncio.sleep(0)

            self.assertFalse(task.done())
            self.assertFalse(publish_settled.is_set())

            allow_publish.set()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertTrue(publish_settled.is_set())

    async def test_publish_completion_race_still_propagates_cancellation(
        self,
    ) -> None:
        async def exercise_race() -> str:
            owner = asyncio.current_task()
            self.assertIsNotNone(owner)

            async def publish() -> str:
                asyncio.get_running_loop().call_soon(
                    typing.cast("asyncio.Task[object]", owner).cancel
                )
                return "message-id"

            return await pipeline._settle_postcommit_publish(publish())

        task = asyncio.create_task(exercise_race())
        with self.assertRaises(asyncio.CancelledError):
            await task
