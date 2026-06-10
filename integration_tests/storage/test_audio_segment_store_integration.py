from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

import pytest
from deepdiff import DeepDiff

from backend.pipeline.storage.audio_segment_store import (
    AudioSegmentStore,
    SortOrder,
)
from backend.services.audio_segments.models import (
    AnnotationType,
    AudioClassification,
)
from integration_tests.storage.storage_feed_util import create_feed


@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> AudioSegmentStore:
    """Provides an AudioSegmentStore instance with a clean database."""
    await db_pool.execute("TRUNCATE feeds CASCADE")
    return AudioSegmentStore(db_pool)


@pytest.fixture
async def test_segments(
    store: AudioSegmentStore, db_pool: asyncpg.Pool
) -> dict:
    feed_id = await create_feed(db_pool)

    ts1 = datetime.datetime(2026, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    ts2 = datetime.datetime(2026, 1, 1, 10, 1, 0, tzinfo=datetime.UTC)
    ts3 = datetime.datetime(2026, 1, 1, 10, 2, 0, tzinfo=datetime.UTC)
    ts4 = datetime.datetime(2026, 1, 1, 10, 3, 0, tzinfo=datetime.UTC)
    ts5 = datetime.datetime(2026, 1, 1, 10, 4, 0, tzinfo=datetime.UTC)

    # Segment 1: No alert (evaluation with empty decisions, and transcript)
    segment_no_alert = await store.create_audio_segment(
        segment_id=str(uuid.uuid4()),
        feed_id=str(feed_id),
        classification=AudioClassification.SPEECH_DETECTED,
        start_timestamp=ts1,
        end_timestamp=ts1 + datetime.timedelta(seconds=30),
        source_audio_uris=["gs://bucket/s1.ogg"],
        missing_prior_context=False,
        missing_post_context=False,
    )
    await store.add_annotation(
        segment_id=segment_no_alert.id,
        annotation_type=AnnotationType.EVALUATION,
        data={"decisions": [], "errors": []},
    )
    await store.add_annotation(
        segment_id=segment_no_alert.id,
        annotation_type=AnnotationType.TRANSCRIPT,
        data={"text": "s1 transcript", "errors": []},
    )

    # Segment 2: Has evaluation with decision, but no transcript
    segment_no_transcript = await store.create_audio_segment(
        segment_id=str(uuid.uuid4()),
        feed_id=str(feed_id),
        classification=AudioClassification.SPEECH_DETECTED,
        start_timestamp=ts2,
        end_timestamp=ts2 + datetime.timedelta(seconds=30),
        source_audio_uris=["gs://bucket/s2.ogg"],
        missing_prior_context=False,
        missing_post_context=False,
    )
    await store.add_annotation(
        segment_id=segment_no_transcript.id,
        annotation_type=AnnotationType.EVALUATION,
        data={"decisions": ["rule-1"], "errors": []},
    )

    # Segment 3: Has transcript, but no evaluation
    segment_no_evaluation = await store.create_audio_segment(
        segment_id=str(uuid.uuid4()),
        feed_id=str(feed_id),
        classification=AudioClassification.SPEECH_DETECTED,
        start_timestamp=ts3,
        end_timestamp=ts3 + datetime.timedelta(seconds=30),
        source_audio_uris=["gs://bucket/s3.ogg"],
        missing_prior_context=False,
        missing_post_context=False,
    )
    await store.add_annotation(
        segment_id=segment_no_evaluation.id,
        annotation_type=AnnotationType.TRANSCRIPT,
        data={"text": "s3 transcript", "errors": []},
    )

    # Segment 4: No annotations
    segment_no_annotation = await store.create_audio_segment(
        segment_id=str(uuid.uuid4()),
        feed_id=str(feed_id),
        classification=AudioClassification.SPEECH_DETECTED,
        start_timestamp=ts4,
        end_timestamp=ts4 + datetime.timedelta(seconds=30),
        source_audio_uris=["gs://bucket/s4.ogg"],
        missing_prior_context=False,
        missing_post_context=False,
    )

    # Segment 5: Has alert (transcript and evaluation with decisions)
    segment_has_alert = await store.create_audio_segment(
        segment_id=str(uuid.uuid4()),
        feed_id=str(feed_id),
        classification=AudioClassification.SPEECH_DETECTED,
        start_timestamp=ts5,
        end_timestamp=ts5 + datetime.timedelta(seconds=30),
        source_audio_uris=["gs://bucket/s5.ogg"],
        missing_prior_context=False,
        missing_post_context=False,
    )
    await store.add_annotation(
        segment_id=segment_has_alert.id,
        annotation_type=AnnotationType.EVALUATION,
        data={"decisions": ["rule-2"], "errors": []},
    )
    await store.add_annotation(
        segment_id=segment_has_alert.id,
        annotation_type=AnnotationType.TRANSCRIPT,
        data={"text": "s5 transcript", "errors": []},
    )

    return {
        "feed_id": feed_id,
        "segment_no_alert": segment_no_alert,
        "segment_no_transcript": segment_no_transcript,
        "segment_no_evaluation": segment_no_evaluation,
        "segment_no_annotation": segment_no_annotation,
        "segment_has_alert": segment_has_alert,
    }


async def test_create_and_list_audio_segments(
    db_pool: asyncpg.Pool, store: AudioSegmentStore
) -> None:
    feed_id = await create_feed(db_pool)

    start_time = datetime.datetime(2026, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2026, 1, 1, 10, 1, 0, tzinfo=datetime.UTC)

    # 1. Create
    segment = await store.create_audio_segment(
        segment_id=str(uuid.uuid4()),
        feed_id=str(feed_id),
        classification=AudioClassification.SPEECH_DETECTED,
        start_timestamp=start_time,
        end_timestamp=end_time,
        source_audio_uris=["gs://bucket/audio1.ogg"],
        missing_prior_context=False,
        missing_post_context=False,
    )

    assert segment.id is not None
    assert segment.feed_id == str(feed_id)
    assert segment.classification == AudioClassification.SPEECH_DETECTED
    assert segment.start_timestamp == start_time
    assert segment.end_timestamp == end_time
    assert segment.source_audio_uris == ["gs://bucket/audio1.ogg"]

    # 2. List
    result = await store.list_audio_segments()
    assert len(result.segments) == 1
    assert result.segments[0].id == segment.id

    # 3. List with feed filter
    result_filtered = await store.list_audio_segments(feed_ids=[str(feed_id)])
    assert len(result_filtered.segments) == 1
    assert result_filtered.segments[0].id == segment.id

    # List with non-existent feed
    result_empty = await store.list_audio_segments(feed_ids=[str(uuid.uuid4())])
    assert len(result_empty.segments) == 0


async def test_add_annotation(
    db_pool: asyncpg.Pool, store: AudioSegmentStore
) -> None:
    feed_id = await create_feed(db_pool)

    start_time = datetime.datetime(2026, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2026, 1, 1, 10, 1, 0, tzinfo=datetime.UTC)

    segment = await store.create_audio_segment(
        segment_id=str(uuid.uuid4()),
        feed_id=str(feed_id),
        classification=AudioClassification.SPEECH_DETECTED,
        start_timestamp=start_time,
        end_timestamp=end_time,
        source_audio_uris=["gs://bucket/audio1.ogg"],
        missing_prior_context=False,
        missing_post_context=False,
    )

    # Add annotation
    annotation_data = {"text": "hello world", "errors": []}
    annotation = await store.add_annotation(
        segment_id=segment.id,
        annotation_type=AnnotationType.TRANSCRIPT,
        data=annotation_data,
    )

    assert annotation.audio_segment_id == segment.id
    assert annotation.type == AnnotationType.TRANSCRIPT
    assert annotation.data.model_dump() == annotation_data

    # Verify annotation is included when listing segments
    result = await store.list_audio_segments()
    assert len(result.segments) == 1
    assert len(result.segments[0].annotations) == 1
    assert result.segments[0].annotations[0].type == AnnotationType.TRANSCRIPT
    assert (
        result.segments[0].annotations[0].data.model_dump() == annotation_data
    )


class TestListAudioSegmentsFilters:
    async def test_has_alert_true(
        self, store: AudioSegmentStore, test_segments: dict
    ) -> None:
        res = await store.list_audio_segments(has_alert=True)
        assert len(res.segments) == 2

        expected_no_trans = test_segments["segment_no_transcript"].model_dump()
        expected_no_trans["annotations"] = [
            {
                "audio_segment_id": test_segments["segment_no_transcript"].id,
                "type": AnnotationType.EVALUATION,
                "data": {"decisions": ["rule-1"], "errors": []},
            }
        ]

        expected_has_alert = test_segments["segment_has_alert"].model_dump()
        expected_has_alert["annotations"] = [
            {
                "audio_segment_id": test_segments["segment_has_alert"].id,
                "type": AnnotationType.EVALUATION,
                "data": {"decisions": ["rule-2"], "errors": []},
            },
            {
                "audio_segment_id": test_segments["segment_has_alert"].id,
                "type": AnnotationType.TRANSCRIPT,
                "data": {"text": "s5 transcript", "errors": []},
            },
        ]

        # list_audio_segments defaults to DESC order, so segment_has_alert comes first.
        expected_segments = [expected_has_alert, expected_no_trans]
        actual_segments = [s.model_dump() for s in res.segments]

        diff = DeepDiff(
            actual_segments,
            expected_segments,
            ignore_order=True,
            exclude_regex_paths=[r".*\['created_at'\]"],
        )
        assert not diff, f"Difference found:\n{diff.pretty()}"

    async def test_has_no_alert(
        self, store: AudioSegmentStore, test_segments: dict
    ) -> None:
        res = await store.list_audio_segments(has_alert=False)
        assert len(res.segments) == 3

        expected_no_alert = test_segments["segment_no_alert"].model_dump()
        expected_no_alert["annotations"] = [
            {
                "audio_segment_id": test_segments["segment_no_alert"].id,
                "type": AnnotationType.EVALUATION,
                "data": {"decisions": [], "errors": []},
            },
            {
                "audio_segment_id": test_segments["segment_no_alert"].id,
                "type": AnnotationType.TRANSCRIPT,
                "data": {"text": "s1 transcript", "errors": []},
            },
        ]

        expected_no_evaluation = test_segments[
            "segment_no_evaluation"
        ].model_dump()
        expected_no_evaluation["annotations"] = [
            {
                "audio_segment_id": test_segments["segment_no_evaluation"].id,
                "type": AnnotationType.TRANSCRIPT,
                "data": {"text": "s3 transcript", "errors": []},
            }
        ]

        expected_no_annotation = test_segments[
            "segment_no_annotation"
        ].model_dump()
        expected_no_annotation["annotations"] = []

        # list_audio_segments defaults to DESC order (ts4 > ts3 > ts1).
        expected_segments = [
            expected_no_annotation,
            expected_no_evaluation,
            expected_no_alert,
        ]
        actual_segments = [s.model_dump() for s in res.segments]

        diff = DeepDiff(
            actual_segments,
            expected_segments,
            ignore_order=True,
            exclude_regex_paths=[r".*\['created_at'\]"],
        )
        assert not diff, f"Difference found:\n{diff.pretty()}"

    async def test_has_alert_unset(
        self, store: AudioSegmentStore, test_segments: dict
    ) -> None:
        res = await store.list_audio_segments(has_alert=None)
        assert len(res.segments) == 5

    async def test_start_time_filter(
        self, store: AudioSegmentStore, test_segments: dict
    ) -> None:
        res = await store.list_audio_segments(
            start_time=test_segments["segment_no_transcript"].start_timestamp
        )
        assert len(res.segments) == 4
        ids = [s.id for s in res.segments]
        assert test_segments["segment_no_transcript"].id in ids
        assert test_segments["segment_no_evaluation"].id in ids
        assert test_segments["segment_no_annotation"].id in ids
        assert test_segments["segment_has_alert"].id in ids

    async def test_end_time_filter(
        self, store: AudioSegmentStore, test_segments: dict
    ) -> None:
        res = await store.list_audio_segments(
            end_time=test_segments["segment_no_transcript"].start_timestamp
            + datetime.timedelta(seconds=30)
        )
        assert len(res.segments) == 2
        ids = [s.id for s in res.segments]
        assert test_segments["segment_no_alert"].id in ids
        assert test_segments["segment_no_transcript"].id in ids

    async def test_sort_order_asc(
        self, store: AudioSegmentStore, test_segments: dict
    ) -> None:
        res = await store.list_audio_segments(order=SortOrder.ASC)
        assert len(res.segments) == 5
        assert res.segments[0].id == test_segments["segment_no_alert"].id
        assert res.segments[1].id == test_segments["segment_no_transcript"].id
        assert res.segments[2].id == test_segments["segment_no_evaluation"].id
        assert res.segments[3].id == test_segments["segment_no_annotation"].id
        assert res.segments[4].id == test_segments["segment_has_alert"].id

    async def test_next_token(
        self, store: AudioSegmentStore, test_segments: dict
    ) -> None:
        res_limit = await store.list_audio_segments(limit=1)
        assert len(res_limit.segments) == 1
        assert res_limit.next_token is not None

        res_next = await store.list_audio_segments(
            next_token=res_limit.next_token
        )
        assert len(res_next.segments) == 4
