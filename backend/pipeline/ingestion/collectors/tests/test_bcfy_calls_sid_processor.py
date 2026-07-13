"""Deterministic contracts for one exact-grant Calls SID processor."""

from __future__ import annotations

# The processor deliberately keeps its page algorithms private and deep.
import collections
import datetime
import typing
import unittest
import uuid

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    provider,
    sid_processor,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import asyncio

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_A = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_B = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
_FEED_C = uuid.UUID("cccccccc-dddd-eeee-ffff-000000000000")
_FEED_D = uuid.UUID("dddddddd-eeee-ffff-0000-111111111111")
_FEED_E = uuid.UUID("eeeeeeee-ffff-0000-1111-222222222222")
_NOW = datetime.datetime(2026, 7, 13, 1, 0, tzinfo=datetime.UTC)
_GRANT = ingestion_lease_store.LeaseGrant(
    feed_store.SourceType.BCFY_CALLS,
    "00123",
    _OWNER_ID,
    7,
)


def _member(
    feed_id: uuid.UUID,
    source_feed_id: str,
    *,
    cursor: datetime.datetime | None = _NOW,
) -> ingestion_lease_store.LeaseMember:
    sid, group_id = source_feed_id.split("-", maxsplit=1)
    identity = ingestion_lease_store._issue_member_identity(
        _GRANT,
        feed_id=feed_id,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id=source_feed_id,
        sid=sid,
        group_id=group_id,
    )
    return ingestion_lease_store.LeaseMember(
        identity=identity,
        status=feed_store.FeedStatus.ACTIVE,
        last_processed_filename=None,
        last_bookmark_time=cursor,
        failure_count=0,
        retry_after=None,
        status_reason=None,
        status_reason_detail=None,
        audit_revision=1,
    )


def _snapshot(
    revision: int,
    *members: ingestion_lease_store.LeaseMember,
) -> ingestion_lease_store.MembershipSnapshot:
    return ingestion_lease_store.MembershipSnapshot(
        grant=_GRANT,
        membership_revision=revision,
        members=members,
        excluded_count=0,
    )


class _ScriptedMembershipStore:
    """Return immutable scripted refresh outcomes and record revisions."""

    def __init__(self, *results: object) -> None:
        self.results = collections.deque(results)
        self.calls: list[
            tuple[ingestion_lease_store.LeaseGrant, int | None]
        ] = []

    async def refresh_membership(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        *,
        known_revision: int | None,
    ) -> object:
        self.calls.append((grant, known_revision))
        return self.results.popleft()


class _ScriptedProvider:
    """Return complete provider envelopes without network or credentials."""

    def __init__(self, *pages: provider.CallsPageEnvelope) -> None:
        self.pages = collections.deque(pages)
        self.calls: list[
            tuple[str, datetime.datetime | None, object, asyncio.Event]
        ] = []

    async def fetch_sid_page(
        self,
        sid: str,
        pos: datetime.datetime | None,
        *,
        subject_id: object,
        shutdown_event: asyncio.Event,
    ) -> provider.CallsPageEnvelope:
        self.calls.append((sid, pos, subject_id, shutdown_event))
        return self.pages.popleft()


class _RecordingLane:
    """Record exact coverage/retirement without downstream work."""

    def __init__(self, *cover_actions: BaseException) -> None:
        self.removed: list[uuid.UUID] = []
        self.cover_actions = collections.deque(cover_actions)
        self.covers: list[
            tuple[
                tuple[feed_work_scheduler.CallSubmission, ...],
                tuple[feed_work_scheduler.BoundaryWork, ...],
                object,
            ]
        ] = []

    async def remove_feed(
        self,
        feed_id: uuid.UUID,
    ) -> feed_work_scheduler.FeedRemoved:
        self.removed.append(feed_id)
        return feed_work_scheduler.FeedRemoved(
            grant=_GRANT,
            feed_id=feed_id,
            released_count=0,
            active_retained=True,
        )

    async def cover_page(
        self,
        *,
        calls: typing.Iterable[feed_work_scheduler.CallSubmission],
        boundaries: typing.Iterable[feed_work_scheduler.BoundaryWork],
        candidate: object,
    ) -> object:
        call_values = tuple(calls)
        boundary_values = tuple(boundaries)
        self.covers.append((call_values, boundary_values, candidate))
        if self.cover_actions:
            failure = self.cover_actions.popleft()
            for call in call_values:
                if call.settlement_observer is not None:
                    call.settlement_observer(
                        feed_work_scheduler.CallSettlement.ABORTED
                    )
            raise failure
        if isinstance(candidate, sid_processor.cursor_policy.PageCursorCandidate):
            return sid_processor.cursor_policy._issue_covered_page(candidate)
        if isinstance(
            candidate,
            sid_processor.cursor_policy.NoProgressPageCandidate,
        ):
            return sid_processor.cursor_policy._issue_no_progress_page(
                candidate
            )
        message = "unexpected candidate"
        raise AssertionError(message)


class _RecordingWait:
    """Record interruptible waits without sleeping."""

    def __init__(self) -> None:
        self.calls: list[tuple[asyncio.Event, float]] = []

    async def __call__(
        self,
        stop_requested: asyncio.Event,
        seconds: float,
    ) -> None:
        self.calls.append((stop_requested, seconds))


def _processor(
    store: _ScriptedMembershipStore,
    *,
    lane: _RecordingLane | None = None,
    calls_provider: _ScriptedProvider | None = None,
    wait: _RecordingWait | None = None,
) -> tuple[sid_processor.BcfyCallsSidProcessor, _RecordingLane]:
    selected_lane = lane or _RecordingLane()
    processor = sid_processor.BcfyCallsSidProcessor(
        _GRANT,
        store,
        calls_provider or _ScriptedProvider(),
        selected_lane,
        now=lambda: _NOW,
        wait=wait or _RecordingWait(),
    )
    return processor, selected_lane


def _raw_call(
    group_id: object,
    url: object,
    ts: object = 1_783_908_001,
) -> dict[str, object]:
    result = {"groupId": group_id, "url": url}
    if ts is not ...:
        result["ts"] = ts
    return result


def _page(
    *calls: object,
    last_pos: object | None,
) -> provider.CallsPageEnvelope:
    payload: dict[str, object] = {"calls": list(calls)}
    if last_pos is not None:
        payload["lastPos"] = last_pos
    return provider.CallsPageEnvelope(
        payload=payload,
        calls=calls,
        last_pos=last_pos,
    )


class TestBcfyCallsSidProcessor(unittest.IsolatedAsyncioTestCase):
    """Exact routing, snapshot, and URL-state contracts."""

    async def test_snapshot_initial_and_unchanged_reuse_identity(self) -> None:
        initial = _snapshot(4, _member(_FEED_A, "00123-00045"))
        store = _ScriptedMembershipStore(
            initial,
            ingestion_lease_store.MembershipUnchanged(_GRANT, 4),
        )
        processor, _ = _processor(store)

        await processor._refresh_membership()
        frozen_snapshot = processor._snapshot
        frozen_routes = processor._members_by_source
        await processor._refresh_membership()

        self.assertIs(processor._snapshot, frozen_snapshot)
        self.assertIs(processor._members_by_source, frozen_routes)
        self.assertEqual(store.calls, [(_GRANT, None), (_GRANT, 4)])
        self.assertIs(
            processor._member_state(_FEED_A).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )

    async def test_snapshot_page_keeps_frozen_routes_until_next_page(
        self,
    ) -> None:
        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()
        old_page = processor._prepare_call_submissions(
            [_raw_call("00123-00046", "https://audio/new")]
        )

        await processor._refresh_membership()

        self.assertEqual(list(old_page), [])
        next_page = list(
            processor._prepare_call_submissions(
                [_raw_call("00123-00046", "https://audio/new")]
            )
        )
        self.assertEqual([call.feed_id for call in next_page], [_FEED_B])

    async def test_snapshot_removal_and_reactivation_plan_drain(self) -> None:
        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a, member_b),
            _snapshot(5, member_a),
            _snapshot(6, member_a, member_b),
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        pending = list(
            processor._prepare_call_submissions(
                [_raw_call("00123-00046", "https://audio/retired")]
            )
        )[0]

        await processor._refresh_membership()

        self.assertEqual(lane.removed, [_FEED_B])
        self.assertNotIn("https://audio/retired", processor._pending_by_url)
        pending.settlement_observer(feed_work_scheduler.CallSettlement.COMPLETED)
        self.assertNotIn("https://audio/retired", processor._recent_urls)
        with self.assertRaises(sid_processor.SidProcessorPlannedDrain):
            await processor._refresh_membership()

    async def test_route_uses_exact_leading_zero_source_key(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045"))
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        submissions = list(
            processor._prepare_call_submissions(
                [
                    _raw_call("123-45", "https://audio/normalized"),
                    _raw_call(12345, "https://audio/integer"),
                    _raw_call("00123-00046", "https://audio/unknown"),
                    _raw_call("00123-00045", "https://audio/exact"),
                ]
            )
        )

        self.assertEqual(len(submissions), 1)
        self.assertEqual(submissions[0].feed_id, _FEED_A)
        payload = submissions[0].payload
        self.assertIsInstance(payload, sid_processor.ScheduledCallPayload)
        assert isinstance(payload, sid_processor.ScheduledCallPayload)
        self.assertEqual(payload.audio_url, "https://audio/exact")

    async def test_malformed_siblings_are_item_local(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045"))
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        calls: list[object] = [
            None,
            [],
            {"groupId": "00123-00045", "url": ""},
            _raw_call("00123-00045", "https://audio/bool", ts=True),
            _raw_call("00123-00045", "https://audio/nan", float("nan")),
            _raw_call("00123-00045", "https://audio/string", "123"),
            _raw_call("00123-00045", "https://audio/valid"),
        ]

        submissions = list(processor._prepare_call_submissions(calls))

        self.assertEqual(len(submissions), 1)
        self.assertEqual(
            submissions[0].payload.audio_url,
            "https://audio/valid",
        )

    async def test_route_missing_ts_and_inclusive_coverage(self) -> None:
        cursor = datetime.datetime.fromtimestamp(1_783_908_001, datetime.UTC)
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(_FEED_A, "00123-00045", cursor=cursor),
            )
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.bcfy_calls.sid_processor",
            level="WARNING",
        ) as captured:
            submissions = list(
                processor._prepare_call_submissions(
                    [
                        _raw_call(
                            "00123-00045",
                            "https://audio/missing",
                            ...,
                        ),
                        _raw_call(
                            "00123-00045",
                            "https://audio/equal",
                            1_783_908_001,
                        ),
                        _raw_call(
                            "00123-00045",
                            "https://audio/newer",
                            1_783_908_002,
                        ),
                    ]
                )
            )

        self.assertEqual(
            [call.payload.audio_url for call in submissions],
            ["https://audio/missing", "https://audio/newer"],
        )
        self.assertIsNone(submissions[0].source_timestamp)
        self.assertEqual(
            captured.records[0].json_fields["event_type"],
            "bcfy_calls_missing_ts",
        )

    async def test_route_equal_ts_distinct_urls_keep_source_order(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(
                    _FEED_A,
                    "00123-00045",
                    cursor=datetime.datetime.fromtimestamp(0, datetime.UTC),
                ),
            )
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        submissions = list(
            processor._prepare_call_submissions(
                [
                    _raw_call("00123-00045", "https://audio/later", 20),
                    _raw_call("00123-00045", "https://audio/equal-a", 10),
                    _raw_call("00123-00045", "https://audio/equal-b", 10),
                ]
            )
        )

        self.assertEqual(
            [call.payload.audio_url for call in submissions],
            [
                "https://audio/equal-a",
                "https://audio/equal-b",
                "https://audio/later",
            ],
        )

    async def test_dedup_pending_and_grant_wide_url_identity(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(_FEED_A, "00123-00045"),
                _member(_FEED_B, "00123-00046"),
            )
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        submissions = list(
            processor._prepare_call_submissions(
                [
                    _raw_call("00123-00045", "https://audio/shared"),
                    _raw_call("00123-00046", "https://audio/shared"),
                ]
            )
        )

        self.assertEqual(len(submissions), 1)
        self.assertEqual(
            processor._pending_by_url,
            {"https://audio/shared": _FEED_A},
        )

    async def test_dedup_all_settlement_outcomes(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045"))
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        for settlement in feed_work_scheduler.CallSettlement:
            with self.subTest(settlement=settlement.value):
                url = f"https://audio/{settlement.value}"
                submission = list(
                    processor._prepare_call_submissions(
                        [_raw_call("00123-00045", url)]
                    )
                )[0]
                self.assertEqual(processor._pending_by_url[url], _FEED_A)

                assert submission.settlement_observer is not None
                submission.settlement_observer(settlement)

                self.assertNotIn(url, processor._pending_by_url)
                if settlement is feed_work_scheduler.CallSettlement.COMPLETED:
                    self.assertIn(url, processor._recent_urls)
                    self.assertEqual(
                        list(
                            processor._prepare_call_submissions(
                                [_raw_call("00123-00045", url)]
                            )
                        ),
                        [],
                    )
                else:
                    self.assertNotIn(url, processor._recent_urls)

    async def test_dedup_recent_cap_evicts_exactly_one_thousand(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045"))
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        for index in range(1_001):
            submission = list(
                processor._prepare_call_submissions(
                    [
                        _raw_call(
                            "00123-00045",
                            f"https://audio/{index}",
                            1_783_908_100 + index,
                        )
                    ]
                )
            )[0]
            assert submission.settlement_observer is not None
            submission.settlement_observer(
                feed_work_scheduler.CallSettlement.COMPLETED
            )

        self.assertEqual(len(processor._recent_order), 1_000)
        self.assertEqual(len(processor._recent_urls), 1_000)
        self.assertNotIn("https://audio/0", processor._recent_urls)
        self.assertIn("https://audio/1", processor._recent_urls)
        self.assertIn("https://audio/1000", processor._recent_urls)

    async def test_dedup_feed_cleanup_late_completion_and_close(self) -> None:
        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a, member_b),
            _snapshot(5, member_b),
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        calls = list(
            processor._prepare_call_submissions(
                [
                    _raw_call("00123-00045", "https://audio/a"),
                    _raw_call("00123-00046", "https://audio/b"),
                ]
            )
        )

        await processor._refresh_membership()

        self.assertEqual(lane.removed, [_FEED_A])
        self.assertEqual(
            processor._pending_by_url,
            {"https://audio/b": _FEED_B},
        )
        assert calls[0].settlement_observer is not None
        calls[0].settlement_observer(
            feed_work_scheduler.CallSettlement.COMPLETED
        )
        self.assertNotIn("https://audio/a", processor._recent_urls)

        processor.close()

        self.assertEqual(processor._pending_by_url, {})
        self.assertEqual(processor._recent_urls, set())
        self.assertEqual(list(processor._recent_order), [])
        self.assertEqual(processor._members_by_source, {})

    async def test_adopt_null_member_on_valid_empty_page_then_route(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(_FEED_A, "00123-00045", cursor=None),
            )
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        request = processor._select_request_position()
        target = _NOW + datetime.timedelta(seconds=10)

        await processor._settle_page(
            _page(
                _raw_call("00123-00045", "https://audio/adoption"),
                last_pos=target.timestamp(),
            ),
            request=request,
        )

        calls, boundaries, _ = lane.covers[0]
        self.assertEqual(calls, ())
        self.assertEqual(
            boundaries,
            (
                feed_work_scheduler.BoundaryWork(
                    processor._member_state(_FEED_A).member.identity,
                    target,
                ),
            ),
        )
        self.assertIs(
            processor._member_state(_FEED_A).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )
        routed = list(
            processor._prepare_call_submissions(
                [
                    _raw_call(
                        "00123-00045",
                        "https://audio/following-poll",
                        target.timestamp() + 1,
                    )
                ]
            )
        )
        self.assertEqual(len(routed), 1)

    async def test_adopt_new_null_member_classification(self) -> None:
        member_a = _member(
            _FEED_A,
            "00123-00045",
            cursor=_NOW - datetime.timedelta(seconds=30),
        )
        member_b = _member(_FEED_B, "00123-00046", cursor=None)
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        await processor._refresh_membership()

        self.assertIs(
            processor._member_state(_FEED_B).route_state,
            sid_processor.MemberRouteState.ADOPTING,
        )

    async def test_adopt_missing_malformed_and_regressive_last_pos_retained(
        self,
    ) -> None:
        cases = (None, "malformed", (_NOW - datetime.timedelta(minutes=1)).timestamp())
        for last_pos in cases:
            with self.subTest(last_pos=last_pos):
                member_a = _member(
                    _FEED_A,
                    "00123-00045",
                    cursor=_NOW - datetime.timedelta(seconds=30),
                )
                member_b = _member(_FEED_B, "00123-00046", cursor=None)
                store = _ScriptedMembershipStore(
                    _snapshot(4, member_a),
                    _snapshot(5, member_a, member_b),
                )
                processor, lane = _processor(store)
                await processor._refresh_membership()
                await processor._refresh_membership()
                original_cursor = processor._lease_cursor.pos

                await processor._settle_page(
                    _page(last_pos=last_pos),
                    request=processor._select_request_position(),
                )

                _, boundaries, candidate = lane.covers[0]
                self.assertEqual(boundaries, ())
                self.assertIsInstance(
                    candidate,
                    sid_processor.cursor_policy.NoProgressPageCandidate,
                )
                self.assertEqual(processor._lease_cursor.pos, original_cursor)
                self.assertIs(
                    processor._member_state(_FEED_B).route_state,
                    sid_processor.MemberRouteState.ADOPTING,
                )

    async def test_adopt_aborted_coverage_retains_state(self) -> None:
        lane = _RecordingLane(RuntimeError("cover aborted"))
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(_FEED_A, "00123-00045", cursor=None),
            )
        )
        processor, _ = _processor(store, lane=lane)
        await processor._refresh_membership()

        with self.assertRaisesRegex(RuntimeError, "cover aborted"):
            await processor._settle_page(
                _page(last_pos=_NOW.timestamp()),
                request=processor._select_request_position(),
            )

        self.assertIs(
            processor._member_state(_FEED_A).route_state,
            sid_processor.MemberRouteState.ADOPTING,
        )
        self.assertIsNotNone(
            processor._lease_cursor.outstanding_candidate,
        )

    async def test_replay_classifies_older_equal_newer_and_null_joins(
        self,
    ) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(
                5,
                member_a,
                _member(
                    _FEED_B,
                    "00123-00046",
                    cursor=current - datetime.timedelta(seconds=1),
                ),
                _member(_FEED_C, "00123-00047", cursor=current),
                _member(
                    _FEED_D,
                    "00123-00048",
                    cursor=current + datetime.timedelta(seconds=1),
                ),
                _member(_FEED_E, "00123-00049", cursor=None),
            ),
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        await processor._refresh_membership()

        self.assertIs(
            processor._member_state(_FEED_B).route_state,
            sid_processor.MemberRouteState.REPLAY_PENDING,
        )
        self.assertIs(
            processor._member_state(_FEED_C).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )
        self.assertIs(
            processor._member_state(_FEED_D).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )
        self.assertIs(
            processor._member_state(_FEED_E).route_state,
            sid_processor.MemberRouteState.ADOPTING,
        )

    async def test_replay_initial_floor_does_not_mark_initial_member(self) -> None:
        old_cursor = _NOW - datetime.timedelta(minutes=30)
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(_FEED_A, "00123-00045", cursor=old_cursor),
            )
        )
        processor, _ = _processor(store)

        await processor._refresh_membership()

        self.assertEqual(
            processor._lease_cursor.pos,
            _NOW - datetime.timedelta(minutes=5),
        )
        self.assertIs(
            processor._member_state(_FEED_A).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )

    async def test_replay_coalesces_minimum_with_five_minute_clamp(self) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(
                5,
                member_a,
                _member(
                    _FEED_B,
                    "00123-00046",
                    cursor=_NOW - datetime.timedelta(minutes=20),
                ),
                _member(
                    _FEED_C,
                    "00123-00047",
                    cursor=_NOW - datetime.timedelta(minutes=2),
                ),
            ),
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()
        await processor._refresh_membership()
        before_cursor = processor._lease_cursor.pos

        request = processor._select_request_position()

        self.assertEqual(
            request.pos,
            _NOW - datetime.timedelta(minutes=5),
        )
        self.assertEqual(request.replay_feed_ids, frozenset({_FEED_B, _FEED_C}))
        self.assertEqual(processor._lease_cursor.pos, before_cursor)

    async def test_replay_valid_override_routes_all_and_consumes_state(
        self,
    ) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        member_b = _member(
            _FEED_B,
            "00123-00046",
            cursor=_NOW - datetime.timedelta(minutes=2),
        )
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        await processor._refresh_membership()
        target = _NOW + datetime.timedelta(seconds=10)
        request = processor._select_request_position()

        await processor._settle_page(
            _page(
                _raw_call(
                    "00123-00045",
                    "https://audio/normal-on-override",
                    current.timestamp() + 1,
                ),
                _raw_call(
                    "00123-00046",
                    "https://audio/replay",
                    (_NOW - datetime.timedelta(minutes=1)).timestamp(),
                ),
                last_pos=target.timestamp(),
            ),
            request=request,
        )

        calls, boundaries, _ = lane.covers[0]
        self.assertEqual(
            [call.feed_id for call in calls],
            [_FEED_B, _FEED_A],
        )
        self.assertEqual(
            {boundary.feed_id for boundary in boundaries},
            {_FEED_A, _FEED_B},
        )
        self.assertEqual(processor._lease_cursor.pos, target)
        self.assertIs(
            processor._member_state(_FEED_B).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )

    async def test_replay_no_progress_retains_override_and_cursor(self) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        member_b = _member(
            _FEED_B,
            "00123-00046",
            cursor=_NOW - datetime.timedelta(minutes=2),
        )
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        await processor._refresh_membership()
        request = processor._select_request_position()
        before_cursor = processor._lease_cursor.pos

        await processor._settle_page(
            _page(
                _raw_call(
                    "00123-00046",
                    "https://audio/no-progress-replay",
                    (_NOW - datetime.timedelta(minutes=1)).timestamp(),
                ),
                last_pos="invalid",
            ),
            request=request,
        )

        calls, boundaries, _ = lane.covers[0]
        self.assertEqual([call.feed_id for call in calls], [_FEED_B])
        self.assertEqual(boundaries, ())
        self.assertEqual(processor._lease_cursor.pos, before_cursor)
        self.assertIs(
            processor._member_state(_FEED_B).route_state,
            sid_processor.MemberRouteState.REPLAY_PENDING,
        )
        self.assertEqual(
            processor._select_request_position().pos,
            request.pos,
        )

    async def test_replay_aborted_override_retains_state_and_unwinds_url(
        self,
    ) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        member_b = _member(
            _FEED_B,
            "00123-00046",
            cursor=_NOW - datetime.timedelta(minutes=2),
        )
        lane = _RecordingLane(RuntimeError("override aborted"))
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        processor, _ = _processor(store, lane=lane)
        await processor._refresh_membership()
        await processor._refresh_membership()

        with self.assertRaisesRegex(RuntimeError, "override aborted"):
            await processor._settle_page(
                _page(
                    _raw_call(
                        "00123-00046",
                        "https://audio/aborted-override",
                        (_NOW - datetime.timedelta(minutes=1)).timestamp(),
                    ),
                    last_pos=_NOW.timestamp(),
                ),
                request=processor._select_request_position(),
            )

        self.assertIs(
            processor._member_state(_FEED_B).route_state,
            sid_processor.MemberRouteState.REPLAY_PENDING,
        )
        self.assertNotIn(
            "https://audio/aborted-override",
            processor._pending_by_url,
        )

    async def test_adopt_null_sibling_on_valid_replay_override(self) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        member_b = _member(
            _FEED_B,
            "00123-00046",
            cursor=_NOW - datetime.timedelta(minutes=2),
        )
        member_c = _member(_FEED_C, "00123-00047", cursor=None)
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b, member_c),
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        await processor._refresh_membership()
        target = _NOW + datetime.timedelta(seconds=1)

        await processor._settle_page(
            _page(
                _raw_call(
                    "00123-00047",
                    "https://audio/ignored-adoption",
                    target.timestamp(),
                ),
                last_pos=target.timestamp(),
            ),
            request=processor._select_request_position(),
        )

        calls, boundaries, _ = lane.covers[0]
        self.assertEqual(calls, ())
        self.assertEqual(
            {boundary.feed_id for boundary in boundaries},
            {_FEED_A, _FEED_B, _FEED_C},
        )
        self.assertIs(
            processor._member_state(_FEED_C).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )

    async def test_last_pos_float_string_equal_uses_progress_path(self) -> None:
        cursor = _NOW - datetime.timedelta(seconds=30)
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045", cursor=cursor))
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()

        await processor._settle_page(
            _page(last_pos=f"{cursor.timestamp():.1f}"),
            request=processor._select_request_position(),
        )

        _, boundaries, candidate = lane.covers[0]
        self.assertIsInstance(
            candidate,
            sid_processor.cursor_policy.PageCursorCandidate,
        )
        self.assertEqual([boundary.target for boundary in boundaries], [cursor])
        self.assertEqual(processor._lease_cursor.pos, cursor)

    async def test_no_progress_routes_calls_without_boundaries_or_cursor(
        self,
    ) -> None:
        cursor = _NOW - datetime.timedelta(seconds=30)
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045", cursor=cursor))
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()

        await processor._settle_page(
            _page(
                _raw_call(
                    "00123-00045",
                    "https://audio/no-progress",
                    cursor.timestamp() + 1,
                ),
                last_pos=None,
            ),
            request=processor._select_request_position(),
        )

        calls, boundaries, candidate = lane.covers[0]
        self.assertEqual([call.feed_id for call in calls], [_FEED_A])
        self.assertEqual(boundaries, ())
        self.assertIsInstance(
            candidate,
            sid_processor.cursor_policy.NoProgressPageCandidate,
        )
        self.assertEqual(processor._lease_cursor.pos, cursor)
        self.assertEqual(processor._lease_cursor.next_page_sequence, 1)


if __name__ == "__main__":
    unittest.main()
