"""Behavioral tests for page-sequential Broadcastify Calls SID ingestion."""

from __future__ import annotations

import asyncio
import datetime
import logging
import uuid
from unittest import mock

import pytest

from backend.pipeline.ingestion import failure_policy, grant_control, models
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    pipeline,
    provider,
    sid_runner,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_NOW = datetime.datetime(2026, 7, 19, 12, tzinfo=datetime.UTC)


def _grant() -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        feed_store.SourceType.BCFY_CALLS,
        "7017",
        uuid.uuid4(),
        8,
    )


def _member(
    group_id: str,
    *,
    bookmark: datetime.datetime | None,
    retry_after: datetime.datetime | None = None,
) -> ingestion_lease_store.LeaseMember:
    return ingestion_lease_store.LeaseMember(
        identity=ingestion_lease_store.LeaseMemberIdentity(
            uuid.uuid4(),
            feed_store.SourceType.BCFY_CALLS,
            f"7017-{group_id}",
        ),
        name=f"Talkgroup {group_id}",
        last_bookmark_time=bookmark,
        retry_after=retry_after,
    )


def _snapshot(
    grant: ingestion_lease_store.LeaseGrant,
    *members: ingestion_lease_store.LeaseMember,
) -> ingestion_lease_store.MembershipSnapshot:
    return ingestion_lease_store.MembershipSnapshot(grant, 1, members)


def _result(
    batch: pipeline.FeedBatch,
    *,
    attempted: int | None = None,
    published: int | None = None,
    terminal: (
        failure_classification.ItemFailure
        | ingestion_lease_store.GrantRejected
        | None
    ) = None,
) -> pipeline.FeedBatchResult:
    attempted_count = len(batch.calls) if attempted is None else attempted
    published_count = attempted_count if published is None else published
    return pipeline.FeedBatchResult(
        attempted_count=attempted_count,
        published_count=published_count,
        next_sequence=batch.starting_sequence + attempted_count,
        committed_urls=tuple(call.audio_url for call in batch.calls),
        terminal=terminal,
    )


class _Store:
    def __init__(
        self,
        snapshot: ingestion_lease_store.MembershipSnapshot,
    ) -> None:
        self.snapshot = snapshot
        self.batches: list[ingestion_lease_store.ChildMutationBatch] = []

    async def load_membership(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> ingestion_lease_store.MembershipSnapshot:
        assert grant == self.snapshot.grant
        return self.snapshot

    async def commit_child_mutations(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        batch: ingestion_lease_store.ChildMutationBatch,
        *,
        actor_id: str,
    ) -> ingestion_lease_store.BatchCommitted:
        assert grant == self.snapshot.grant
        assert actor_id == "test"
        self.batches.append(batch)
        children = tuple(
            ingestion_lease_store.ChildMutationResult(
                mutation.member.feed_id,
                ingestion_lease_store.ChildDisposition.COMMITTED,
            )
            for mutation in batch.mutations
        )
        return ingestion_lease_store.BatchCommitted(children)


class _Provider:
    def __init__(
        self,
        pages: list[provider.CallsPageEnvelope],
        context: grant_control.RunContext,
    ) -> None:
        self.pages = pages
        self.context = context
        self.positions: list[datetime.datetime | None] = []

    async def fetch_sid_page(
        self,
        sid: str,
        pos: datetime.datetime | None,
        *,
        shutdown_event: asyncio.Event,
    ) -> provider.CallsPageEnvelope:
        assert sid == "7017"
        assert not shutdown_event.is_set()
        self.positions.append(pos)
        page = self.pages.pop(0)
        if not self.pages:
            self.context.stop_requested.set()
        return page


class _Pool:
    def __init__(
        self,
        result_factory: mock.Mock | None = None,
    ) -> None:
        self.batches: list[pipeline.FeedBatch] = []
        self.result_factory = result_factory or mock.Mock(side_effect=_result)

    async def submit(
        self,
        batch: pipeline.FeedBatch,
    ) -> asyncio.Future[pipeline.FeedBatchResult]:
        self.batches.append(batch)
        future = asyncio.get_running_loop().create_future()
        future.set_result(self.result_factory(batch))
        return future


def _failure_planner(
    status_reason: feed_store.FeedStatusReason,
    reason: str | None,
) -> failure_policy.FailurePersistencePlan:
    return failure_policy.plan_failure(
        status_reason,
        reason,
        budgeted=failure_policy.ConsumeFailureBudget(5, 60, 900),
        non_budgeted=_retry_without_budget,
    )


def _retry_without_budget() -> failure_policy.RetryWithoutBudget:
    return failure_policy.RetryWithoutBudget(
        _NOW + datetime.timedelta(minutes=5)
    )


def _context() -> grant_control.RunContext:
    return grant_control.RunContext(asyncio.Event(), asyncio.Event())


def test_provider_timestamp_preserves_fractional_seconds() -> None:
    timestamp = _NOW.timestamp() + 0.75

    assert sid_runner._utc_timestamp(timestamp) == (
        datetime.datetime.fromtimestamp(timestamp, datetime.UTC)
    )


@pytest.mark.asyncio
async def test_accepted_batches_settle_before_an_error_surfaces() -> None:
    first = asyncio.get_running_loop().create_future()
    second = asyncio.get_running_loop().create_future()
    first.set_exception(RuntimeError("first batch failed"))

    settlement = asyncio.create_task(
        sid_runner._settle_accepted((first, second))
    )
    await asyncio.sleep(0)
    assert not settlement.done()

    second.set_result(pipeline.FeedBatchResult(0, 0, 0, (), None))
    with pytest.raises(RuntimeError, match="first batch failed"):
        await settlement


@pytest.mark.asyncio
async def test_cancellation_wins_after_accepted_batch_failure() -> None:
    first = asyncio.get_running_loop().create_future()
    second = asyncio.get_running_loop().create_future()
    first.set_exception(RuntimeError("first batch failed"))

    settlement = asyncio.create_task(
        sid_runner._settle_accepted((first, second))
    )
    await asyncio.sleep(0)
    settlement.cancel()
    await asyncio.sleep(0)
    second.set_result(pipeline.FeedBatchResult(0, 0, 0, (), None))

    with pytest.raises(asyncio.CancelledError) as context:
        await settlement
    assert isinstance(context.value.__cause__, RuntimeError)


@pytest.mark.asyncio
async def test_admission_error_drains_already_accepted_batch() -> None:
    grant = _grant()
    first = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    second = _member("200", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-200",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )

    class PartiallyRejectingPool:
        def __init__(self) -> None:
            self.batches: list[pipeline.FeedBatch] = []
            self.completion = asyncio.get_running_loop().create_future()

        async def submit(
            self,
            batch: pipeline.FeedBatch,
        ) -> asyncio.Future[pipeline.FeedBatchResult]:
            self.batches.append(batch)
            if len(self.batches) == 1:
                return self.completion
            message = "admission closed"
            raise RuntimeError(message)

    pool = PartiallyRejectingPool()
    runner = sid_runner.BcfyCallsSidRunner(
        _Store(_snapshot(grant, first, second)),
        _Provider([page], context),
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    run = asyncio.create_task(
        runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )
    )
    await asyncio.sleep(0)
    assert not run.done()
    pool.completion.set_result(_result(pool.batches[0]))

    with pytest.raises(RuntimeError, match="admission closed"):
        await run


@pytest.mark.asyncio
async def test_routes_due_members_and_adopts_null_cursor_at_boundary(
    caplog: pytest.LogCaptureFixture,
) -> None:
    grant = _grant()
    due = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(seconds=30),
    )
    adopter = _member("200", bookmark=None)
    deferred = _member(
        "300",
        bookmark=_NOW - datetime.timedelta(minutes=2),
        retry_after=_NOW + datetime.timedelta(minutes=1),
    )
    context = _context()
    calls = (
        {
            "groupId": "7017-100",
            "url": "https://audio/1",
            "ts": _NOW.timestamp(),
        },
        {
            "groupId": "7017-100",
            "url": "https://audio/1",
            "ts": _NOW.timestamp(),
        },
        {
            "groupId": "7017-200",
            "url": "https://audio/2",
            "ts": _NOW.timestamp(),
        },
        {
            "groupId": "7017-300",
            "url": "https://audio/3",
            "ts": _NOW.timestamp(),
        },
    )
    page = provider.CallsPageEnvelope({}, calls, _NOW.timestamp())
    store = _Store(_snapshot(grant, due, adopter, deferred))
    calls_provider = _Provider([page], context)
    pool = _Pool()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    with caplog.at_level(logging.INFO, sid_runner.__name__):
        outcome = await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert calls_provider.positions == [due.last_bookmark_time]
    assert [batch.member for batch in pool.batches] == [due]
    assert len(pool.batches[0].calls) == 1
    assert len(store.batches) == 1
    committed_ids = {
        mutation.member.feed_id for mutation in store.batches[0].mutations
    }
    assert committed_ids == {
        due.identity.feed_id,
        adopter.identity.feed_id,
    }
    assert isinstance(
        store.batches[0].lease_effect,
        ingestion_lease_store.FinalizeLeaseRecovery,
    )
    settled = [
        record
        for record in caplog.records
        if getattr(record, "json_fields", {}).get("event_type")
        == "bcfy_calls_sid_poll_settled"
    ]
    assert len(settled) == 1
    assert getattr(settled[0], "json_fields", {}).get("status") == "completed"


@pytest.mark.asyncio
async def test_routes_only_new_well_formed_calls_for_current_members() -> None:
    grant = _grant()
    bookmark = _NOW - datetime.timedelta(seconds=30)
    member = _member("100", bookmark=bookmark)
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/equal",
                "ts": bookmark.timestamp(),
            },
            {
                "groupId": "7017-100",
                "url": "https://audio/older",
                "ts": (bookmark - datetime.timedelta(seconds=1)).timestamp(),
            },
            {
                "groupId": "7017-999",
                "url": "https://audio/untracked",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": True,
                "url": "https://audio/bool-group",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-100",
                "url": "",
                "ts": _NOW.timestamp(),
            },
            "not-a-call",
            {
                "groupId": "7017-100",
                "url": "https://audio/new",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    pool = _Pool()
    runner = sid_runner.BcfyCallsSidRunner(
        _Store(_snapshot(grant, member)),
        _Provider([page], context),
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert len(pool.batches) == 1
    assert [call.audio_url for call in pool.batches[0].calls] == [
        "https://audio/new"
    ]


@pytest.mark.asyncio
async def test_all_null_members_start_at_live_edge_without_routing() -> None:
    grant = _grant()
    first = _member("100", bookmark=None)
    second = _member("200", bookmark=None)
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/first-page",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    store = _Store(_snapshot(grant, first, second))
    calls_provider = _Provider([page], context)
    pool = _Pool()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert calls_provider.positions == [None]
    assert pool.batches == []
    assert {
        mutation.member.feed_id for mutation in store.batches[0].mutations
    } == {first.identity.feed_id, second.identity.feed_id}


@pytest.mark.asyncio
async def test_metadata_fetch_uses_supervisor_stop_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(seconds=30),
    )
    context = _context()
    page = provider.CallsPageEnvelope({}, (), _NOW.timestamp())

    class StopAwareProvider:
        def __init__(self) -> None:
            self.shutdown_event: asyncio.Event | None = None

        async def fetch_sid_page(
            self,
            sid: str,
            pos: datetime.datetime | None,
            *,
            shutdown_event: asyncio.Event,
        ) -> provider.CallsPageEnvelope:
            del sid, pos
            self.shutdown_event = shutdown_event
            context.stop_requested.set()
            if shutdown_event.is_set():
                raise provider.TokenLoadStopped
            return page

    calls_provider = StopAwareProvider()
    runner = sid_runner.BcfyCallsSidRunner(
        _Store(_snapshot(grant, member)),
        calls_provider,
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    with caplog.at_level(logging.INFO, sid_runner.__name__):
        outcome = await runner.run(
            grant,
            grant_control.ClaimMode.PRIMARY,
            context,
        )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert calls_provider.shutdown_event is context.stop_requested
    settled = [
        record
        for record in caplog.records
        if getattr(record, "json_fields", {}).get("event_type")
        == "bcfy_calls_sid_poll_settled"
    ]
    assert len(settled) == 1
    assert getattr(settled[0], "json_fields", {}).get("status") == "stopped"


@pytest.mark.asyncio
async def test_authentication_failure_retries_the_owned_sid() -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(seconds=30),
    )
    context = _context()
    page = provider.CallsPageEnvelope({}, (), _NOW.timestamp())

    class RefreshingProvider(_Provider):
        def __init__(self) -> None:
            super().__init__([page], context)
            self.failed_once = False

        async def fetch_sid_page(
            self,
            sid: str,
            pos: datetime.datetime | None,
            *,
            shutdown_event: asyncio.Event,
        ) -> provider.CallsPageEnvelope:
            if not self.failed_once:
                self.failed_once = True
                raise models.FeedFailure(
                    feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                    "calls_api_http_401",
                )
            return await super().fetch_sid_page(
                sid,
                pos,
                shutdown_event=shutdown_event,
            )

    store = _Store(_snapshot(grant, member))
    calls_provider = RefreshingProvider()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert calls_provider.failed_once
    assert calls_provider.positions == [member.last_bookmark_time]
    assert len(store.batches) == 1


@pytest.mark.asyncio
async def test_invalid_boundary_replays_bookmark_and_deduplicates_url() -> None:
    grant = _grant()
    bookmark = _NOW - datetime.timedelta(seconds=30)
    member = _member("100", bookmark=bookmark)
    context = _context()
    call_time = _NOW - datetime.timedelta(seconds=10)
    call = {
        "groupId": "7017-100",
        "url": "https://audio/duplicate",
        "ts": call_time.timestamp(),
    }
    pages = [
        provider.CallsPageEnvelope({}, (call,), bookmark.timestamp() - 1),
        provider.CallsPageEnvelope({}, (call,), _NOW.timestamp()),
    ]
    store = _Store(_snapshot(grant, member))
    calls_provider = _Provider(pages, context)
    pool = _Pool()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.RECOVERY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    # Durable membership is deliberately reloaded every poll. This fake store
    # keeps returning the old bookmark, so it remains the safe request floor.
    assert calls_provider.positions == [bookmark, bookmark]
    assert len(pool.batches) == 1
    assert len(store.batches) == 2
    assert store.batches[0].mutations == ()
    assert len(store.batches[1].mutations) == 1


@pytest.mark.asyncio
async def test_future_page_boundary_does_not_advance_member() -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(seconds=30),
    )
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (),
        (_NOW + datetime.timedelta(seconds=1)).timestamp(),
    )
    store = _Store(_snapshot(grant, member))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        _Pool(),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    assert len(store.batches) == 1
    assert store.batches[0].mutations == ()


@pytest.mark.asyncio
async def test_grant_loss_during_fetch_still_settles_admitted_page() -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(minutes=1),
    )
    context = _context()

    class LosingProvider(_Provider):
        async def fetch_sid_page(
            self,
            sid: str,
            pos: datetime.datetime | None,
            *,
            shutdown_event: asyncio.Event,
        ) -> provider.CallsPageEnvelope:
            page = await super().fetch_sid_page(
                sid,
                pos,
                shutdown_event=shutdown_event,
            )
            context.grant_lost.set()
            return page

    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/admitted",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    store = _Store(_snapshot(grant, member))
    calls_provider = LosingProvider([page], context)
    pool = _Pool()
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunLost)
    assert len(pool.batches) == 1
    assert len(store.batches) == 1


@pytest.mark.asyncio
async def test_batch_grant_rejection_stops_sid_without_page_commit() -> None:
    grant = _grant()
    member = _member(
        "100",
        bookmark=_NOW - datetime.timedelta(minutes=1),
    )
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/rejected",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    rejection = ingestion_lease_store.GrantRejected(
        ingestion_lease_store.GrantRejectionReason.FENCE_MISMATCH
    )

    def rejected_result(
        batch: pipeline.FeedBatch,
    ) -> pipeline.FeedBatchResult:
        return _result(batch, terminal=rejection)

    store = _Store(_snapshot(grant, member))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        _Provider([page], context),
        _Pool(mock.Mock(side_effect=rejected_result)),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunLost)
    assert store.batches == []


@pytest.mark.asyncio
async def test_all_failed_multi_feed_page_promotes_only_parent() -> None:
    grant = _grant()
    first = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    second = _member("200", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-100",
                "url": "https://audio/1b",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-200",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    first_failure = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "first",
    )
    second_failure = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "second",
    )
    failures = iter((first_failure, second_failure))

    def failed_result(batch: pipeline.FeedBatch) -> pipeline.FeedBatchResult:
        return _result(
            batch,
            published=0,
            terminal=next(failures),
        )

    store = _Store(_snapshot(grant, first, second))
    calls_provider = _Provider([page], context)
    pool = _Pool(mock.Mock(side_effect=failed_result))
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        pool,
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert outcome == grant_control.RunFailed(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "first",
    )
    assert store.batches[0].mutations == ()
    assert isinstance(
        store.batches[0].lease_effect,
        ingestion_lease_store.NoLeaseEffect,
    )


@pytest.mark.asyncio
async def test_single_feed_failure_stays_child_local() -> None:
    grant = _grant()
    member = _member("100", bookmark=_NOW - datetime.timedelta(minutes=1))
    context = _context()
    page = provider.CallsPageEnvelope(
        {},
        (
            {
                "groupId": "7017-100",
                "url": "https://audio/1",
                "ts": _NOW.timestamp(),
            },
            {
                "groupId": "7017-100",
                "url": "https://audio/2",
                "ts": _NOW.timestamp(),
            },
        ),
        _NOW.timestamp(),
    )
    item_failure = failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "audio unavailable",
    )

    def failed_result(batch: pipeline.FeedBatch) -> pipeline.FeedBatchResult:
        return _result(batch, published=0, terminal=item_failure)

    store = _Store(_snapshot(grant, member))
    calls_provider = _Provider([page], context)
    runner = sid_runner.BcfyCallsSidRunner(
        store,
        calls_provider,
        _Pool(mock.Mock(side_effect=failed_result)),
        _failure_planner,
        actor_id="test",
        poll_interval_sec=0,
        clock=lambda: _NOW,
    )

    outcome = await runner.run(
        grant,
        grant_control.ClaimMode.PRIMARY,
        context,
    )

    assert isinstance(outcome, grant_control.RunCompleted)
    mutation = store.batches[0].mutations[0]
    assert isinstance(
        mutation,
        ingestion_lease_store.FeedFailureTransition,
    )
    assert mutation.completion_cursor is None
    assert isinstance(
        mutation.action,
        ingestion_lease_store.NonBudgetedFailure,
    )
