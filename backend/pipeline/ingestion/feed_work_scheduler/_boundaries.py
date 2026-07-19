"""Exact-grant coordinator for counted trailing Feed boundaries."""

# Private sibling modules deliberately share closed scheduler internals.
# ruff: noqa: SLF001

from __future__ import annotations

import asyncio
import collections
import typing

from backend.pipeline.ingestion.feed_work_scheduler import _shard, _types

if typing.TYPE_CHECKING:
    from backend.pipeline.storage import ingestion_lease_store

_BOUNDARY_BATCH_SIZE = 100


class _BoundaryCoordinatorError(RuntimeError):
    """Boundary settlement can no longer prove a safe outcome."""


class _BoundaryAuthorityLostError(_BoundaryCoordinatorError):
    """The committer rejected this exact Lease generation."""


class _BoundaryCoordinator:
    """One Event flusher with bounded generation state for one grant."""

    def __init__(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        shards: tuple[_shard._Shard, ...],
        committer: _types.BoundaryCommitter,
        *,
        authority_lost: typing.Callable[[], object],
        fatal_observer: typing.Callable[[BaseException], None],
        batch_size: int = _BOUNDARY_BATCH_SIZE,
    ) -> None:
        _types._require_positive_integer(batch_size, "batch_size")
        self._grant = grant
        self._shards = shards
        self._committer = committer
        self._authority_lost = authority_lost
        self._fatal_observer = fatal_observer
        self._batch_size = batch_size
        self._signal = asyncio.Event()
        self._generation_changed = asyncio.Condition()
        self._requested_generation = 0
        self._completed_generation = 0
        self._requested_final = False
        self._retryable_generation: int | None = None
        self._closing = False
        self._authority_rejected = False
        self._fatal: BaseException | None = None
        self._task = asyncio.create_task(
            self._run(),
            name=(
                f"feed-boundary-flusher-{grant.lease_key}-{grant.fencing_token}"
            ),
        )

    def notify_ready(self) -> None:
        """Coalesce physical readiness without allocating a message."""
        if not self._closing and self._fatal is None:
            self._signal.set()

    async def request_relief(self) -> _types._BoundaryPressureResult:
        """Request one pressure-relief generation."""
        return await self._request_generation(final_logical=False)

    async def request_final(self) -> _types._BoundaryPressureResult:
        """Request the page's required final logical attempt."""
        return await self._request_generation(final_logical=True)

    async def close(self) -> None:
        """Stop new selection after any begun mutation settles."""
        self._closing = True
        self._signal.set()
        await self._task

    async def _request_generation(
        self,
        *,
        final_logical: bool,
    ) -> _types._BoundaryPressureResult:
        async with self._generation_changed:
            self._raise_unavailable()
            await self._generation_changed.wait_for(
                lambda: (
                    self._requested_generation == self._completed_generation
                    or self._fatal is not None
                    or self._authority_rejected
                    or self._closing
                )
            )
            self._raise_unavailable()
            self._requested_generation += 1
            generation = self._requested_generation
            self._requested_final = final_logical
            self._signal.set()
            await self._generation_changed.wait_for(
                lambda: (
                    self._completed_generation >= generation
                    or self._fatal is not None
                    or self._authority_rejected
                    or self._closing
                )
            )
            self._raise_unavailable()
            if self._retryable_generation == generation:
                return _types._BoundaryPressureResult.RETRYABLE
            return _types._BoundaryPressureResult.COMPLETED

    async def _run(self) -> None:
        try:
            while True:
                await self._signal.wait()
                self._signal.clear()
                if self._closing:
                    await self._notify_generation_waiters()
                    return

                generation = self._requested_generation
                logical = generation > self._completed_generation
                final_logical = logical and self._requested_final
                selected = await self._select_batch(include_suspended=logical)
                if not selected and not logical:
                    continue
                retryable = await self._commit_selected(
                    selected,
                    final_logical=final_logical,
                )
                async with self._generation_changed:
                    if (
                        not logical
                        and self._requested_generation
                        > self._completed_generation
                        and not self._requested_final
                    ):
                        generation = self._requested_generation
                        logical = True
                    if logical:
                        self._completed_generation = generation
                        self._retryable_generation = (
                            generation if retryable else None
                        )
                        self._generation_changed.notify_all()
                    pending_final = (
                        self._requested_generation > self._completed_generation
                        and self._requested_final
                    )
                    if retryable and not pending_final and not self._closing:
                        self._signal.clear()
                if self._closing:
                    await self._notify_generation_waiters()
                    return
                if self._authority_rejected:
                    return
                if not retryable and await self._has_ready_boundary():
                    self._signal.set()
        except asyncio.CancelledError:
            failure = _BoundaryCoordinatorError(
                "boundary flusher was cancelled before settlement"
            )
            await self._fail(failure)
        except BaseException as exc:
            await self._fail(exc)

    async def _select_batch(
        self,
        *,
        include_suspended: bool,
    ) -> tuple[tuple[_shard._Shard, _types._BoundaryRecord], ...]:
        selected = []
        remaining = self._batch_size
        for shard in self._shards:
            if remaining <= 0:
                break
            records = await shard.select_boundary_batch(
                self._grant,
                remaining,
                include_suspended=include_suspended,
            )
            selected.extend((shard, record) for record in records)
            remaining -= len(records)
        return tuple(selected)

    async def _commit_selected(
        self,
        selected: tuple[
            tuple[_shard._Shard, _types._BoundaryRecord],
            ...,
        ],
        *,
        final_logical: bool,
    ) -> bool:
        targets = tuple(
            record.detached_work() for _shard_value, record in selected
        )
        result, cancelled = await self._settle_committed(
            self._committer.commit(
                self._grant,
                targets,
                final_logical=final_logical,
            )
        )
        if type(result) is _types.BoundaryGrantRejected:
            await self._discard_selected(selected)
            self._authority_rejected = True
            self._authority_lost()
            self._raise_if_cancelled(cancelled=cancelled)
            return False
        if type(result) is _types.BoundaryBatchRetryable:
            dispositions = (_types.BoundaryDisposition.RETRYABLE,) * len(
                targets
            )
        else:
            dispositions = self._correlate_dispositions(result, targets)

        by_shard: dict[
            _shard._Shard,
            list[tuple[_types._BoundaryRecord, _types.BoundaryDisposition]],
        ] = collections.defaultdict(list)
        for (shard, record), disposition in zip(
            selected,
            dispositions,
            strict=True,
        ):
            by_shard[shard].append((record, disposition))
        retryable = type(result) is _types.BoundaryBatchRetryable
        for shard, shard_results in by_shard.items():
            retryable = (
                await shard.apply_boundary_results(tuple(shard_results))
                or retryable
            )
        self._raise_if_cancelled(cancelled=cancelled)
        return retryable

    @staticmethod
    def _correlate_dispositions(
        result: object,
        targets: tuple[_types.BoundaryWork, ...],
    ) -> tuple[_types.BoundaryDisposition, ...]:
        if type(result) is not _types.BoundaryBatchCommitted:
            message = "committer returned outside the closed vocabulary"
            raise _BoundaryCoordinatorError(message)
        if len(result.results) != len(targets):
            message = "committer result cardinality does not match targets"
            raise _BoundaryCoordinatorError(message)
        dispositions = []
        for target, correlated in zip(
            targets,
            result.results,
            strict=True,
        ):
            if type(correlated) is not _types.BoundaryResult:
                message = "committer returned an unknown boundary result"
                raise _BoundaryCoordinatorError(message)
            if correlated.boundary is not target:
                message = "committer result correlation changed"
                raise _BoundaryCoordinatorError(message)
            if type(correlated.disposition) is not _types.BoundaryDisposition:
                message = "committer returned an unknown disposition"
                raise _BoundaryCoordinatorError(message)
            dispositions.append(correlated.disposition)
        return tuple(dispositions)

    async def _discard_selected(
        self,
        selected: tuple[
            tuple[_shard._Shard, _types._BoundaryRecord],
            ...,
        ],
    ) -> None:
        by_shard: dict[_shard._Shard, list[_types._BoundaryRecord]] = (
            collections.defaultdict(list)
        )
        for shard, record in selected:
            by_shard[shard].append(record)
        for shard, records in by_shard.items():
            await shard.discard_boundary_batch(tuple(records))

    async def _has_ready_boundary(self) -> bool:
        for shard in self._shards:
            if await shard.has_ready_boundary(self._grant):
                return True
        return False

    async def _settle_committed[ResultT](
        self,
        awaitable: typing.Awaitable[ResultT],
    ) -> tuple[ResultT, bool]:
        task = asyncio.ensure_future(awaitable)
        cancelled = False
        while True:
            try:
                return await asyncio.shield(task), cancelled
            except asyncio.CancelledError:
                cancelled = True
                if not task.done():
                    continue
                if task.cancelled():
                    message = "committed boundary mutation lost its outcome"
                    raise _BoundaryCoordinatorError(message) from None
                return task.result(), cancelled

    @staticmethod
    def _raise_if_cancelled(*, cancelled: bool) -> None:
        if cancelled:
            message = "boundary flusher was cancelled during settlement"
            raise _BoundaryCoordinatorError(message)

    async def _fail(self, failure: BaseException) -> None:
        if self._fatal is None:
            self._fatal = failure
            self._fatal_observer(failure)
        await self._notify_generation_waiters()

    async def _notify_generation_waiters(self) -> None:
        async with self._generation_changed:
            self._generation_changed.notify_all()

    def _raise_unavailable(self) -> None:
        if self._fatal is not None:
            message = "boundary coordinator integrity failed"
            raise _BoundaryCoordinatorError(message) from self._fatal
        if self._authority_rejected:
            message = "boundary committer rejected the exact grant"
            raise _BoundaryAuthorityLostError(message)
        if self._closing:
            message = "boundary coordinator is closing"
            raise _BoundaryCoordinatorError(message)
