"""Controlled exact-lane lifecycle for one Broadcastify Calls SID grant."""

from __future__ import annotations

import asyncio
import enum
import typing

from backend.pipeline.ingestion import (
    feed_work_scheduler,
    grant_control,
)

if typing.TYPE_CHECKING:
    from backend.pipeline.storage import ingestion_lease_store


__all__ = ["BcfyCallsSidRunner", "SidRunnerIntegrityError"]


class SidRunnerIntegrityError(grant_control.GrantControlIntegrityError):
    """The SID runner cannot prove mutation closure for its exact lane."""


class _TerminalSignal(enum.IntEnum):
    """Deterministic precedence for monotonic runner signals."""

    STOP = 1
    LOSS = 2
    INTEGRITY = 3


class BcfyCallsSidRunner:
    """Open and settle one invocation-local scheduler lane per exact grant."""

    __slots__ = ("_scheduler",)

    def __init__(
        self,
        scheduler: feed_work_scheduler.FeedWorkScheduler,
    ) -> None:
        """Bind the one process scheduler shared by every SID invocation."""
        required = (
            "open_lane",
            "integrity_failure_event",
            "raise_if_failed",
        )
        if any(not hasattr(scheduler, name) for name in required):
            message = "scheduler does not provide the SID runner contract"
            raise TypeError(message)
        self._scheduler = scheduler

    async def run(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        payload: ingestion_lease_store.LeaseSnapshot,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        """Wait for control signals and acknowledge a settled exact lane."""
        del payload
        try:
            lane = self._scheduler.open_lane(grant)
        except feed_work_scheduler.SchedulerIntegrityError as exc:
            message = "scheduler failed before the exact lane opened"
            raise SidRunnerIntegrityError(message) from exc

        stop_wait = asyncio.create_task(
            context.stop_requested.wait(),
            name="bcfy-calls-sid-runner-stop-wait",
        )
        loss_wait = asyncio.create_task(
            context.grant_lost.wait(),
            name="bcfy-calls-sid-runner-loss-wait",
        )
        integrity_wait = asyncio.create_task(
            self._scheduler.integrity_failure_event.wait(),
            name="bcfy-calls-sid-runner-integrity-wait",
        )
        signal_waiters = (stop_wait, loss_wait, integrity_wait)
        close_tasks: list[asyncio.Task[object]] = []
        try:
            await asyncio.wait(
                signal_waiters,
                return_when=asyncio.FIRST_COMPLETED,
            )
            signal = self._require_current_signal(context)

            close_signal, result = await self._close_for_signal(
                lane,
                context,
                signal,
                loss_wait=loss_wait,
                integrity_wait=integrity_wait,
                close_tasks=close_tasks,
            )
            self._validate_close_result(grant, close_signal, result)

            latest = self._current_signal(context)
            if latest is not None and latest > signal:
                signal = latest
            if signal is _TerminalSignal.INTEGRITY:
                self._raise_scheduler_integrity()
            if signal is _TerminalSignal.LOSS:
                return grant_control.RunLost()
            return grant_control.RunStopped(
                grant_control.TerminalCause.PLANNED_DRAIN
            )
        except asyncio.CancelledError:
            reason = self._cancellation_reason(context)
            cleanup = self._create_close_task(lane, reason)
            close_tasks.append(cleanup)
            await self._settle_cancelled_cleanup(cleanup)
            raise
        except SidRunnerIntegrityError:
            raise
        except feed_work_scheduler.SchedulerIntegrityError as exc:
            message = "scheduler failed while the exact lane was closing"
            raise SidRunnerIntegrityError(message) from exc
        except Exception as exc:
            message = "exact lane close failed before settlement was proved"
            raise SidRunnerIntegrityError(message) from exc
        finally:
            await self._cancel_and_settle(signal_waiters)
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)

    async def _close_for_signal(
        self,
        lane: feed_work_scheduler.GrantLane,
        context: grant_control.RunContext,
        signal: _TerminalSignal,
        *,
        loss_wait: asyncio.Task[bool],
        integrity_wait: asyncio.Task[bool],
        close_tasks: list[asyncio.Task[object]],
    ) -> tuple[_TerminalSignal, object]:
        """Close strongly while retaining loss/integrity race observation."""
        close_task = self._create_close_task(
            lane,
            self._close_reason(signal, context),
        )
        close_tasks.append(close_task)
        close_signal = signal

        while True:
            waiters: set[asyncio.Task[object]] = {close_task}
            if close_signal is not _TerminalSignal.INTEGRITY:
                waiters.add(typing.cast("asyncio.Task[object]", integrity_wait))
            if close_signal is _TerminalSignal.STOP:
                waiters.add(typing.cast("asyncio.Task[object]", loss_wait))
            done, _pending = await asyncio.wait(
                waiters,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if close_task in done:
                return close_signal, await close_task

            latest = self._current_signal(context)
            if latest is _TerminalSignal.INTEGRITY and (
                close_signal is not _TerminalSignal.INTEGRITY
            ):
                close_signal = latest
                close_task = self._create_close_task(
                    lane,
                    self._close_reason(latest, context),
                )
                close_tasks.append(close_task)
                continue
            if (
                latest is _TerminalSignal.LOSS
                and close_signal is _TerminalSignal.STOP
            ):
                close_signal = latest
                close_task = self._create_close_task(
                    lane,
                    feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
                )
                close_tasks.append(close_task)
                continue

    def _current_signal(
        self,
        context: grant_control.RunContext,
    ) -> _TerminalSignal | None:
        """Recheck monotonic predicates in deterministic precedence order."""
        if self._scheduler.integrity_failure_event.is_set():
            return _TerminalSignal.INTEGRITY
        if context.grant_lost.is_set():
            return _TerminalSignal.LOSS
        if context.stop_requested.is_set():
            return _TerminalSignal.STOP
        return None

    def _require_current_signal(
        self,
        context: grant_control.RunContext,
    ) -> _TerminalSignal:
        signal = self._current_signal(context)
        if signal is None:
            message = "runner signal waiter completed without a predicate"
            raise SidRunnerIntegrityError(message)
        return signal

    def _close_reason(
        self,
        signal: _TerminalSignal,
        context: grant_control.RunContext,
    ) -> feed_work_scheduler.LaneCloseReason:
        """Choose one strong reason without weakening confirmed loss."""
        if context.grant_lost.is_set() or signal is _TerminalSignal.LOSS:
            return feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
        if signal is _TerminalSignal.INTEGRITY:
            return feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN
        return feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN

    def _cancellation_reason(
        self,
        context: grant_control.RunContext,
    ) -> feed_work_scheduler.LaneCloseReason:
        """Request immediate cleanup without manufacturing a valid outcome."""
        if context.grant_lost.is_set():
            return feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
        return feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN

    @staticmethod
    def _create_close_task(
        lane: feed_work_scheduler.GrantLane,
        reason: feed_work_scheduler.LaneCloseReason,
    ) -> asyncio.Task[object]:
        return asyncio.create_task(
            lane.close(reason),
            name="bcfy-calls-sid-runner-lane-close",
        )

    @staticmethod
    async def _settle_cancelled_cleanup(task: asyncio.Task[object]) -> None:
        """Keep raw cancellation pending until the cleanup task settles."""
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                return
        if not task.cancelled():
            task.exception()

    @staticmethod
    async def _cancel_and_settle(
        tasks: tuple[asyncio.Task[bool], ...],
    ) -> None:
        """Cancel and await every registered signal helper explicitly."""
        for task in tasks:
            if not task.done():
                task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

    def _raise_scheduler_integrity(self) -> typing.Never:
        try:
            self._scheduler.raise_if_failed()
        except feed_work_scheduler.SchedulerIntegrityError as exc:
            message = "process scheduler integrity failed"
            raise SidRunnerIntegrityError(message) from exc
        message = "scheduler integrity event has no failure evidence"
        raise SidRunnerIntegrityError(message)

    @staticmethod
    def _validate_close_result(
        grant: ingestion_lease_store.LeaseGrant,
        close_signal: _TerminalSignal,
        result: object,
    ) -> None:
        if isinstance(result, feed_work_scheduler.Undrained):
            message = "exact lane did not prove mutation closure"
            raise SidRunnerIntegrityError(message)
        if not isinstance(result, feed_work_scheduler.LaneClosed):
            message = "exact lane returned an invalid close result"
            raise SidRunnerIntegrityError(message)
        if result.grant != grant:
            message = "exact lane close result crossed grant identity"
            raise SidRunnerIntegrityError(message)
        if (
            close_signal is _TerminalSignal.STOP
            and result.reason
            is not feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN
        ):
            message = "planned drain returned the wrong close reason"
            raise SidRunnerIntegrityError(message)
        if (
            close_signal is _TerminalSignal.LOSS
            and result.reason
            is not feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
        ):
            message = "pending loss did not escalate exact lane cancellation"
            raise SidRunnerIntegrityError(message)
        if close_signal is _TerminalSignal.INTEGRITY and result.reason not in (
            feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
            feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN,
        ):
            message = "scheduler integrity did not request strong lane cleanup"
            raise SidRunnerIntegrityError(message)
