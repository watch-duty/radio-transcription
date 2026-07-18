"""Pure bootstrap and progress policy for Broadcastify Calls cursors."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import typing

if typing.TYPE_CHECKING:
    import collections.abc

    from backend.pipeline.storage import ingestion_lease_store


__all__ = [
    "CursorIntegrityError",
    "CursorOutcome",
    "LeaseCursor",
    "PageCandidate",
    "clamp_live_request_start",
    "minimum_durable_cursor",
]


class CursorIntegrityError(ValueError):
    """A Lease Cursor operation violates its exact page contract."""


class CursorOutcome(enum.Enum):
    """Behavior-driving result of settling one exact source page."""

    COVERED = enum.auto()
    REPLAYABLE = enum.auto()


_LIVE_REQUEST_WINDOW = datetime.timedelta(minutes=5)


@dataclasses.dataclass(frozen=True, slots=True)
class PageCandidate:
    """One proposed source-page position bound to exact Lease authority.

    ``last_pos=None`` represents a provider page without usable cursor
    evidence. Preparing a candidate never advances the Lease Cursor.
    """

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    last_pos: datetime.datetime | None

    def __post_init__(self) -> None:
        _require_page_sequence(self.page_sequence)
        if self.last_pos is not None:
            _require_integrity_utc_datetime(
                self.last_pos,
                field_name="last_pos",
            )


def _require_utc_datetime(
    value: datetime.datetime,
    *,
    field_name: str,
) -> datetime.datetime:
    if value.utcoffset() != datetime.timedelta(0):
        msg = f"{field_name} must be UTC-aware"
        raise ValueError(msg)
    return value


def _require_integrity_utc_datetime(
    value: datetime.datetime,
    *,
    field_name: str,
) -> datetime.datetime:
    try:
        return _require_utc_datetime(value, field_name=field_name)
    except ValueError as exc:
        raise CursorIntegrityError(str(exc)) from exc


def _require_page_sequence(value: int) -> int:
    if isinstance(value, bool):
        msg = "page_sequence must be an integer"
        raise CursorIntegrityError(msg)
    if value < 0:
        msg = "page_sequence must be nonnegative"
        raise CursorIntegrityError(msg)
    return value


def minimum_durable_cursor(
    cursors: collections.abc.Iterable[datetime.datetime | None],
) -> datetime.datetime | None:
    """Return the minimum non-null durable Feed cursor.

    The result remains unclamped so callers can apply the provider's live
    retrieval window at the actual request boundary.

    Args:
        cursors: Eligible independent durable Feed cursors.

    Returns:
        The minimum inclusive cursor, or ``None`` for all-null input.

    Raises:
        ValueError: A non-null Feed cursor is not UTC-aware.
    """
    durable_minimum: datetime.datetime | None = None
    for cursor in cursors:
        if cursor is None:
            continue
        validated_cursor = _require_utc_datetime(
            cursor,
            field_name="Feed cursor",
        )
        if durable_minimum is None or validated_cursor < durable_minimum:
            durable_minimum = validated_cursor
    return durable_minimum


def clamp_live_request_start(
    requested_start: datetime.datetime | None,
    *,
    now: datetime.datetime,
) -> datetime.datetime | None:
    """Clamp a known request start to Broadcastify's live retrieval window.

    Args:
        requested_start: Desired inclusive position, or ``None`` to omit it.
        now: UTC request-boundary time used to calculate the source window.

    Returns:
        The requested position within the five-minute live window, or ``None``.

    Raises:
        ValueError: ``now`` or ``requested_start`` is not UTC-aware.
    """
    validated_now = _require_utc_datetime(now, field_name="now")
    if requested_start is None:
        return None

    validated_start = _require_utc_datetime(
        requested_start,
        field_name="requested_start",
    )
    live_window_start = validated_now - _LIVE_REQUEST_WINDOW
    return max(validated_start, live_window_start)


class LeaseCursor:
    """Grant-local cursor advanced only by an exact covered page.

    The cursor retains at most one outstanding candidate. Its state is
    process-local and is reconstructed from durable Feed cursors after restart.
    """

    __slots__ = (
        "_grant",
        "_next_page_sequence",
        "_outstanding_candidate",
        "_pos",
    )

    def __init__(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        *,
        pos: datetime.datetime | None,
    ) -> None:
        """Create fresh in-memory state for one exact Lease grant."""
        self._grant = grant
        self._pos = (
            None
            if pos is None
            else _require_integrity_utc_datetime(pos, field_name="pos")
        )
        self._next_page_sequence = 0
        self._outstanding_candidate: PageCandidate | None = None

    @property
    def pos(self) -> datetime.datetime | None:
        """Return the current grant-local upstream cursor."""
        return self._pos

    def prepare(
        self,
        last_pos: datetime.datetime | None,
    ) -> PageCandidate:
        """Prepare the exact next page without advancing progress.

        ``last_pos=None`` represents a provider response without usable cursor
        evidence.

        Raises:
            CursorIntegrityError: Another page is outstanding or ``last_pos``
                is invalid or regressive.
        """
        if self._outstanding_candidate is not None:
            msg = "A page candidate is already outstanding"
            raise CursorIntegrityError(msg)
        candidate = PageCandidate(
            grant=self._grant,
            page_sequence=self._next_page_sequence,
            last_pos=last_pos,
        )
        if (
            candidate.last_pos is not None
            and self._pos is not None
            and candidate.last_pos < self._pos
        ):
            msg = "last_pos must not regress the Lease Cursor"
            raise CursorIntegrityError(msg)
        self._outstanding_candidate = candidate
        return candidate

    def settle(
        self,
        candidate: PageCandidate,
        outcome: CursorOutcome,
    ) -> None:
        """Settle the exact outstanding candidate once.

        ``COVERED`` accepts a candidate's position when one exists.
        ``REPLAYABLE`` releases a positioned page while retaining the previous
        position.

        Raises:
            CursorIntegrityError: The candidate is crossed or already settled,
                or its cursor evidence disagrees with the outcome.
        """
        if candidate is not self._outstanding_candidate:
            msg = "candidate is not the exact outstanding page"
            raise CursorIntegrityError(msg)

        last_pos = candidate.last_pos
        next_pos = self._pos
        if outcome is CursorOutcome.COVERED:
            if last_pos is not None:
                next_pos = (
                    last_pos if next_pos is None else max(next_pos, last_pos)
                )
        elif outcome is CursorOutcome.REPLAYABLE:
            if last_pos is None:
                msg = "replayable outcome requires a page position"
                raise CursorIntegrityError(msg)
        else:
            typing.assert_never(outcome)

        self._pos = next_pos
        self._next_page_sequence += 1
        self._outstanding_candidate = None
