"""Pure bootstrap and progress policy for Broadcastify Calls cursors."""

from __future__ import annotations

import dataclasses
import datetime
import typing

if typing.TYPE_CHECKING:
    import collections.abc


__all__ = ["BootstrapDecision", "bootstrap_cursor"]


@dataclasses.dataclass(frozen=True, slots=True)
class BootstrapDecision:
    """Evidence for one bounded durable Feed cursor bootstrap.

    Attributes:
        pos: Inclusive replay position, or ``None`` for all-null input.
        durable_minimum: Minimum non-null durable Feed cursor observed.
        replay_floor: Oldest replay position allowed for this bootstrap.
        clamped: Whether ``pos`` was raised to ``replay_floor``.
    """

    pos: datetime.datetime | None
    durable_minimum: datetime.datetime | None
    replay_floor: datetime.datetime
    clamped: bool


def _require_utc_datetime(
    value: object,
    *,
    field_name: str,
) -> datetime.datetime:
    if not isinstance(value, datetime.datetime):
        msg = f"{field_name} must be a datetime"
        raise TypeError(msg)
    if value.utcoffset() != datetime.timedelta(0):
        msg = f"{field_name} must be UTC-aware"
        raise ValueError(msg)
    return value


def bootstrap_cursor(
    cursors: collections.abc.Iterable[datetime.datetime | None],
    *,
    now: datetime.datetime,
) -> BootstrapDecision:
    """Select one bounded inclusive replay position from Feed cursors.

    Args:
        cursors: Eligible independent durable Feed cursors.
        now: Explicit UTC time used to calculate the replay floor.

    Returns:
        Immutable bootstrap evidence with the selected replay position.

    Raises:
        TypeError: ``now`` or a non-null Feed cursor is not a datetime.
        ValueError: ``now`` or a non-null Feed cursor is not UTC-aware.
    """
    validated_now = _require_utc_datetime(now, field_name="now")
    replay_floor = validated_now - datetime.timedelta(minutes=5)
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

    if durable_minimum is None:
        return BootstrapDecision(
            pos=None,
            durable_minimum=None,
            replay_floor=replay_floor,
            clamped=False,
        )

    clamped = durable_minimum < replay_floor
    return BootstrapDecision(
        pos=replay_floor if clamped else durable_minimum,
        durable_minimum=durable_minimum,
        replay_floor=replay_floor,
        clamped=clamped,
    )
