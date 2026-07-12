"""Narrow exact-lane facade for bounded Feed-affine scheduling."""

from backend.pipeline.ingestion.feed_work_scheduler import _scheduler, _types

CallAuthorityLost = _types.CallAuthorityLost
CallCompleted = _types.CallCompleted
CallIntegrityFailure = _types.CallIntegrityFailure
CallMembershipRejected = _types.CallMembershipRejected
CallRetryable = _types.CallRetryable
CallSubmission = _types.CallSubmission
FeedRemoved = _types.FeedRemoved
FeedWorkScheduler = _scheduler.FeedWorkScheduler
GrantLane = _scheduler.GrantLane
LaneCloseReason = _types.LaneCloseReason
LaneClosed = _types.LaneClosed
SchedulerIntegrityError = _scheduler.SchedulerIntegrityError
Undrained = _types.Undrained

__all__ = [
    "CallAuthorityLost",
    "CallCompleted",
    "CallIntegrityFailure",
    "CallMembershipRejected",
    "CallRetryable",
    "CallSubmission",
    "FeedRemoved",
    "FeedWorkScheduler",
    "GrantLane",
    "LaneCloseReason",
    "LaneClosed",
    "SchedulerIntegrityError",
    "Undrained",
]
