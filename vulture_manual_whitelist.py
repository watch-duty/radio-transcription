# Manual Vulture Whitelist
# This file contains unused code/symbols that we intentionally keep in the codebase.
# Unlike vulture_whitelist.py, this file is maintained manually and WILL NOT be
# overwritten when running 'mise run lint:dead-code:update'.
# Please add detailed comments for why each entry is kept.

# MockCacheProvider is a mock implementation of CacheProvider used in integration and unit tests.
# Vulture excludes test directories (**/tests/**, **/test_*.py) from analysis, so it misses
# the imports of this class in tests, falsely flagging it as dead code.
from backend.pipeline.common.storage.mock_cache_provider import (
    MockCacheProvider,
)
from backend.pipeline.ingestion import grant_supervisor
from backend.pipeline.storage import ingestion_lease_store

MockCacheProvider
_.get_value

# SourceObservationResult TypedDict fields are read by key in
# CollectorRuntime. Vulture cannot connect subscript access to their
# declarations.
current_worker
current_fencing_token

# Feed TypedDict and pydantic response-model lease-health fields are read
# by key / serialized by pydantic; Vulture cannot connect that access to
# their declarations.
bcfy_calls_sid
lease_last_heartbeat
lease_status_reason

# GrantSupervisor exposes admission state for focused lifecycle tests. Vulture
# excludes tests from its analysis.
grant_supervisor.GrantSupervisor.admission_enabled

# Release causes are a public telemetry vocabulary with one storage policy.
ingestion_lease_store.LeaseReleaseCause.SHUTDOWN
ingestion_lease_store.LeaseReleaseCause.REBALANCE
ingestion_lease_store.LeaseReleaseCause.CANCELLATION
ingestion_lease_store.LeaseReleaseCause.ABANDONMENT
# FeedChangeNotificationPayload fields are consumed by Pydantic model validation
# and schema reflection, which Vulture cannot trace through direct Python
# references.
event_type
schema_version
event_id
action
occurred_at
actor_id
feed_revision
before_values
after_values

# FastAPI discovers this route handler through decorator registration.
receive_feed_change_notification

# StitcherDlqPayload TypedDict fields consumed by structure definition and dictionary creation.
error_message

# VoiceActivityDetector public methods called by unit tests and diagnostic scripts
from backend.pipeline.segmentation.audio.vad import VoiceActivityDetector

VoiceActivityDetector.is_speech_segment

