import dataclasses
import unittest
import uuid
from unittest.mock import patch

from backend.pipeline.ingestion import grant_control, worker_profiles
from backend.pipeline.ingestion import settings as settings_module
from backend.pipeline.ingestion.settings import CollectorSettings
from backend.pipeline.storage.feed_store import SourceType


def _required_env() -> dict[str, str]:
    return {
        "AUDIO_STAGING_BUCKET": "staging-bucket",
        "CONTINUOUS_PUBSUB_TOPIC_PATH": "projects/test-project/topics/test-topic",
        "ALLOYDB_HOST": "127.0.0.1",
        "ALLOYDB_USER": "radio_user",
        "ALLOYDB_DB": "radio_db",
    }


class TestCollectorSettings(unittest.TestCase):
    """Test suite for environment-driven CollectorSettings parsing."""

    def test_normal_expected_inputs(self) -> None:
        """Loads all settings from valid environment variables."""
        env = {
            **_required_env(),
            "MAX_FEEDS_PER_WORKER": "500",
            "LEASE_ADMISSION_CYCLE_BUDGET": "11",
            "LEASE_POLL_INTERVAL_SEC": "2.5",
            "HEARTBEAT_INTERVAL_SEC": "10.0",
            "HEARTBEAT_STALL_TIMEOUT_SEC": "30.0",
            "GRACEFUL_SHUTDOWN_TIMEOUT_SEC": "15.0",
            "TASK_CANCEL_BUDGET_SEC": "12.0",
            "RSS_WATCHDOG_POLL_INTERVAL_SEC": "1.0",
            "RSS_WATCHDOG_PAUSE_THRESHOLD": "0.65",
            "RSS_WATCHDOG_EXIT_THRESHOLD": "0.85",
            "RSS_WATCHDOG_PAUSE_CONSECUTIVE_SAMPLES": "5",
            "RSS_WATCHDOG_EXIT_CONSECUTIVE_SAMPLES": "5",
            "RSS_WATCHDOG_WARMUP_SEC": "30.0",
            "ALLOYDB_POOL_MIN_SIZE": "3",
            "ALLOYDB_POOL_MAX_SIZE": "25",
            "ALLOYDB_COMMAND_TIMEOUT_SEC": "40.0",
            "ALLOYDB_CONNECT_TIMEOUT_SEC": "12.5",
            "FEED_FAILURE_THRESHOLD": "7",
            "ABANDONMENT_WINDOW_SEC": "120.0",
            "ALLOYDB_PORT": "6543",
            "ALLOYDB_PASSWORD": "secret",
            "GCS_UPLOAD_MAX_RETRIES": "5",
            "GCS_UPLOAD_RETRY_BASE_DELAY_SEC": "1.0",
            "GCS_UPLOAD_RETRY_MAX_DELAY_SEC": "16.0",
            "BOOKMARK_MAX_RETRIES": "4",
            "BOOKMARK_RETRY_BASE_DELAY_SEC": "0.25",
            "BOOKMARK_RETRY_MAX_DELAY_SEC": "2.0",
            "PUBSUB_PUBLISH_MAX_RETRIES": "4",
            "PUBSUB_PUBLISH_RETRY_BASE_DELAY_SEC": "0.25",
            "PUBSUB_PUBLISH_RETRY_MAX_DELAY_SEC": "2.0",
            "GOOGLE_CLOUD_PROJECT": "test-project",
            "HEALTH_CHECK_PORT": "9090",
            "HEALTH_CHECK_STARTUP_GRACE_SEC": "90.0",
            "SEGMENTED_PUBSUB_TOPIC_PATH": "projects/test-project/topics/test-segmented-topic",
            "CAP_BCFY_FEEDS": "200",
            "CAP_BCFY_CALLS": "400",
            "CAP_OPENMHZ": "700",
            "CAP_FIRE_NOTIFICATIONS": "500",
        }

        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings()

        self.assertIsInstance(settings.worker_id, uuid.UUID)
        self.assertEqual(settings.max_feeds_per_worker, 500)
        self.assertEqual(settings.lease_admission_cycle_budget, 11)
        self.assertEqual(settings.lease_poll_interval_sec, 2.5)
        self.assertEqual(settings.heartbeat_interval_sec, 10.0)
        self.assertEqual(settings.heartbeat_stall_timeout_sec, 30.0)
        self.assertEqual(settings.graceful_shutdown_timeout_sec, 15.0)
        self.assertEqual(settings.task_cancel_budget_sec, 12.0)
        self.assertEqual(settings.rss_watchdog_poll_interval_sec, 1.0)
        self.assertEqual(settings.rss_watchdog_pause_threshold, 0.65)
        self.assertEqual(settings.rss_watchdog_exit_threshold, 0.85)
        self.assertEqual(settings.rss_watchdog_pause_consecutive_samples, 5)
        self.assertEqual(settings.rss_watchdog_exit_consecutive_samples, 5)
        self.assertEqual(settings.rss_watchdog_warmup_sec, 30.0)
        self.assertEqual(settings.audio_staging_bucket, "staging-bucket")
        self.assertEqual(settings.db.pool_min_size, 3)
        self.assertEqual(settings.db.pool_max_size, 25)
        self.assertEqual(settings.db.command_timeout_sec, 40.0)
        self.assertEqual(settings.db.connect_timeout_sec, 12.5)
        self.assertEqual(settings.feed_failure_threshold, 7)
        self.assertEqual(settings.abandonment_window_sec, 120.0)
        self.assertEqual(settings.db.host, "127.0.0.1")
        self.assertEqual(settings.db.port, 6543)
        self.assertEqual(settings.db.user, "radio_user")
        self.assertEqual(settings.db.db_name, "radio_db")
        self.assertEqual(settings.db.password, "secret")
        self.assertEqual(settings.gcs_upload_max_retries, 5)
        self.assertEqual(settings.gcs_upload_retry_base_delay_sec, 1.0)
        self.assertEqual(settings.gcs_upload_retry_max_delay_sec, 16.0)
        self.assertEqual(settings.bookmark_max_retries, 4)
        self.assertEqual(settings.bookmark_retry_base_delay_sec, 0.25)
        self.assertEqual(settings.bookmark_retry_max_delay_sec, 2.0)
        self.assertEqual(settings.pubsub_publish_max_retries, 4)
        self.assertEqual(settings.pubsub_publish_retry_base_delay_sec, 0.25)
        self.assertEqual(settings.pubsub_publish_retry_max_delay_sec, 2.0)
        self.assertEqual(settings.google_cloud_project, "test-project")
        self.assertEqual(settings.health_check_port, 9090)
        self.assertEqual(settings.health_check_startup_grace_sec, 90.0)
        self.assertEqual(
            settings.continuous_pubsub_topic_path,
            "projects/test-project/topics/test-topic",
        )
        self.assertEqual(
            settings.segmented_pubsub_topic_path,
            "projects/test-project/topics/test-segmented-topic",
        )
        self.assertEqual(settings.caps[SourceType.BCFY_FEEDS], 200)
        self.assertEqual(settings.caps[SourceType.BCFY_CALLS], 400)
        self.assertEqual(settings.caps[SourceType.OPENMHZ], 700)
        self.assertEqual(settings.caps[SourceType.FIRE_NOTIFICATIONS], 300)
        self.assertEqual(settings.worker_profile.name, "legacy")

    def test_phase1_expected_inputs(self) -> None:
        """Loads Phase 1 lease-admission settings from environment."""
        env = {
            **_required_env(),
            "LEASE_ADMISSION_CYCLE_BUDGET": "7",
            "STARTUP_STAGGER_MAX_SEC": "12.5",
            "STARTUP_JITTER_MAX_SEC": "0.25",
            "LEASE_POLL_JITTER_MAX_SEC": "0.75",
            "WORKER_INDEX": "2",
        }

        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings()

        self.assertEqual(settings.lease_admission_cycle_budget, 7)
        self.assertEqual(settings.startup_stagger_max_sec, 12.5)
        self.assertEqual(settings.startup_jitter_max_sec, 0.25)
        self.assertEqual(settings.lease_poll_jitter_max_sec, 0.75)
        self.assertEqual(settings.worker_index, 2)

    def test_edge_case_uses_defaults_and_generates_worker_id(self) -> None:
        """Uses defaults for optional settings when only required vars are set."""
        with patch.dict("os.environ", _required_env(), clear=True):
            settings = CollectorSettings()

        self.assertIsInstance(settings.worker_id, uuid.UUID)
        self.assertEqual(settings.max_feeds_per_worker, 800)
        self.assertEqual(settings.lease_poll_interval_sec, 5.0)
        self.assertEqual(settings.lease_admission_cycle_budget, 20)
        self.assertEqual(settings.startup_stagger_max_sec, 60.0)
        self.assertEqual(settings.startup_jitter_max_sec, 2.0)
        self.assertEqual(settings.lease_poll_jitter_max_sec, 1.0)
        self.assertIsNone(settings.worker_index)
        self.assertEqual(settings.heartbeat_interval_sec, 15.0)
        self.assertEqual(settings.heartbeat_stall_timeout_sec, 45.0)
        self.assertEqual(settings.graceful_shutdown_timeout_sec, 90.0)
        self.assertEqual(settings.task_cancel_budget_sec, 30.0)
        self.assertEqual(settings.rss_watchdog_poll_interval_sec, 2.0)
        self.assertEqual(settings.rss_watchdog_pause_threshold, 0.70)
        self.assertEqual(settings.rss_watchdog_exit_threshold, 0.90)
        self.assertEqual(settings.rss_watchdog_pause_consecutive_samples, 3)
        self.assertEqual(settings.rss_watchdog_exit_consecutive_samples, 3)
        self.assertEqual(settings.rss_watchdog_warmup_sec, 60.0)
        self.assertEqual(settings.db.pool_min_size, 8)
        self.assertEqual(settings.db.pool_max_size, 8)
        self.assertEqual(settings.db.command_timeout_sec, 30.0)
        self.assertEqual(settings.db.connect_timeout_sec, 10.0)
        self.assertEqual(settings.feed_failure_threshold, 5)
        self.assertEqual(settings.abandonment_window_sec, 60.0)
        self.assertEqual(settings.db.port, 6432)
        self.assertEqual(settings.db.password, "")
        self.assertEqual(settings.gcs_upload_max_retries, 3)
        self.assertEqual(settings.gcs_upload_retry_base_delay_sec, 0.5)
        self.assertEqual(settings.gcs_upload_retry_max_delay_sec, 8.0)
        self.assertEqual(settings.bookmark_max_retries, 2)
        self.assertEqual(settings.bookmark_retry_base_delay_sec, 0.5)
        self.assertEqual(settings.bookmark_retry_max_delay_sec, 4.0)
        self.assertEqual(settings.pubsub_publish_max_retries, 2)
        self.assertEqual(settings.pubsub_publish_retry_base_delay_sec, 0.5)
        self.assertEqual(settings.pubsub_publish_retry_max_delay_sec, 4.0)
        self.assertIsNone(settings.google_cloud_project)
        self.assertEqual(settings.health_check_port, 8080)
        self.assertEqual(settings.health_check_startup_grace_sec, 120.0)
        self.assertEqual(
            settings.continuous_pubsub_topic_path,
            "projects/test-project/topics/test-topic",
        )
        self.assertIsNone(settings.segmented_pubsub_topic_path)
        self.assertEqual(settings.caps[SourceType.BCFY_FEEDS], 240)
        self.assertEqual(settings.caps[SourceType.BCFY_CALLS], 600)
        self.assertEqual(settings.caps[SourceType.OPENMHZ], 900)
        self.assertEqual(settings.caps[SourceType.FIRE_NOTIFICATIONS], 600)

    def test_two_process_epochs_with_same_index_get_distinct_worker_ids(
        self,
    ) -> None:
        env = {**_required_env(), "WORKER_INDEX": "2"}

        with patch.dict("os.environ", env, clear=True):
            first = CollectorSettings()
            second = CollectorSettings()

        self.assertEqual(first.worker_index, second.worker_index)
        self.assertEqual(first.worker_index, 2)
        self.assertNotEqual(first.worker_id, second.worker_id)

    def test_explicit_worker_id_is_allowed_without_environment_override(
        self,
    ) -> None:
        worker_id = uuid.UUID("00000000-0000-0000-0000-000000000123")

        with patch.dict("os.environ", _required_env(), clear=True):
            settings = CollectorSettings(worker_id=worker_id)

        self.assertIs(settings.worker_id, worker_id)

    def test_collector_settings_retains_selected_immutable_profile(
        self,
    ) -> None:
        profile = worker_profiles.SID_DORMANT_PROFILE

        env = {
            **_required_env(),
            "BCFY_CALLS_AUTHORITY_MODE": "sid_lease",
        }
        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings(worker_profile=profile)

        self.assertEqual(
            dataclasses.replace(
                settings.worker_profile.allocations[0],
                claims_enabled=False,
            ),
            profile.allocations[0],
        )
        self.assertTrue(settings.worker_profile.allocations[0].claims_enabled)
        self.assertEqual(settings.max_feeds_per_worker, 0)
        self.assertEqual(settings.lease_admission_cycle_budget, 0)
        self.assertEqual(settings.feed_claim_caps, {})

    def test_sid_only_settings_skip_feed_caps_but_load_calls_topic(
        self,
    ) -> None:
        env = _required_env()
        del env["CONTINUOUS_PUBSUB_TOPIC_PATH"]
        env.update(
            {
                "WORKER_PROFILE": "sid-dormant",
                "BCFY_CALLS_AUTHORITY_MODE": "sid_lease",
                "MAX_FEEDS_PER_WORKER": "not-an-int",
                "LEASE_ADMISSION_CYCLE_BUDGET": "not-an-int",
                "CAP_BCFY_CALLS": "not-an-int",
                "SEGMENTED_PUBSUB_TOPIC_PATH": "projects/p/topics/segmented",
            }
        )

        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings()

        self.assertEqual(settings.caps, settings_module._DEFAULT_CAPS)
        self.assertIn(SourceType.BCFY_CALLS, settings.caps)
        self.assertEqual(settings.feed_claim_caps, {})
        self.assertEqual(settings.continuous_pubsub_topic_path, "")
        self.assertEqual(
            settings.segmented_pubsub_topic_path,
            "projects/p/topics/segmented",
        )

    def test_feed_only_profile_skips_sid_allocation_configuration(self) -> None:
        env = {
            **_required_env(),
            "MAX_SIDS_PER_WORKER": "not-an-int",
            "SID_LEASE_ADMISSION_CYCLE_BUDGET": "not-an-int",
        }

        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings()

        self.assertEqual(
            settings.worker_profile, worker_profiles.LEGACY_PROFILE
        )

    def test_explicit_empty_feed_configuration_is_not_replaced(self) -> None:
        with patch.dict("os.environ", _required_env(), clear=True):
            settings = CollectorSettings(
                caps={},
                continuous_pubsub_topic_path="",
                segmented_pubsub_topic_path=None,
            )

        self.assertEqual(settings.caps, {})
        self.assertEqual(settings.continuous_pubsub_topic_path, "")
        self.assertIsNone(settings.segmented_pubsub_topic_path)

    def test_feed_overrides_have_one_profile_allocation_source(self) -> None:
        env = {
            **_required_env(),
            "MAX_FEEDS_PER_WORKER": "123",
            "LEASE_ADMISSION_CYCLE_BUDGET": "7",
        }

        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings()
            feed = worker_profiles.allocation_for_domain(
                settings.worker_profile,
                grant_control.DomainId.FEED,
            )
            self.assertIsNotNone(feed)
            assert feed is not None
            self.assertEqual(feed.owned_cap, 123)
            self.assertEqual(feed.claims_per_cycle, 7)
            os_environ = settings_module.os.environ
            os_environ["MAX_FEEDS_PER_WORKER"] = "999"
            os_environ["LEASE_ADMISSION_CYCLE_BUDGET"] = "999"
            self.assertEqual(settings.max_feeds_per_worker, 123)
            self.assertEqual(settings.lease_admission_cycle_budget, 7)

    def test_profile_loader_delegates_once_with_all_allocations(self) -> None:
        env = {
            "WORKER_PROFILE": "mixed-dormant",
            "MAX_FEEDS_PER_WORKER": "123",
            "LEASE_ADMISSION_CYCLE_BUDGET": "7",
            "MAX_SIDS_PER_WORKER": "31",
            "SID_LEASE_ADMISSION_CYCLE_BUDGET": "1",
        }
        expected = worker_profiles.MIXED_DORMANT_PROFILE

        with (
            patch.dict("os.environ", env, clear=True),
            patch.object(
                worker_profiles,
                "resolve_worker_profile",
                return_value=expected,
            ) as resolve_profile,
        ):
            profile = settings_module.load_worker_profile_from_env()

        self.assertIs(profile, expected)
        resolve_profile.assert_called_once_with(
            "mixed-dormant",
            feed_owned_cap=123,
            feed_claims_per_cycle=7,
            sid_owned_cap=31,
            sid_claims_per_cycle=1,
        )

    def test_sid_profile_defaults_and_overrides_are_one_allocation(
        self,
    ) -> None:
        cases = (
            (
                {
                    "WORKER_PROFILE": "sid-dormant",
                    "BCFY_CALLS_AUTHORITY_MODE": "sid_lease",
                },
                32,
                2,
            ),
            (
                {
                    "WORKER_PROFILE": "sid-dormant",
                    "BCFY_CALLS_AUTHORITY_MODE": "sid_lease",
                    "MAX_SIDS_PER_WORKER": "31",
                    "SID_LEASE_ADMISSION_CYCLE_BUDGET": "1",
                },
                31,
                1,
            ),
        )

        for env, expected_cap, expected_budget in cases:
            with self.subTest(env=env):
                with patch.dict("os.environ", env, clear=True):
                    profile = settings_module.load_worker_profile_from_env()

                self.assertEqual(len(profile.allocations), 1)
                allocation = profile.allocations[0]
                self.assertIs(allocation.domain_id, grant_control.DomainId.SID)
                self.assertEqual(allocation.owned_cap, expected_cap)
                self.assertEqual(
                    allocation.claims_per_cycle,
                    expected_budget,
                )
                self.assertFalse(allocation.claims_enabled)

    def test_authority_mode_defaults_and_parses_exact_values(self) -> None:
        cases = (
            ({}, worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED),
            (
                {"BCFY_CALLS_AUTHORITY_MODE": "legacy_feed"},
                worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED,
            ),
            (
                {
                    "WORKER_PROFILE": "mixed-dormant",
                    "BCFY_CALLS_AUTHORITY_MODE": "sid_lease",
                },
                worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
            ),
        )

        for additions, expected in cases:
            with self.subTest(additions=additions):
                env = {**_required_env(), **additions}
                with patch.dict("os.environ", env, clear=True):
                    settings = CollectorSettings()

                self.assertIs(settings.bcfy_calls_authority_mode, expected)
                if (
                    expected
                    is worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED
                ):
                    self.assertEqual(settings.feed_claim_caps, settings.caps)

    def test_authority_mode_rejects_nonexact_environment_values(self) -> None:
        for value in (
            "",
            " ",
            "SID_LEASE",
            " sid_lease",
            "sid_lease ",
            "unknown",
        ):
            with self.subTest(value=value):
                env = {
                    **_required_env(),
                    "WORKER_PROFILE": "mixed-dormant",
                    "BCFY_CALLS_AUTHORITY_MODE": value,
                }
                with patch.dict("os.environ", env, clear=True):
                    with self.assertRaisesRegex(
                        ValueError,
                        "BCFY_CALLS_AUTHORITY_MODE",
                    ):
                        CollectorSettings()

    def test_authority_mode_rejects_incompatible_profiles(self) -> None:
        cases = (
            ("legacy", "sid_lease"),
            ("sid-dormant", "legacy_feed"),
        )

        for profile, mode in cases:
            with self.subTest(profile=profile, mode=mode):
                env = {
                    **_required_env(),
                    "WORKER_PROFILE": profile,
                    "BCFY_CALLS_AUTHORITY_MODE": mode,
                }
                with patch.dict("os.environ", env, clear=True):
                    with self.assertRaisesRegex(ValueError, "requires.*domain"):
                        CollectorSettings()

    def test_authority_derives_static_and_effective_feed_caps(self) -> None:
        env = {
            **_required_env(),
            "WORKER_PROFILE": "mixed-dormant",
            "BCFY_CALLS_AUTHORITY_MODE": "sid_lease",
            "CAP_BCFY_CALLS": "401",
        }
        explicit_caps = {
            SourceType.BCFY_FEEDS: 11,
            SourceType.BCFY_CALLS: 12,
            SourceType.OPENMHZ: 13,
        }

        with patch.dict("os.environ", env, clear=True):
            from_env = CollectorSettings()
            explicit = CollectorSettings(caps=explicit_caps)

        self.assertEqual(from_env.caps[SourceType.BCFY_CALLS], 401)
        self.assertNotIn(SourceType.BCFY_CALLS, from_env.feed_claim_caps)
        self.assertEqual(
            set(from_env.feed_claim_caps),
            set(from_env.caps) - {SourceType.BCFY_CALLS},
        )
        self.assertEqual(
            explicit_caps,
            {
                SourceType.BCFY_FEEDS: 11,
                SourceType.BCFY_CALLS: 12,
                SourceType.OPENMHZ: 13,
            },
        )
        self.assertEqual(
            dict(explicit.feed_claim_caps),
            {
                SourceType.BCFY_FEEDS: 11,
                SourceType.OPENMHZ: 13,
            },
        )
        with self.assertRaises(TypeError):
            explicit.feed_claim_caps[SourceType.BCFY_FEEDS] = 99

    def test_authority_is_read_once_and_frozen_after_construction(self) -> None:
        class CountingEnvironment(dict[str, str]):
            authority_reads = 0

            def get(self, key: str, default: str | None = None) -> str | None:
                if key == "BCFY_CALLS_AUTHORITY_MODE":
                    self.authority_reads += 1
                return super().get(key, default)

        env = CountingEnvironment(
            {
                **_required_env(),
                "WORKER_PROFILE": "mixed-dormant",
                "BCFY_CALLS_AUTHORITY_MODE": "sid_lease",
            }
        )

        with patch.object(settings_module.os, "environ", env):
            settings = CollectorSettings()
            original_profile = settings.worker_profile
            original_caps = settings.feed_claim_caps
            env["BCFY_CALLS_AUTHORITY_MODE"] = "legacy_feed"

            self.assertIs(
                settings.bcfy_calls_authority_mode,
                worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
            )
            self.assertIs(settings.worker_profile, original_profile)
            self.assertIs(settings.feed_claim_caps, original_caps)

        self.assertEqual(env.authority_reads, 1)

    def test_invalid_profile_fails_before_required_setting_reads(self) -> None:
        with (
            patch.dict(
                "os.environ",
                {"WORKER_PROFILE": "unknown"},
                clear=True,
            ),
            patch.object(settings_module, "_require_env") as require_env,
        ):
            with self.assertRaisesRegex(ValueError, "Unknown WORKER_PROFILE"):
                CollectorSettings()

        require_env.assert_not_called()

    def test_invalid_operational_timing_values_raise(self) -> None:
        cases = (
            ("LEASE_POLL_INTERVAL_SEC", "0"),
            ("LEASE_POLL_INTERVAL_SEC", "nan"),
            ("HEARTBEAT_INTERVAL_SEC", "-1"),
            ("HEARTBEAT_STALL_TIMEOUT_SEC", "0"),
            ("ABANDONMENT_WINDOW_SEC", "-0.5"),
            ("GRACEFUL_SHUTDOWN_TIMEOUT_SEC", "inf"),
            ("TASK_CANCEL_BUDGET_SEC", "-1"),
            ("HEALTH_CHECK_STARTUP_GRACE_SEC", "-1"),
            ("RSS_WATCHDOG_POLL_INTERVAL_SEC", "0"),
            ("RSS_WATCHDOG_WARMUP_SEC", "-1"),
        )

        for name, value in cases:
            with self.subTest(name=name, value=value):
                env = {**_required_env(), name: value}
                with patch.dict("os.environ", env, clear=True):
                    with self.assertRaisesRegex(ValueError, name.lower()):
                        CollectorSettings()

    def test_invalid_retry_and_watchdog_values_raise(self) -> None:
        cases = (
            ("FEED_FAILURE_THRESHOLD", "0"),
            ("GCS_UPLOAD_MAX_RETRIES", "-1"),
            ("GCS_UPLOAD_RETRY_BASE_DELAY_SEC", "-1"),
            ("GCS_UPLOAD_RETRY_MAX_DELAY_SEC", "nan"),
            ("BOOKMARK_MAX_RETRIES", "-1"),
            ("BOOKMARK_RETRY_BASE_DELAY_SEC", "5"),
            ("PUBSUB_PUBLISH_MAX_RETRIES", "-1"),
            ("PUBSUB_PUBLISH_RETRY_BASE_DELAY_SEC", "5"),
            ("RSS_WATCHDOG_PAUSE_THRESHOLD", "0"),
            ("RSS_WATCHDOG_EXIT_THRESHOLD", "1.1"),
            ("RSS_WATCHDOG_PAUSE_CONSECUTIVE_SAMPLES", "0"),
            ("RSS_WATCHDOG_EXIT_CONSECUTIVE_SAMPLES", "-1"),
            ("HEALTH_CHECK_PORT", "0"),
            ("WORKER_INDEX", "-1"),
        )

        for name, value in cases:
            with self.subTest(name=name, value=value):
                env = {**_required_env(), name: value}
                with patch.dict("os.environ", env, clear=True):
                    with self.assertRaisesRegex(ValueError, name.lower()):
                        CollectorSettings()

    def test_zero_retry_delay_and_warmup_controls_are_valid(self) -> None:
        env = {
            **_required_env(),
            "TASK_CANCEL_BUDGET_SEC": "0",
            "GRACEFUL_SHUTDOWN_TIMEOUT_SEC": "2",
            "GCS_UPLOAD_MAX_RETRIES": "0",
            "GCS_UPLOAD_RETRY_BASE_DELAY_SEC": "0",
            "GCS_UPLOAD_RETRY_MAX_DELAY_SEC": "0",
            "BOOKMARK_MAX_RETRIES": "0",
            "BOOKMARK_RETRY_BASE_DELAY_SEC": "0",
            "BOOKMARK_RETRY_MAX_DELAY_SEC": "0",
            "PUBSUB_PUBLISH_MAX_RETRIES": "0",
            "PUBSUB_PUBLISH_RETRY_BASE_DELAY_SEC": "0",
            "PUBSUB_PUBLISH_RETRY_MAX_DELAY_SEC": "0",
            "HEALTH_CHECK_STARTUP_GRACE_SEC": "0",
            "RSS_WATCHDOG_WARMUP_SEC": "0",
        }

        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings()

        self.assertEqual(settings.task_cancel_budget_sec, 0)
        self.assertEqual(settings.gcs_upload_max_retries, 0)
        self.assertEqual(settings.health_check_startup_grace_sec, 0)
        self.assertEqual(settings.rss_watchdog_warmup_sec, 0)

    def test_caps_partial_env_override(self) -> None:
        """Setting CAP_<NAME> for one type overrides only that one; others use defaults."""
        env = {**_required_env(), "CAP_BCFY_FEEDS": "999"}

        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings()

        self.assertEqual(settings.caps[SourceType.BCFY_FEEDS], 999)
        self.assertEqual(settings.caps[SourceType.BCFY_CALLS], 600)
        self.assertEqual(settings.caps[SourceType.OPENMHZ], 900)
        self.assertEqual(settings.caps[SourceType.FIRE_NOTIFICATIONS], 600)

    def test_caps_keys_match_default_caps_registry(self) -> None:
        """settings.caps populates exactly the SourceTypes registered in _DEFAULT_CAPS."""
        with patch.dict("os.environ", _required_env(), clear=True):
            settings = CollectorSettings()

        self.assertEqual(
            set(settings.caps),
            {
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
                SourceType.FIRE_NOTIFICATIONS,
            },
        )

    def test_invalid_missing_required_env_var_raises(self) -> None:
        """Raises ValueError when a required environment variable is missing."""
        env = _required_env()
        del env["AUDIO_STAGING_BUCKET"]

        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError) as context:
                CollectorSettings()

        self.assertIn("AUDIO_STAGING_BUCKET", str(context.exception))

    def test_invalid_empty_required_env_var_raises(self) -> None:
        """Raises ValueError when a required environment variable is empty."""
        env = {**_required_env(), "AUDIO_STAGING_BUCKET": ""}

        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError) as context:
                CollectorSettings()

        self.assertIn("AUDIO_STAGING_BUCKET", str(context.exception))

    def test_any_environment_worker_id_is_rejected(self) -> None:
        """Static ownership IDs are forbidden, even with explicit injection."""
        explicit = uuid.UUID("00000000-0000-0000-0000-000000000123")
        for value in (
            "",
            "not-a-uuid",
            "00000000-0000-0000-0000-000000000456",
        ):
            with self.subTest(value=value):
                env = {**_required_env(), "WORKER_ID": value}
                with patch.dict("os.environ", env, clear=True):
                    with self.assertRaisesRegex(
                        ValueError,
                        "WORKER_ID must not be supplied",
                    ):
                        CollectorSettings(worker_id=explicit)

    def test_invalid_integer_env_raises(self) -> None:
        """Raises ValueError for non-integer integer-backed settings."""
        env = {**_required_env(), "MAX_FEEDS_PER_WORKER": "abc"}

        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError):
                CollectorSettings()

    def test_invalid_float_env_raises(self) -> None:
        """Raises ValueError for non-float float-backed settings."""
        env = {**_required_env(), "LEASE_POLL_INTERVAL_SEC": "not-a-float"}

        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError):
                CollectorSettings()

    def test_invalid_lease_admission_cycle_budget_raises(self) -> None:
        """Rejects non-numeric, zero, and negative admission budgets."""
        for value in ("not-an-int", "0", "-1"):
            with self.subTest(value=value):
                env = {
                    **_required_env(),
                    "LEASE_ADMISSION_CYCLE_BUDGET": value,
                }

                with patch.dict("os.environ", env, clear=True):
                    with self.assertRaises(ValueError):
                        CollectorSettings()

    def test_invalid_negative_startup_and_poll_jitter_raise(self) -> None:
        """Rejects non-numeric and negative pacing/jitter values."""
        cases = (
            ("STARTUP_STAGGER_MAX_SEC", "not-a-float"),
            ("STARTUP_STAGGER_MAX_SEC", "-0.1"),
            ("STARTUP_JITTER_MAX_SEC", "not-a-float"),
            ("STARTUP_JITTER_MAX_SEC", "-0.1"),
            ("LEASE_POLL_JITTER_MAX_SEC", "not-a-float"),
            ("LEASE_POLL_JITTER_MAX_SEC", "-0.1"),
        )

        for name, value in cases:
            with self.subTest(name=name, value=value):
                env = {**_required_env(), name: value}

                with patch.dict("os.environ", env, clear=True):
                    with self.assertRaises(ValueError):
                        CollectorSettings()

    def test_nonfinite_startup_and_poll_jitter_raise(self) -> None:
        """Rejects NaN and infinity for pacing/jitter values."""
        cases = (
            ("STARTUP_STAGGER_MAX_SEC", "nan"),
            ("STARTUP_STAGGER_MAX_SEC", "inf"),
            ("STARTUP_STAGGER_MAX_SEC", "-inf"),
            ("STARTUP_JITTER_MAX_SEC", "nan"),
            ("STARTUP_JITTER_MAX_SEC", "inf"),
            ("STARTUP_JITTER_MAX_SEC", "-inf"),
            ("LEASE_POLL_JITTER_MAX_SEC", "nan"),
            ("LEASE_POLL_JITTER_MAX_SEC", "inf"),
            ("LEASE_POLL_JITTER_MAX_SEC", "-inf"),
        )

        for name, value in cases:
            with self.subTest(name=name, value=value):
                env = {**_required_env(), name: value}

                with patch.dict("os.environ", env, clear=True):
                    with self.assertRaises(ValueError):
                        CollectorSettings()

    def test_zero_pacing_values_disable_delays(self) -> None:
        """Allows zero delay and jitter values to disable pacing."""
        env = {
            **_required_env(),
            "STARTUP_STAGGER_MAX_SEC": "0",
            "STARTUP_JITTER_MAX_SEC": "0",
            "LEASE_POLL_JITTER_MAX_SEC": "0",
        }

        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings()

        self.assertEqual(settings.startup_stagger_max_sec, 0.0)
        self.assertEqual(settings.startup_jitter_max_sec, 0.0)
        self.assertEqual(settings.lease_poll_jitter_max_sec, 0.0)

    def test_worker_index_nullable_and_invalid_values(self) -> None:
        """Parses WORKER_INDEX when present and allows it to be absent."""
        with patch.dict("os.environ", _required_env(), clear=True):
            settings = CollectorSettings()

        self.assertIsNone(settings.worker_index)

        env = {**_required_env(), "WORKER_INDEX": "2"}
        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings()

        self.assertEqual(settings.worker_index, 2)

        env = {**_required_env(), "WORKER_INDEX": "not-an-int"}
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError):
                CollectorSettings()

    def test_invalid_task_cancel_budget_exceeds_graceful_shutdown_raises(
        self,
    ) -> None:
        """SHUTDOWN-02: ValueError raised when task_cancel_budget_sec +
        2s settle exceeds graceful_shutdown_timeout_sec (D-03 / D-11).
        """
        env = {
            **_required_env(),
            "TASK_CANCEL_BUDGET_SEC": "120.0",
            "GRACEFUL_SHUTDOWN_TIMEOUT_SEC": "90.0",
        }

        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError) as context:
                CollectorSettings()

        self.assertIn("task_cancel_budget_sec", str(context.exception))
        self.assertIn("graceful_shutdown_timeout_sec", str(context.exception))

    def test_edge_case_task_cancel_budget_at_boundary_does_not_raise(
        self,
    ) -> None:
        """Boundary: task_cancel_budget_sec + 2.0 == graceful_shutdown_
        timeout_sec is allowed (D-03 uses `>`, not `>=`).
        """
        env = {
            **_required_env(),
            "TASK_CANCEL_BUDGET_SEC": "88.0",
            "GRACEFUL_SHUTDOWN_TIMEOUT_SEC": "90.0",
        }

        with patch.dict("os.environ", env, clear=True):
            settings = CollectorSettings()

        self.assertEqual(settings.task_cancel_budget_sec, 88.0)
        self.assertEqual(settings.graceful_shutdown_timeout_sec, 90.0)


if __name__ == "__main__":
    unittest.main()
