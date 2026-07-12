from __future__ import annotations

import dataclasses
import inspect
import types
import typing
import unittest
from unittest import mock

from backend.pipeline.ingestion import (
    main,
    settings,
    source_runtime_specs,
    worker_profiles,
)
from backend.pipeline.storage import feed_store


def _settings_value(
    profile: worker_profiles.WorkerProfile,
    *,
    caps: dict[feed_store.SourceType, int] | None = None,
) -> settings.CollectorSettings:
    value = types.SimpleNamespace(worker_profile=profile)
    if caps is not None:
        value.caps = caps
    return typing.cast("settings.CollectorSettings", value)


class TestMain(unittest.TestCase):
    """Tests for profile-first startup and selected-domain validation."""

    def test_valid_startup_has_exact_observable_order(self) -> None:
        order: list[str] = []
        profile = worker_profiles.LEGACY_PROFILE
        collector_settings = _settings_value(profile)
        runtime = mock.Mock()
        runtime.run.side_effect = lambda: order.append("run")

        with (
            mock.patch.object(
                main.settings,
                "load_worker_profile_from_env",
                side_effect=lambda: (order.append("profile"), profile)[1],
            ) as load_profile,
            mock.patch.object(
                main.settings,
                "CollectorSettings",
                side_effect=lambda **_kwargs: (
                    order.append("settings"),
                    collector_settings,
                )[1],
            ) as settings_constructor,
            mock.patch.object(
                main,
                "_validate_selected_domain_configuration",
                side_effect=lambda *_args: order.append("selected config"),
            ) as validate_selected,
            mock.patch.object(
                main.log_helper,
                "setup_logging",
                side_effect=lambda: order.append("logging"),
            ) as setup_logging,
            mock.patch.object(
                main.tracing_utils,
                "setup_tracing",
                side_effect=lambda **_kwargs: order.append("tracing"),
            ) as setup_tracing,
            mock.patch.object(
                main.collector_runtime,
                "CollectorRuntime",
                side_effect=lambda *_args: (
                    order.append("runtime"),
                    runtime,
                )[1],
            ) as runtime_constructor,
        ):
            main.main()

        self.assertEqual(
            order,
            [
                "profile",
                "settings",
                "selected config",
                "logging",
                "tracing",
                "runtime",
                "run",
            ],
        )
        load_profile.assert_called_once_with()
        settings_constructor.assert_called_once_with(worker_profile=profile)
        validate_selected.assert_called_once_with(profile, collector_settings)
        setup_logging.assert_called_once_with()
        setup_tracing.assert_called_once_with(
            service_name="ingestion-service",
            is_ingestion=True,
        )
        runtime_constructor.assert_called_once_with(
            main.router.route_capturer,
            collector_settings,
        )
        runtime.run.assert_called_once_with()

    def test_invalid_profile_has_no_settings_telemetry_or_runtime_calls(
        self,
    ) -> None:
        with (
            mock.patch.object(
                main.settings,
                "load_worker_profile_from_env",
                side_effect=ValueError("invalid profile"),
            ),
            mock.patch.object(
                main.settings, "CollectorSettings"
            ) as settings_ctor,
            mock.patch.object(
                main,
                "_validate_selected_domain_configuration",
            ) as validate_selected,
            mock.patch.object(
                main.log_helper, "setup_logging"
            ) as setup_logging,
            mock.patch.object(
                main.tracing_utils, "setup_tracing"
            ) as setup_tracing,
            mock.patch.object(
                main.collector_runtime,
                "CollectorRuntime",
            ) as runtime_constructor,
            mock.patch(
                "backend.pipeline.common.log_helper.cloud_logging.Client",
            ) as cloud_logging_client,
            mock.patch(
                "backend.pipeline.common.tracing_utils.CloudTraceSpanExporter",
            ) as cloud_trace_exporter,
            mock.patch(
                "backend.pipeline.common.tracing_utils.BatchSpanProcessor",
            ) as batch_span_processor,
        ):
            with self.assertRaisesRegex(ValueError, "invalid profile"):
                main.main()

        settings_ctor.assert_not_called()
        validate_selected.assert_not_called()
        setup_logging.assert_not_called()
        setup_tracing.assert_not_called()
        cloud_logging_client.assert_not_called()
        cloud_trace_exporter.assert_not_called()
        batch_span_processor.assert_not_called()
        runtime_constructor.assert_not_called()

    def test_invalid_selected_config_has_no_telemetry_or_runtime_calls(
        self,
    ) -> None:
        profile = worker_profiles.LEGACY_PROFILE
        collector_settings = _settings_value(profile)

        with (
            mock.patch.object(
                main.settings,
                "load_worker_profile_from_env",
                return_value=profile,
            ),
            mock.patch.object(
                main.settings,
                "CollectorSettings",
                return_value=collector_settings,
            ) as settings_ctor,
            mock.patch.object(
                main,
                "_validate_selected_domain_configuration",
                side_effect=RuntimeError("invalid selected config"),
            ),
            mock.patch.object(
                main.log_helper, "setup_logging"
            ) as setup_logging,
            mock.patch.object(
                main.tracing_utils, "setup_tracing"
            ) as setup_tracing,
            mock.patch.object(
                main.collector_runtime,
                "CollectorRuntime",
            ) as runtime_constructor,
            mock.patch(
                "backend.pipeline.common.log_helper.cloud_logging.Client",
            ) as cloud_logging_client,
            mock.patch(
                "backend.pipeline.common.tracing_utils.CloudTraceSpanExporter",
            ) as cloud_trace_exporter,
            mock.patch(
                "backend.pipeline.common.tracing_utils.BatchSpanProcessor",
            ) as batch_span_processor,
        ):
            with self.assertRaisesRegex(
                RuntimeError, "invalid selected config"
            ):
                main.main()

        settings_ctor.assert_called_once_with(worker_profile=profile)
        setup_logging.assert_not_called()
        setup_tracing.assert_not_called()
        cloud_logging_client.assert_not_called()
        cloud_trace_exporter.assert_not_called()
        batch_span_processor.assert_not_called()
        runtime_constructor.assert_not_called()

    def test_static_worker_id_has_no_validation_telemetry_or_runtime_calls(
        self,
    ) -> None:
        env = {
            "WORKER_ID": "00000000-0000-0000-0000-000000000123",
            "AUDIO_STAGING_BUCKET": "staging-bucket",
            "CONTINUOUS_PUBSUB_TOPIC_PATH": "projects/p/topics/t",
        }

        with (
            mock.patch.dict("os.environ", env, clear=True),
            mock.patch.object(
                main,
                "_validate_selected_domain_configuration",
            ) as validate_selected,
            mock.patch.object(
                main.log_helper, "setup_logging"
            ) as setup_logging,
            mock.patch.object(
                main.tracing_utils, "setup_tracing"
            ) as setup_tracing,
            mock.patch.object(
                main.collector_runtime,
                "CollectorRuntime",
            ) as runtime_constructor,
            mock.patch(
                "backend.pipeline.common.log_helper.cloud_logging.Client",
            ) as cloud_logging_client,
            mock.patch(
                "backend.pipeline.common.tracing_utils.CloudTraceSpanExporter",
            ) as cloud_trace_exporter,
            mock.patch(
                "backend.pipeline.common.tracing_utils.BatchSpanProcessor",
            ) as batch_span_processor,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "WORKER_ID must not be supplied",
            ):
                main.main()

        validate_selected.assert_not_called()
        setup_logging.assert_not_called()
        setup_tracing.assert_not_called()
        cloud_logging_client.assert_not_called()
        cloud_trace_exporter.assert_not_called()
        batch_span_processor.assert_not_called()
        runtime_constructor.assert_not_called()

    def test_feed_only_validates_existing_topics_caps_and_calls(self) -> None:
        profile = worker_profiles.LEGACY_PROFILE
        source_types = list(source_runtime_specs.claimable_source_specs())
        collector_settings = _settings_value(
            profile,
            caps=dict.fromkeys(source_types, 1),
        )

        with (
            mock.patch.object(
                main.router,
                "supported_source_types",
                return_value=[
                    source_type.value for source_type in source_types
                ],
            ) as supported_source_types,
            mock.patch.object(
                main.router,
                "resolve_topic_path",
                return_value="projects/p/topics/t",
            ) as resolve_topic_path,
        ):
            main._validate_selected_domain_configuration(
                profile,
                collector_settings,
            )

        supported_source_types.assert_called_once_with()
        self.assertEqual(resolve_topic_path.call_count, len(source_types))
        self.assertIn(feed_store.SourceType.BCFY_CALLS, source_types)
        for source_type in source_types:
            resolve_topic_path.assert_any_call(
                source_type,
                collector_settings,
            )

    def test_sid_only_skips_all_feed_configuration_callbacks(self) -> None:
        profile = worker_profiles.SID_DORMANT_PROFILE
        collector_settings = _settings_value(profile)

        with (
            mock.patch.object(
                main.router,
                "supported_source_types",
            ) as supported_source_types,
            mock.patch.object(
                main.router,
                "resolve_topic_path",
            ) as resolve_topic_path,
        ):
            main._validate_selected_domain_configuration(
                profile,
                collector_settings,
            )

        supported_source_types.assert_not_called()
        resolve_topic_path.assert_not_called()

    def test_mixed_profile_validates_each_selected_branch_once(self) -> None:
        profile = worker_profiles.MIXED_DORMANT_PROFILE
        collector_settings = _settings_value(profile)

        with (
            mock.patch.object(
                main,
                "_validate_feed_domain_configuration",
            ) as validate_feed,
            mock.patch.object(
                main,
                "_validate_sid_domain_configuration",
            ) as validate_sid,
        ):
            main._validate_selected_domain_configuration(
                profile,
                collector_settings,
            )

        validate_feed.assert_called_once_with(collector_settings)
        validate_sid.assert_called_once_with(profile.allocations[1])

    def test_sid_claims_enabled_is_rejected_before_runtime(self) -> None:
        sid_allocation = dataclasses.replace(
            worker_profiles.SID_DORMANT_PROFILE.allocations[0],
            claims_enabled=True,
        )
        profile = dataclasses.replace(
            worker_profiles.SID_DORMANT_PROFILE,
            allocations=(sid_allocation,),
        )
        collector_settings = _settings_value(profile)

        with self.assertRaisesRegex(RuntimeError, "SID claims must remain"):
            main._validate_selected_domain_configuration(
                profile,
                collector_settings,
            )

    def test_feed_topic_error_preserves_existing_startup_context(self) -> None:
        profile = worker_profiles.LEGACY_PROFILE
        collector_settings = _settings_value(
            profile,
            caps={feed_store.SourceType.BCFY_CALLS: 1},
        )

        with (
            mock.patch.object(
                main.router,
                "supported_source_types",
                return_value=[feed_store.SourceType.BCFY_CALLS.value],
            ),
            mock.patch.object(
                main.router,
                "resolve_topic_path",
                side_effect=ValueError("segmented topic missing"),
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "Startup check failed for source type bcfy_calls: "
                "segmented topic missing",
            ):
                main._validate_selected_domain_configuration(
                    profile,
                    collector_settings,
                )

    def test_feed_empty_topic_is_rejected_by_selected_validation(self) -> None:
        profile = worker_profiles.LEGACY_PROFILE
        collector_settings = _settings_value(
            profile,
            caps={feed_store.SourceType.BCFY_FEEDS: 1},
        )

        with (
            mock.patch.object(
                main.router,
                "supported_source_types",
                return_value=[feed_store.SourceType.BCFY_FEEDS.value],
            ),
            mock.patch.object(
                main.router,
                "resolve_topic_path",
                return_value="",
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "Pub/Sub topic path not configured",
            ):
                main._validate_selected_domain_configuration(
                    profile,
                    collector_settings,
                )

    def test_feed_registry_mismatch_preserves_existing_error(self) -> None:
        profile = worker_profiles.LEGACY_PROFILE
        collector_settings = _settings_value(profile, caps={})

        with (
            mock.patch.object(
                main.router,
                "supported_source_types",
                return_value=[feed_store.SourceType.BCFY_CALLS.value],
            ),
            mock.patch.object(
                main.router,
                "resolve_topic_path",
                return_value="projects/p/topics/t",
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "Startup invariant violated: collector registry",
            ):
                main._validate_selected_domain_configuration(
                    profile,
                    collector_settings,
                )

    def test_selected_config_rejects_profile_replacement(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "must retain"):
            main._validate_selected_domain_configuration(
                worker_profiles.LEGACY_PROFILE,
                _settings_value(worker_profiles.SID_DORMANT_PROFILE),
            )

    def test_selected_configuration_calls_no_sid_runtime_apis(self) -> None:
        source = inspect.getsource(main)

        self.assertNotIn("claim(", source)
        self.assertNotIn("heartbeat(", source)
        self.assertNotIn("membership", source)
        self.assertNotIn("scheduler", source)
        self.assertNotIn("poller", source)


if __name__ == "__main__":
    unittest.main()
