from __future__ import annotations

import inspect
import json
import subprocess
import types
import typing
import unittest
from unittest import mock

import pytest

if typing.TYPE_CHECKING:
    import pathlib

from backend.pipeline.ingestion import (
    main,
    settings,
    source_runtime_specs,
    worker_profiles,
)
from backend.pipeline.storage import feed_store
from scripts import check_ty_baseline as ty_baseline_checker


def _settings_value(
    profile: worker_profiles.WorkerProfile,
    *,
    caps: dict[feed_store.SourceType, int] | None = None,
    mode: worker_profiles.BcfyCallsAuthorityMode = (
        worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED
    ),
    feed_claim_caps: dict[feed_store.SourceType, int] | None = None,
) -> settings.CollectorSettings:
    static_caps = (
        source_runtime_specs.default_caps() if caps is None else dict(caps)
    )
    claim_caps = (
        dict(static_caps) if feed_claim_caps is None else dict(feed_claim_caps)
    )
    value = types.SimpleNamespace(
        worker_profile=profile,
        bcfy_calls_authority_mode=mode,
        caps=static_caps,
        feed_claim_caps=claim_caps,
        continuous_pubsub_topic_path="projects/p/topics/continuous",
        segmented_pubsub_topic_path="projects/p/topics/segmented",
    )
    return typing.cast("settings.CollectorSettings", value)


class TestMain(unittest.TestCase):
    """Tests for profile-first startup and selected-domain validation."""

    def test_valid_startup_has_exact_observable_order(self) -> None:
        order: list[str] = []
        profile = worker_profiles.derive_bcfy_calls_authority(
            worker_profiles.LEGACY_PROFILE,
            worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED,
        )
        collector_settings = _settings_value(profile)
        runtime = mock.Mock()
        runtime.run.side_effect = lambda: order.append("run")

        with (
            mock.patch.object(
                main.settings,
                "CollectorSettings",
                side_effect=lambda: (
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
                "settings",
                "selected config",
                "logging",
                "tracing",
                "runtime",
                "run",
            ],
        )
        settings_constructor.assert_called_once_with()
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

    def test_invalid_settings_has_no_validation_telemetry_or_runtime_calls(
        self,
    ) -> None:
        with (
            mock.patch.object(
                main.settings,
                "CollectorSettings",
                side_effect=ValueError("invalid profile"),
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

        settings_ctor.assert_called_once_with()
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
        profile = worker_profiles.derive_bcfy_calls_authority(
            worker_profiles.LEGACY_PROFILE,
            worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED,
        )
        collector_settings = _settings_value(profile)

        with (
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

        settings_ctor.assert_called_once_with()
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
        profile = worker_profiles.derive_bcfy_calls_authority(
            worker_profiles.LEGACY_PROFILE,
            worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED,
        )
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

    def test_sid_only_skips_feed_registry_but_validates_calls_topic(
        self,
    ) -> None:
        profile = worker_profiles.derive_bcfy_calls_authority(
            worker_profiles.SID_DORMANT_PROFILE,
            worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
        )
        collector_settings = _settings_value(
            profile,
            mode=worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
            feed_claim_caps={},
        )

        with (
            mock.patch.object(
                main.router,
                "supported_source_types",
            ) as supported_source_types,
            mock.patch.object(
                main.router,
                "resolve_topic_path",
                return_value="projects/p/topics/segmented",
            ) as resolve_topic_path,
        ):
            main._validate_selected_domain_configuration(
                profile,
                collector_settings,
            )

        supported_source_types.assert_not_called()
        resolve_topic_path.assert_called_once_with(
            feed_store.SourceType.BCFY_CALLS,
            collector_settings,
        )

    def test_mixed_profile_validates_each_selected_branch_once(self) -> None:
        profile = worker_profiles.derive_bcfy_calls_authority(
            worker_profiles.MIXED_DORMANT_PROFILE,
            worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED,
        )
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
        validate_sid.assert_called_once_with(
            profile.allocations[1],
            collector_settings,
        )

    def test_impossible_dual_and_neither_authority_are_rejected(self) -> None:
        sid_profile = worker_profiles.derive_bcfy_calls_authority(
            worker_profiles.MIXED_DORMANT_PROFILE,
            worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
        )
        legacy_profile = worker_profiles.derive_bcfy_calls_authority(
            worker_profiles.MIXED_DORMANT_PROFILE,
            worker_profiles.BcfyCallsAuthorityMode.LEGACY_FEED,
        )
        static_caps = source_runtime_specs.default_caps()
        cases = (
            (
                sid_profile,
                _settings_value(sid_profile, caps=static_caps),
            ),
            (
                legacy_profile,
                _settings_value(
                    legacy_profile,
                    caps=static_caps,
                    mode=worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
                    feed_claim_caps={
                        source_type: cap
                        for source_type, cap in static_caps.items()
                        if source_type is not feed_store.SourceType.BCFY_CALLS
                    },
                ),
            ),
        )

        for profile, collector_settings in cases:
            with self.subTest(
                mode=collector_settings.bcfy_calls_authority_mode.value,
            ):
                with self.assertRaisesRegex(RuntimeError, "Exactly one"):
                    main._validate_selected_domain_configuration(
                        profile,
                        collector_settings,
                    )

    def test_sid_production_entrypoint_selects_composed_runtime(self) -> None:
        env = {
            "WORKER_PROFILE": "mixed-dormant",
            "BCFY_CALLS_AUTHORITY_MODE": "sid_lease",
            "AUDIO_STAGING_BUCKET": "staging-bucket",
            "CONTINUOUS_PUBSUB_TOPIC_PATH": "projects/p/topics/continuous",
            "SEGMENTED_PUBSUB_TOPIC_PATH": "projects/p/topics/segmented",
            "ALLOYDB_HOST": "127.0.0.1",
            "ALLOYDB_USER": "radio_user",
            "ALLOYDB_DB": "radio_db",
        }

        runtime_instance = mock.Mock()
        with (
            mock.patch.dict("os.environ", env, clear=True),
            mock.patch.object(main.log_helper, "setup_logging") as logging,
            mock.patch.object(main.tracing_utils, "setup_tracing") as tracing,
            mock.patch.object(
                main.collector_runtime,
                "CollectorRuntime",
                return_value=runtime_instance,
            ) as runtime,
        ):
            main.main()

        logging.assert_called_once_with()
        tracing.assert_called_once_with(
            service_name="ingestion-service",
            is_ingestion=True,
        )
        runtime.assert_called_once()
        runtime_args = runtime.call_args.args
        self.assertIs(runtime_args[0], main.router.route_capturer)
        collector_settings = runtime_args[1]
        self.assertIs(
            collector_settings.bcfy_calls_authority_mode,
            worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
        )
        sid_allocation = worker_profiles.allocation_for_domain(
            collector_settings.worker_profile,
            main.grant_control.DomainId.SID,
        )
        self.assertIsNotNone(sid_allocation)
        assert sid_allocation is not None
        self.assertTrue(sid_allocation.claims_enabled)
        self.assertNotIn(
            feed_store.SourceType.BCFY_CALLS,
            collector_settings.feed_claim_caps,
        )
        runtime_instance.run.assert_called_once_with()

    def test_invalid_authority_mode_precedes_telemetry_and_runtime(
        self,
    ) -> None:
        env = {
            "WORKER_PROFILE": "mixed-dormant",
            "BCFY_CALLS_AUTHORITY_MODE": "",
        }

        with (
            mock.patch.dict("os.environ", env, clear=True),
            mock.patch.object(main.log_helper, "setup_logging") as logging,
            mock.patch.object(main.tracing_utils, "setup_tracing") as tracing,
            mock.patch.object(
                main.collector_runtime,
                "CollectorRuntime",
            ) as runtime,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "BCFY_CALLS_AUTHORITY_MODE",
            ):
                main.main()

        logging.assert_not_called()
        tracing.assert_not_called()
        runtime.assert_not_called()

    def test_incompatible_authority_profile_precedes_telemetry_and_runtime(
        self,
    ) -> None:
        env = {
            "WORKER_PROFILE": "legacy",
            "BCFY_CALLS_AUTHORITY_MODE": "sid_lease",
            "AUDIO_STAGING_BUCKET": "staging-bucket",
            "CONTINUOUS_PUBSUB_TOPIC_PATH": "projects/p/topics/continuous",
            "SEGMENTED_PUBSUB_TOPIC_PATH": "projects/p/topics/segmented",
            "ALLOYDB_HOST": "127.0.0.1",
            "ALLOYDB_USER": "radio_user",
            "ALLOYDB_DB": "radio_db",
        }

        with (
            mock.patch.dict("os.environ", env, clear=True),
            mock.patch.object(main.log_helper, "setup_logging") as logging,
            mock.patch.object(main.tracing_utils, "setup_tracing") as tracing,
            mock.patch.object(
                main.collector_runtime,
                "CollectorRuntime",
            ) as runtime,
        ):
            with self.assertRaisesRegex(ValueError, "requires.*SID domain"):
                main.main()

        logging.assert_not_called()
        tracing.assert_not_called()
        runtime.assert_not_called()

    def test_impossible_authority_precedes_telemetry_and_runtime(self) -> None:
        sid_profile = worker_profiles.derive_bcfy_calls_authority(
            worker_profiles.MIXED_DORMANT_PROFILE,
            worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
        )
        collector_settings = _settings_value(sid_profile)

        with (
            mock.patch.object(
                main.settings,
                "CollectorSettings",
                return_value=collector_settings,
            ) as settings_constructor,
            mock.patch.object(main.log_helper, "setup_logging") as logging,
            mock.patch.object(main.tracing_utils, "setup_tracing") as tracing,
            mock.patch.object(
                main.collector_runtime,
                "CollectorRuntime",
            ) as runtime,
        ):
            with self.assertRaisesRegex(RuntimeError, "Exactly one"):
                main.main()

        settings_constructor.assert_called_once_with()
        logging.assert_not_called()
        tracing.assert_not_called()
        runtime.assert_not_called()

    def test_sid_source_requires_segmented_topic_and_url(self) -> None:
        profile = worker_profiles.derive_bcfy_calls_authority(
            worker_profiles.SID_DORMANT_PROFILE,
            worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
        )
        collector_settings = _settings_value(
            profile,
            mode=worker_profiles.BcfyCallsAuthorityMode.SID_LEASE,
            feed_claim_caps={},
        )
        allocation = profile.allocations[0]

        collector_settings.segmented_pubsub_topic_path = None
        with self.assertRaisesRegex(RuntimeError, "segmented topic"):
            main._validate_sid_domain_configuration(
                allocation,
                collector_settings,
            )

        collector_settings.segmented_pubsub_topic_path = (
            "projects/p/topics/segmented"
        )
        with (
            mock.patch.dict(
                "os.environ",
                {"BCFY_CALLS_URL_BASE": ""},
                clear=True,
            ),
            self.assertRaisesRegex(RuntimeError, "URL base"),
        ):
            main._validate_sid_domain_configuration(
                allocation,
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

    def test_main_has_one_settings_epoch_and_no_separate_profile_load(
        self,
    ) -> None:
        source = inspect.getsource(main.main)

        self.assertEqual(source.count("settings.CollectorSettings("), 1)
        self.assertNotIn("load_worker_profile_from_env", source)


def _ty_diagnostic(
    *,
    path: str = "backend/example.py",
    check_name: str = "invalid-type",
    description: str = "invalid-type: reviewed diagnostic",
) -> dict[str, object]:
    return {
        "location": {"path": path},
        "check_name": check_name,
        "description": description,
    }


def _ty_baseline_document(*, count: int = 332) -> dict[str, object]:
    return {
        "baseline_commit": "55df18dd",
        "ty_version": "0.0.42",
        "diagnostic_count": 332,
        "diagnostics": [
            {
                "path": "backend/example.py",
                "check_name": "invalid-type",
                "description": "invalid-type: reviewed diagnostic",
                "count": count,
            }
        ],
    }


def _write_ty_baseline(
    tmp_path: pathlib.Path,
    document: object,
) -> pathlib.Path:
    path = tmp_path / "ty_baseline.json"
    if isinstance(document, str):
        path.write_text(document, encoding="utf-8")
    else:
        path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _stub_ty_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    *,
    diagnostics: object,
    version: str = "ty 0.0.42",
    check_returncode: int = 0,
) -> None:
    def run(
        command: tuple[str, ...],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        if command == ("uv", "run", "ty", "--version"):
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=f"{version}\n",
                stderr="",
            )
        if command == (
            "uv",
            "run",
            "ty",
            "check",
            "--output-format",
            "gitlab",
            "--exit-zero",
        ):
            stdout = (
                diagnostics
                if isinstance(diagnostics, str)
                else json.dumps(diagnostics)
            )
            return subprocess.CompletedProcess(
                command,
                check_returncode,
                stdout=stdout,
                stderr="ty failed" if check_returncode else "",
            )
        raise AssertionError(command)

    monkeypatch.setattr(ty_baseline_checker.subprocess, "run", run)


@pytest.mark.parametrize(
    "current_count",
    [332, 331],
    ids=["identical", "reduction"],
)
def test_ty_baseline_ratchet_accepts_reviewed_multisets(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    current_count: int,
) -> None:
    baseline_path = _write_ty_baseline(tmp_path, _ty_baseline_document())
    _stub_ty_subprocess(
        monkeypatch,
        diagnostics=[_ty_diagnostic()] * current_count,
    )

    assert ty_baseline_checker.check_ty_baseline(baseline_path) == (
        current_count,
        332,
    )


@pytest.mark.parametrize(
    "diagnostics",
    [
        [_ty_diagnostic()] * 333,
        [
            *([_ty_diagnostic()] * 332),
            _ty_diagnostic(
                path="backend/new.py",
                description="invalid-type: new diagnostic",
            ),
        ],
    ],
    ids=["increased-multiplicity", "new-diagnostic"],
)
def test_ty_baseline_ratchet_rejects_regressions(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    diagnostics: list[dict[str, object]],
) -> None:
    baseline_path = _write_ty_baseline(tmp_path, _ty_baseline_document())
    _stub_ty_subprocess(monkeypatch, diagnostics=diagnostics)

    with pytest.raises(
        ty_baseline_checker.BaselineCheckError,
        match="new or increased",
    ):
        ty_baseline_checker.check_ty_baseline(baseline_path)


@pytest.mark.parametrize(
    "document",
    [
        {
            key: value
            for key, value in _ty_baseline_document().items()
            if key != "baseline_commit"
        },
        "{malformed",
        _ty_baseline_document(count=331),
    ],
    ids=["missing-field", "malformed-json", "count-inconsistent"],
)
def test_ty_baseline_ratchet_rejects_invalid_baseline(
    tmp_path: pathlib.Path,
    document: object,
) -> None:
    baseline_path = _write_ty_baseline(tmp_path, document)

    with pytest.raises(ty_baseline_checker.BaselineCheckError):
        ty_baseline_checker.check_ty_baseline(baseline_path)


def test_ty_baseline_ratchet_rejects_version_mismatch(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline_path = _write_ty_baseline(tmp_path, _ty_baseline_document())
    _stub_ty_subprocess(
        monkeypatch,
        diagnostics=[],
        version="ty 0.0.43",
    )

    with pytest.raises(
        ty_baseline_checker.BaselineCheckError,
        match="version must be exactly",
    ):
        ty_baseline_checker.check_ty_baseline(baseline_path)


@pytest.mark.parametrize(
    "diagnostics",
    ["{malformed", {"diagnostics": []}],
    ids=["malformed-json", "non-list"],
)
def test_ty_baseline_ratchet_rejects_invalid_current_diagnostics(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    diagnostics: object,
) -> None:
    baseline_path = _write_ty_baseline(tmp_path, _ty_baseline_document())
    _stub_ty_subprocess(monkeypatch, diagnostics=diagnostics)

    with pytest.raises(ty_baseline_checker.BaselineCheckError):
        ty_baseline_checker.check_ty_baseline(baseline_path)


@pytest.mark.parametrize(
    "failure_mode",
    ["nonzero", "os-error"],
    ids=["nonzero-exit", "execution-error"],
)
def test_ty_baseline_ratchet_rejects_subprocess_failure(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    baseline_path = _write_ty_baseline(tmp_path, _ty_baseline_document())
    if failure_mode == "nonzero":
        _stub_ty_subprocess(
            monkeypatch,
            diagnostics=[],
            check_returncode=2,
        )
    else:

        def fail_run(*_args: object, **_kwargs: object) -> typing.NoReturn:
            raise OSError

        monkeypatch.setattr(
            ty_baseline_checker.subprocess,
            "run",
            fail_run,
        )

    with pytest.raises(ty_baseline_checker.BaselineCheckError):
        ty_baseline_checker.check_ty_baseline(baseline_path)


if __name__ == "__main__":
    unittest.main()
