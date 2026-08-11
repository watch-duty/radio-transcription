from __future__ import annotations

import unittest
from unittest import mock

from backend.pipeline.common.constants import CONTINUOUS_SOURCE_TYPES
from backend.pipeline.ingestion import main as ingestion_main
from backend.pipeline.ingestion import source_runtime_specs
from backend.pipeline.ingestion.router import _COLLECTORS
from backend.pipeline.storage import feed_store


class TestSourceRuntimeSpecs(unittest.TestCase):
    """Tests for source-type runtime metadata."""

    def test_every_source_type_has_spec(self) -> None:
        self.assertEqual(
            set(source_runtime_specs.SOURCE_RUNTIME_SPECS),
            set(feed_store.SourceType),
        )

    def test_registered_collectors_have_runtime_specs(self) -> None:
        self.assertLessEqual(
            set(_COLLECTORS),
            set(source_runtime_specs.SOURCE_RUNTIME_SPECS),
        )

    def test_startup_rejects_collector_outside_current_authorities(
        self,
    ) -> None:
        settings = mock.Mock(
            feed_claim_caps={
                feed_store.SourceType.BCFY_FEEDS: 240,
                feed_store.SourceType.GENERIC_ICECAST: 240,
                feed_store.SourceType.OPENMHZ: 900,
                feed_store.SourceType.FIRE_NOTIFICATIONS: 600,
            }
        )
        registered_collectors = [
            feed_store.SourceType.BCFY_FEEDS.value,
            feed_store.SourceType.BCFY_CALLS.value,
            feed_store.SourceType.OPENMHZ.value,
            feed_store.SourceType.FIRE_NOTIFICATIONS.value,
            feed_store.SourceType.ECHO.value,
        ]

        with (
            mock.patch.object(ingestion_main, "setup_logging"),
            mock.patch.object(ingestion_main, "setup_tracing"),
            mock.patch.object(
                ingestion_main,
                "CollectorSettings",
                return_value=settings,
            ),
            mock.patch.object(
                ingestion_main,
                "supported_source_types",
                return_value=registered_collectors,
            ),
            mock.patch.object(ingestion_main, "resolve_topic_path"),
            mock.patch.object(ingestion_main, "CollectorRuntime"),
            self.assertRaisesRegex(RuntimeError, "collector registry"),
        ):
            ingestion_main.main()

    def test_feed_claimable_specs_exclude_non_feed_authorities(self) -> None:
        self.assertEqual(
            set(source_runtime_specs.feed_claimable_source_specs()),
            {
                feed_store.SourceType.BCFY_FEEDS,
                feed_store.SourceType.GENERIC_ICECAST,
                feed_store.SourceType.OPENMHZ,
                feed_store.SourceType.FIRE_NOTIFICATIONS,
            },
        )

    def test_default_feed_claim_caps_match_feed_claimable_specs(self) -> None:
        caps = source_runtime_specs.default_feed_claim_caps()

        self.assertEqual(
            caps,
            {
                feed_store.SourceType.BCFY_FEEDS: 240,
                feed_store.SourceType.GENERIC_ICECAST: 240,
                feed_store.SourceType.OPENMHZ: 900,
                feed_store.SourceType.FIRE_NOTIFICATIONS: 600,
            },
        )

    def test_continuous_source_types_constant_tracks_the_specs(self) -> None:
        """CONTINUOUS_SOURCE_TYPES must equal the CONTINUOUS spec set.

        Dataflow can't import this registry, so it compares against that
        constant instead. Drift silently routes continuous audio as segmented.
        """
        continuous_specs = {
            source_type.value
            for source_type, spec in (
                source_runtime_specs.SOURCE_RUNTIME_SPECS.items()
            )
            if spec.topic_kind is source_runtime_specs.TopicKind.CONTINUOUS
        }

        self.assertEqual(set(CONTINUOUS_SOURCE_TYPES), continuous_specs)

    def test_url_base_uses_env_override(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"BCFY_CALLS_URL_BASE": "https://example.invalid/live/"},
        ):
            self.assertEqual(
                source_runtime_specs.url_base_for(
                    feed_store.SourceType.BCFY_CALLS
                ),
                "https://example.invalid/live/",
            )

    def test_topic_kinds_are_registered(self) -> None:
        for source_type in (
            feed_store.SourceType.BCFY_FEEDS,
            feed_store.SourceType.GENERIC_ICECAST,
        ):
            with self.subTest(source_type=source_type.value):
                self.assertIs(
                    source_runtime_specs.source_spec(source_type).topic_kind,
                    source_runtime_specs.TopicKind.CONTINUOUS,
                )
        for source_type in (
            feed_store.SourceType.BCFY_CALLS,
            feed_store.SourceType.ECHO,
            feed_store.SourceType.OPENMHZ,
            feed_store.SourceType.FIRE_NOTIFICATIONS,
        ):
            with self.subTest(source_type=source_type.value):
                self.assertIs(
                    source_runtime_specs.source_spec(source_type).topic_kind,
                    source_runtime_specs.TopicKind.SEGMENTED,
                )


if __name__ == "__main__":
    unittest.main()
