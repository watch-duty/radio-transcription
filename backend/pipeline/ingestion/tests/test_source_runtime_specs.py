from __future__ import annotations

import unittest
from unittest import mock

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

    def test_feed_claimable_specs_exclude_non_feed_authorities(self) -> None:
        self.assertEqual(
            set(source_runtime_specs.feed_claimable_source_specs()),
            {
                feed_store.SourceType.BCFY_FEEDS,
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
                feed_store.SourceType.OPENMHZ: 900,
                feed_store.SourceType.FIRE_NOTIFICATIONS: 600,
            },
        )

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
        self.assertIs(
            source_runtime_specs.source_spec(
                feed_store.SourceType.BCFY_FEEDS
            ).topic_kind,
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
