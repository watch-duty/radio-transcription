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

    def test_claimable_specs_match_registered_collectors(self) -> None:
        self.assertEqual(
            set(source_runtime_specs.claimable_source_specs()),
            set(_COLLECTORS),
        )

    def test_default_caps_include_only_claimable_sources(self) -> None:
        caps = source_runtime_specs.default_caps()

        self.assertEqual(caps[feed_store.SourceType.BCFY_FEEDS], 240)
        self.assertEqual(caps[feed_store.SourceType.BCFY_CALLS], 600)
        self.assertEqual(caps[feed_store.SourceType.OPENMHZ], 900)
        self.assertEqual(caps[feed_store.SourceType.FIRE_NOTIFICATIONS], 300)
        self.assertNotIn(feed_store.SourceType.ECHO, caps)

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
