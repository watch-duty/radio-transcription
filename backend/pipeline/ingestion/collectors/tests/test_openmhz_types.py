from __future__ import annotations

import datetime
import unittest

from backend.pipeline.ingestion.collectors.openmhz._types import (
    CallEvent,
    TransportFactory,
)


class TestCallEvent(unittest.TestCase):
    def test_constructs_from_fields(self) -> None:
        event = CallEvent(
            id="69cef3a1302a9885edbabbca",
            talkgroup_num=34480,
            url="https://media2.openmhz.com/media/wmata/34480/wmata-34480-1775170421.m4a",
            time=datetime.datetime(2026, 4, 2, 22, 53, 41, tzinfo=datetime.UTC),
            length_sec=31,
            freq=490762500,
            src_list=[{"pos": 0, "src": "36720"}],
            short_name="wmata",
            emergency=False,
        )
        self.assertEqual(event.id, "69cef3a1302a9885edbabbca")
        self.assertEqual(event.talkgroup_num, 34480)
        self.assertEqual(event.length_sec, 31)
        self.assertEqual(event.short_name, "wmata")

    def test_is_frozen(self) -> None:
        event = CallEvent(
            id="abc",
            talkgroup_num=1,
            url="https://example.com/a.m4a",
            time=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            length_sec=5,
            freq=100000,
            src_list=[],
            short_name="test",
            emergency=False,
        )
        with self.assertRaises(AttributeError):
            event.id = "xyz"  # type: ignore[misc]

    def test_transport_factory_type_is_importable(self) -> None:
        self.assertTrue(TransportFactory is not None)
