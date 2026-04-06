from __future__ import annotations

import datetime
import json
import unittest

from backend.pipeline.ingestion.collectors.openmhz._ws_transport import (
    _parse_eio_open,
    _parse_sio_event,
)


class TestParseEioOpen(unittest.TestCase):
    def test_parses_valid_open_packet(self) -> None:
        frame = '0{"sid":"QQuLyyFysZRX12FrwZX8","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}'
        result = _parse_eio_open(frame)
        self.assertEqual(result["sid"], "QQuLyyFysZRX12FrwZX8")
        self.assertEqual(result["pingInterval"], 25000)
        self.assertEqual(result["pingTimeout"], 20000)

    def test_rejects_non_zero_prefix(self) -> None:
        with self.assertRaises(ValueError, msg="Expected EIO open"):
            _parse_eio_open('4{"sid":"x"}')


class TestParseSioEvent(unittest.TestCase):
    CALL_DICT = {
        "_id": "69cef458302a9885edbce107",
        "talkgroupNum": 32816,
        "url": "https://media2.openmhz.com/media/wmata/32816/wmata-32816-1775170640.m4a",
        "time": "2026-04-02T22:57:20.000Z",
        "len": 4,
        "freq": 490962500,
        "srcList": [{"pos": 0, "src": "65520"}],
        "shortName": "wmata",
        "emergency": False,
    }

    def test_parses_new_message_event(self) -> None:
        inner_json = json.dumps(self.CALL_DICT)
        frame = f'42{json.dumps(["new message", inner_json])}'
        result = _parse_sio_event(frame)
        self.assertIsNotNone(result)
        self.assertEqual(result.id, "69cef458302a9885edbce107")
        self.assertEqual(result.talkgroup_num, 32816)
        self.assertEqual(result.length_sec, 4)
        self.assertEqual(result.short_name, "wmata")
        self.assertFalse(result.emergency)
        self.assertEqual(
            result.time,
            datetime.datetime(2026, 4, 2, 22, 57, 20, tzinfo=datetime.UTC),
        )

    def test_returns_none_for_non_42_frame(self) -> None:
        self.assertIsNone(_parse_sio_event("2"))
        self.assertIsNone(_parse_sio_event('40{"sid":"x"}'))

    def test_returns_none_for_unknown_event_name(self) -> None:
        frame = "42" + json.dumps(["other event", "{}"])
        self.assertIsNone(_parse_sio_event(frame))
