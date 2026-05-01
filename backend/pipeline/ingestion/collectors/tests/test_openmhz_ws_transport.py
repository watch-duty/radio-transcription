from __future__ import annotations

import asyncio
import datetime
import json
import os
import unittest
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

if TYPE_CHECKING:
    from collections.abc import Sequence

from curl_cffi.curl import CurlError
from curl_cffi.requests.websockets import WebSocketClosed, WebSocketTimeout

from backend.pipeline.ingestion.collectors.openmhz._ws_transport import (
    _connect_with_fallback,
    _parse_eio_open,
    _parse_sio_event,
    websocket_transport,
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
        frame = f"42{json.dumps(['new message', inner_json])}"
        result = _parse_sio_event(frame)
        assert result is not None
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


# ---------------------------------------------------------------------------
# Transport context-manager tests
# ---------------------------------------------------------------------------


class _MockWebSocket:
    """Simulates a curl_cffi AsyncWebSocket returning scripted frames."""

    def __init__(self, frames: Sequence[str | None]) -> None:
        self._frames = iter(frames)
        self.sent: list[str] = []
        self._closed = False

    async def recv_str(self, **_kwargs: object) -> str:
        try:
            frame = next(self._frames)
        except StopIteration:
            raise WebSocketClosed("closed")  # noqa: EM101
        if frame is None:
            raise WebSocketTimeout("timeout")  # noqa: EM101
        return frame

    async def send_str(self, payload: str) -> None:
        self.sent.append(payload)

    async def close(self) -> None:
        self._closed = True


def _make_handshake_frames(
    *,
    sid: str = "test-sid",
    ping_interval: int = 25000,
    ping_timeout: int = 20000,
) -> list[str]:
    """Return the 2 frames for a successful EIO/SIO handshake."""
    open_pkt = json.dumps(
        {
            "sid": sid,
            "upgrades": [],
            "pingInterval": ping_interval,
            "pingTimeout": ping_timeout,
            "maxPayload": 1000000,
        }
    )
    return [f"0{open_pkt}", f'40{{"sid":"{sid}"}}']


def _make_call_frame(
    *,
    call_id: str = "abc123",
    talkgroup: int = 34480,
    length: int = 5,
    short_name: str = "wmata",
) -> str:
    call_dict = {
        "_id": call_id,
        "talkgroupNum": talkgroup,
        "url": f"https://media2.openmhz.com/media/{short_name}/{talkgroup}/{short_name}-{talkgroup}-123.m4a",
        "time": "2026-04-02T22:57:20.000Z",
        "len": length,
        "freq": 490962500,
        "srcList": [],
        "shortName": short_name,
        "emergency": False,
    }
    return f"42{json.dumps(['new message', json.dumps(call_dict)])}"


_WS_MOD = "backend.pipeline.ingestion.collectors.openmhz._ws_transport"


class TestWebsocketTransport(unittest.IsolatedAsyncioTestCase):
    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_yields_call_events(
        self, mock_session_cls: MagicMock
    ) -> None:
        frames = [
            *_make_handshake_frames(),
            _make_call_frame(call_id="call1"),
            _make_call_frame(call_id="call2"),
        ]
        mock_ws = _MockWebSocket(frames)
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        shutdown = asyncio.Event()
        calls = []
        async with websocket_transport(
            "wmata", "https://api.openmhz.com/", shutdown
        ) as events:
            async for call in events:
                calls.append(call)
                if len(calls) == 2:
                    shutdown.set()

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].id, "call1")
        self.assertEqual(calls[1].id, "call2")

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_sends_start_event_with_short_name(
        self, mock_session_cls: MagicMock
    ) -> None:
        frames = [*_make_handshake_frames()]
        mock_ws = _MockWebSocket(frames)
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        shutdown = asyncio.Event()
        shutdown.set()
        async with websocket_transport(
            "mySystem", "https://api.openmhz.com/", shutdown
        ) as events:
            async for _ in events:
                pass

        self.assertEqual(mock_ws.sent[0], "40")
        start_frame = mock_ws.sent[1]
        self.assertTrue(start_frame.startswith("42"))
        payload = json.loads(start_frame[2:])
        self.assertEqual(payload[0], "start")
        self.assertEqual(payload[1]["shortName"], "mySystem")

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_responds_pong_to_ping(
        self, mock_session_cls: MagicMock
    ) -> None:
        frames = [
            *_make_handshake_frames(),
            "2",
            _make_call_frame(call_id="after-ping"),
        ]
        mock_ws = _MockWebSocket(frames)
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        shutdown = asyncio.Event()
        calls = []
        async with websocket_transport(
            "wmata", "https://api.openmhz.com/", shutdown
        ) as events:
            async for call in events:
                calls.append(call)
                shutdown.set()

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].id, "after-ping")
        self.assertIn("3", mock_ws.sent)

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_exits_on_server_disconnect_41(
        self, mock_session_cls: MagicMock
    ) -> None:
        frames = [
            *_make_handshake_frames(),
            _make_call_frame(call_id="before-dc"),
            "41",
            _make_call_frame(call_id="never-seen"),
        ]
        mock_ws = _MockWebSocket(frames)
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        shutdown = asyncio.Event()
        calls = []
        async with websocket_transport(
            "wmata", "https://api.openmhz.com/", shutdown
        ) as events:
            async for call in events:
                calls.append(call)

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].id, "before-dc")

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_exits_on_connect_error_44(
        self, mock_session_cls: MagicMock
    ) -> None:
        frames = [
            *_make_handshake_frames(),
            '44{"message":"server error"}',
        ]
        mock_ws = _MockWebSocket(frames)
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        shutdown = asyncio.Event()
        calls = []
        async with websocket_transport(
            "wmata", "https://api.openmhz.com/", shutdown
        ) as events:
            async for call in events:
                calls.append(call)

        self.assertEqual(len(calls), 0)

    @patch(f"{_WS_MOD}.AsyncSession")
    @patch(f"{_WS_MOD}.time")
    async def test_exits_on_ping_timeout(
        self, mock_time: MagicMock, mock_session_cls: MagicMock
    ) -> None:
        frames = [
            *_make_handshake_frames(ping_interval=1000, ping_timeout=1000),
            None,  # recv timeout
        ]
        mock_ws = _MockWebSocket(frames)
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        # 4 calls: init, 1st check, 2nd check (triggers), log message
        mock_time.monotonic = MagicMock(side_effect=[0.0, 0.0, 3.0, 3.0])

        shutdown = asyncio.Event()
        calls = []
        async with websocket_transport(
            "wmata", "https://api.openmhz.com/", shutdown
        ) as events:
            async for call in events:
                calls.append(call)

        self.assertEqual(len(calls), 0)

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_connects_with_wss_scheme(
        self, mock_session_cls: MagicMock
    ) -> None:
        """Verify https:// base_url is converted to wss:// for ws_connect.

        libcurl requires the wss:// scheme to perform the WebSocket upgrade.
        With https://, it opens a plain TLS connection and curl_ws_recv()
        fails with error 43.
        """
        frames = [*_make_handshake_frames()]
        mock_ws = _MockWebSocket(frames)
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        shutdown = asyncio.Event()
        shutdown.set()
        async with websocket_transport(
            "wmata", "https://api.openmhz.com/", shutdown
        ) as events:
            async for _ in events:
                pass

        ws_url = mock_session.ws_connect.call_args[0][0]
        self.assertTrue(
            ws_url.startswith("wss://"),
            f"Expected wss:// scheme, got: {ws_url}",
        )
        self.assertIn("socket.io/?EIO=4&transport=websocket", ws_url)

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_connects_with_ws_scheme_for_http(
        self, mock_session_cls: MagicMock
    ) -> None:
        """Verify http:// base_url is converted to ws:// (for local testing)."""
        frames = [*_make_handshake_frames()]
        mock_ws = _MockWebSocket(frames)
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        shutdown = asyncio.Event()
        shutdown.set()
        async with websocket_transport(
            "wmata", "http://localhost:8080/", shutdown
        ) as events:
            async for _ in events:
                pass

        ws_url = mock_session.ws_connect.call_args[0][0]
        self.assertTrue(
            ws_url.startswith("ws://"),
            f"Expected ws:// scheme, got: {ws_url}",
        )

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_closes_session_on_exit(
        self, mock_session_cls: MagicMock
    ) -> None:
        frames = [*_make_handshake_frames()]
        mock_ws = _MockWebSocket(frames)
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        shutdown = asyncio.Event()
        shutdown.set()
        async with websocket_transport(
            "wmata", "https://api.openmhz.com/", shutdown
        ) as events:
            async for _ in events:
                pass

        self.assertTrue(mock_ws._closed)
        mock_session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# Multi-profile fallback tests
# ---------------------------------------------------------------------------

_BLOCKED_MSG = (
    "Failed to perform, curl: (22) Refused WebSockets upgrade: 403. "
    "See https://curl.se/libcurl/c/libcurl-errors.html first for more details."
)


def _make_blocked_session() -> AsyncMock:
    """An AsyncSession whose ws_connect raises the Cloudflare-403 CurlError."""
    s = AsyncMock()
    s.ws_connect = AsyncMock(side_effect=CurlError(_BLOCKED_MSG))
    s.close = AsyncMock()
    return s


def _make_timeout_session() -> AsyncMock:
    """An AsyncSession whose ws_connect raises asyncio.TimeoutError."""
    s = AsyncMock()
    s.ws_connect = AsyncMock(side_effect=asyncio.TimeoutError())
    s.close = AsyncMock()
    return s


def _make_working_session() -> tuple[AsyncMock, _MockWebSocket]:
    ws = _MockWebSocket([])
    s = AsyncMock()
    s.ws_connect = AsyncMock(return_value=ws)
    s.close = AsyncMock()
    return s, ws


class TestConnectWithFallback(unittest.IsolatedAsyncioTestCase):
    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_uses_first_profile_on_success(
        self, mock_session_cls: MagicMock
    ) -> None:
        working, ws = _make_working_session()
        mock_session_cls.return_value = working

        session, returned_ws, profile = await _connect_with_fallback(
            "wss://api.openmhz.com/socket.io/", "wmata"
        )

        self.assertEqual(profile, "firefox133")
        self.assertIs(session, working)
        self.assertIs(returned_ws, ws)
        self.assertEqual(mock_session_cls.call_count, 1)
        self.assertEqual(
            mock_session_cls.call_args.kwargs, {"impersonate": "firefox133"}
        )

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_falls_back_on_403_signature(
        self, mock_session_cls: MagicMock
    ) -> None:
        blocked = _make_blocked_session()
        working, _ = _make_working_session()
        mock_session_cls.side_effect = [blocked, working]

        _, _, profile = await _connect_with_fallback(
            "wss://api.openmhz.com/socket.io/", "wmata"
        )

        self.assertEqual(profile, "safari17_2_ios")
        self.assertEqual(mock_session_cls.call_count, 2)
        self.assertEqual(
            mock_session_cls.call_args_list[0].kwargs,
            {"impersonate": "firefox133"},
        )
        self.assertEqual(
            mock_session_cls.call_args_list[1].kwargs,
            {"impersonate": "safari17_2_ios"},
        )
        blocked.close.assert_awaited_once()

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_falls_back_on_timeout(
        self, mock_session_cls: MagicMock
    ) -> None:
        timed_out = _make_timeout_session()
        working, _ = _make_working_session()
        mock_session_cls.side_effect = [timed_out, working]

        _, _, profile = await _connect_with_fallback(
            "wss://api.openmhz.com/socket.io/", "wmata"
        )

        self.assertEqual(profile, "safari17_2_ios")
        self.assertEqual(mock_session_cls.call_count, 2)
        timed_out.close.assert_awaited_once()

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_does_not_fall_back_on_non_403_curl_error(
        self, mock_session_cls: MagicMock
    ) -> None:
        non_403 = AsyncMock()
        non_403.ws_connect = AsyncMock(
            side_effect=CurlError(
                "Failed to perform, curl: (35) TLS handshake error"
            )
        )
        non_403.close = AsyncMock()
        mock_session_cls.return_value = non_403

        with self.assertRaises(CurlError):
            await _connect_with_fallback(
                "wss://api.openmhz.com/socket.io/", "wmata"
            )

        self.assertEqual(mock_session_cls.call_count, 1)
        non_403.close.assert_awaited_once()

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_does_not_fall_back_on_arbitrary_exception(
        self, mock_session_cls: MagicMock
    ) -> None:
        blowup = AsyncMock()
        blowup.ws_connect = AsyncMock(side_effect=OSError("network down"))
        blowup.close = AsyncMock()
        mock_session_cls.return_value = blowup

        with self.assertRaises(OSError):
            await _connect_with_fallback(
                "wss://api.openmhz.com/socket.io/", "wmata"
            )

        self.assertEqual(mock_session_cls.call_count, 1)
        blowup.close.assert_awaited_once()

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_falls_back_to_third_profile(
        self, mock_session_cls: MagicMock
    ) -> None:
        b1 = _make_blocked_session()
        b2 = _make_blocked_session()
        working, _ = _make_working_session()
        mock_session_cls.side_effect = [b1, b2, working]

        _, _, profile = await _connect_with_fallback(
            "wss://api.openmhz.com/socket.io/", "wmata"
        )

        self.assertEqual(profile, "safari15_5")
        self.assertEqual(mock_session_cls.call_count, 3)

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_raises_runtime_error_when_all_profiles_blocked(
        self, mock_session_cls: MagicMock
    ) -> None:
        b1 = _make_blocked_session()
        b2 = _make_blocked_session()
        b3 = _make_blocked_session()
        mock_session_cls.side_effect = [b1, b2, b3]

        with self.assertRaises(RuntimeError) as cm:
            await _connect_with_fallback(
                "wss://api.openmhz.com/socket.io/", "wmata"
            )

        self.assertIn("3 impersonation profiles failed", str(cm.exception))
        self.assertIsInstance(cm.exception.__cause__, CurlError)

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_default_profile_list_when_env_unset(
        self, mock_session_cls: MagicMock
    ) -> None:
        sessions = [
            _make_blocked_session(),
            _make_blocked_session(),
            _make_blocked_session(),
        ]
        mock_session_cls.side_effect = sessions

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OPENMHZ_IMPERSONATE_PROFILES", None)
            with self.assertRaises(RuntimeError):
                await _connect_with_fallback(
                    "wss://api.openmhz.com/socket.io/", "wmata"
                )

        attempted = [
            c.kwargs["impersonate"] for c in mock_session_cls.call_args_list
        ]
        self.assertEqual(
            attempted, ["firefox133", "safari17_2_ios", "safari15_5"]
        )

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_custom_profile_list_via_env(
        self, mock_session_cls: MagicMock
    ) -> None:
        s1 = _make_blocked_session()
        working, _ = _make_working_session()
        mock_session_cls.side_effect = [s1, working]

        with patch.dict(
            os.environ, {"OPENMHZ_IMPERSONATE_PROFILES": "foo, bar"}
        ):
            _, _, profile = await _connect_with_fallback(
                "wss://api.openmhz.com/socket.io/", "wmata"
            )

        self.assertEqual(profile, "bar")
        attempted = [
            c.kwargs["impersonate"] for c in mock_session_cls.call_args_list
        ]
        self.assertEqual(attempted, ["foo", "bar"])

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_single_profile_via_env_disables_fallback(
        self, mock_session_cls: MagicMock
    ) -> None:
        blocked = _make_blocked_session()
        mock_session_cls.return_value = blocked

        with patch.dict(
            os.environ, {"OPENMHZ_IMPERSONATE_PROFILES": "firefox133"}
        ), self.assertRaises(RuntimeError):
            await _connect_with_fallback(
                "wss://api.openmhz.com/socket.io/", "wmata"
            )

        self.assertEqual(mock_session_cls.call_count, 1)

    async def test_empty_profile_list_raises_at_construction(self) -> None:
        with patch.dict(
            os.environ, {"OPENMHZ_IMPERSONATE_PROFILES": ""}
        ), self.assertRaises(ValueError):
            await _connect_with_fallback(
                "wss://api.openmhz.com/socket.io/", "wmata"
            )

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_failed_profile_session_is_closed(
        self, mock_session_cls: MagicMock
    ) -> None:
        b1 = _make_blocked_session()
        b2 = _make_blocked_session()
        working, _ = _make_working_session()
        mock_session_cls.side_effect = [b1, b2, working]

        await _connect_with_fallback(
            "wss://api.openmhz.com/socket.io/", "wmata"
        )

        b1.close.assert_awaited_once()
        b2.close.assert_awaited_once()
        # The chosen working session should NOT be closed by the helper —
        # caller (websocket_transport's finally block) is responsible for that.
        working.close.assert_not_awaited()


class TestWebsocketTransportLogging(unittest.IsolatedAsyncioTestCase):
    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_logs_chosen_profile_on_success(
        self, mock_session_cls: MagicMock
    ) -> None:
        frames = [*_make_handshake_frames()]
        mock_ws = _MockWebSocket(frames)
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        shutdown = asyncio.Event()
        shutdown.set()
        with self.assertLogs(_WS_MOD, level="INFO") as captured:
            async with websocket_transport(
                "wmata", "https://api.openmhz.com/", shutdown
            ) as events:
                async for _ in events:
                    pass

        connected_lines = [
            r.getMessage()
            for r in captured.records
            if "WebSocket connected" in r.getMessage()
        ]
        self.assertEqual(len(connected_lines), 1)
        self.assertIn("impersonate=firefox133", connected_lines[0])
        self.assertIn("short_name=wmata", connected_lines[0])
