from __future__ import annotations

import asyncio
import datetime
import json
import unittest
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

if TYPE_CHECKING:
    from collections.abc import Sequence

from curl_cffi.requests.websockets import WebSocketClosed, WebSocketTimeout

from backend.pipeline.ingestion.collectors.openmhz._ws_transport import (
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
        with self.assertRaises(ValueError) as ctx:
            _parse_eio_open('4{"sid":"x"}')

        self.assertIn("Expected EIO open", str(ctx.exception))


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

    @patch(f"{_WS_MOD}.AsyncSession")
    async def test_pending_receive_exits_on_shutdown(
        self, mock_session_cls: MagicMock
    ) -> None:
        started = asyncio.Event()
        cancelled = asyncio.Event()

        class _BlockingAfterHandshakeWebSocket:
            def __init__(self) -> None:
                self.sent: list[str] = []
                self._closed = False
                self._recv_count = 0

            async def recv_str(self, **_kwargs: object) -> str:
                self._recv_count += 1
                if self._recv_count == 1:
                    return _make_handshake_frames()[0]
                if self._recv_count == 2:
                    return _make_handshake_frames()[1]

                started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise
                msg = "unreachable"
                raise AssertionError(msg)

            async def send_str(self, payload: str) -> None:
                self.sent.append(payload)

            async def close(self) -> None:
                self._closed = True

        mock_ws = _BlockingAfterHandshakeWebSocket()
        mock_session = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session_cls.return_value = mock_session

        shutdown = asyncio.Event()

        async def _consume() -> None:
            async with websocket_transport(
                "wmata", "https://api.openmhz.com/", shutdown
            ) as events:
                async for _ in events:
                    pass

        task = asyncio.create_task(_consume())
        await asyncio.wait_for(started.wait(), timeout=1.0)
        shutdown.set()

        await asyncio.wait_for(task, timeout=1.0)
        await asyncio.wait_for(cancelled.wait(), timeout=1.0)
        self.assertTrue(mock_ws._closed)
        mock_session.close.assert_awaited_once()
