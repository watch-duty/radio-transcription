from __future__ import annotations

import asyncio
import contextlib
import datetime
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, NoReturn

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.websockets import WebSocketClosed, WebSocketTimeout

from backend.pipeline.ingestion.collectors.openmhz._types import CallEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable

logger = logging.getLogger(__name__)


class OpenMHzTransportError(Exception):
    """Expected OpenMHz websocket connection or protocol failure."""


def _raise_cancelled() -> NoReturn:
    raise asyncio.CancelledError


def _raise_protocol_error(message: str) -> NoReturn:
    raise ValueError(message)


_START_PAYLOAD_TEMPLATE: dict[str, object] = {
    "filterCode": "",
    "filterType": "all",
    "filterName": "OpenMHz",
    "filterStarred": False,
}


def _parse_eio_open(frame: str) -> dict[str, Any]:
    """Parse Engine.IO v4 open packet ``0{...}``."""
    if not frame.startswith("0"):
        msg = f"Expected EIO open packet (0{{...}}), got: {frame[:60]}"
        raise ValueError(msg)
    return json.loads(frame[1:])


def _parse_sio_event(frame: str) -> CallEvent | None:
    """Parse Socket.IO v4 event ``42["new message","<json>"]``.

    Returns ``None`` for non-event frames or unknown event names.
    Double-parses: outer JSON array, then inner JSON string.
    """
    if not frame.startswith("42"):
        return None
    array = json.loads(frame[2:])
    if not isinstance(array, list) or len(array) < 2:
        return None
    if array[0] != "new message":
        return None
    call: dict[str, Any] = json.loads(array[1])
    return CallEvent(
        id=call["_id"],
        talkgroup_num=call["talkgroupNum"],
        url=call["url"],
        time=datetime.datetime.fromisoformat(call["time"]),
        length_sec=call["len"],
        freq=call["freq"],
        src_list=call.get("srcList", []),
        short_name=call.get("shortName", ""),
        emergency=call.get("emergency", False),
    )


@asynccontextmanager
async def websocket_transport(
    short_name: str,
    base_url: str,
    shutdown: asyncio.Event,
) -> AsyncIterator[AsyncIterator[CallEvent]]:
    """Single-session WebSocket transport for OpenMHZ.

    Connects via curl_cffi with browser TLS impersonation, performs
    the EIO/SIO v4 handshake, subscribes to *short_name*, and yields
    an async iterator of :class:`CallEvent` objects.

    Does **not** reconnect internally -- that is the collector's job.
    """
    normalized = base_url if base_url.endswith("/") else f"{base_url}/"
    ws_url = f"{normalized}socket.io/?EIO=4&transport=websocket"
    # libcurl requires wss:// (not https://) to perform the WebSocket
    # upgrade handshake.  With https:// it opens a plain TLS connection
    # and curl_ws_recv() fails with error 43.
    ws_url = ws_url.replace("https://", "wss://", 1).replace(
        "http://", "ws://", 1
    )

    session = AsyncSession(impersonate="chrome")
    ws = None
    try:
        ws = await _await_or_shutdown(session.ws_connect(ws_url), shutdown)

        # --- EIO handshake ---
        open_frame = await _recv_str_or_shutdown(ws, shutdown, 10.0)
        if open_frame is None:
            _raise_cancelled()
        open_data = _parse_eio_open(open_frame)
        sid = open_data["sid"]
        ping_interval_sec: float = open_data["pingInterval"] / 1000
        ping_timeout_sec: float = open_data["pingTimeout"] / 1000

        logger.info(
            "WebSocket connected: short_name=%s sid=%s",
            short_name,
            sid,
        )

        # --- SIO connect ---
        await _await_or_shutdown(ws.send_str("40"), shutdown)
        ack = await _recv_str_or_shutdown(ws, shutdown, 10.0)
        if ack is None:
            _raise_cancelled()
        if not ack.startswith("40"):
            msg = f"Expected SIO connect ack (40...), got: {ack[:60]}"
            _raise_protocol_error(msg)

        # --- Subscribe ---
        start_payload = json.dumps(
            ["start", {**_START_PAYLOAD_TEMPLATE, "shortName": short_name}]
        )
        await _await_or_shutdown(ws.send_str(f"42{start_payload}"), shutdown)
        logger.info("Subscribed to system: short_name=%s", short_name)
    except asyncio.CancelledError:
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()
        await session.close()
        raise
    except Exception as e:
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()
        await session.close()
        msg = "openmhz_transport_error"
        raise OpenMHzTransportError(msg) from e

    try:
        yield _stream_frames(
            ws, shutdown, short_name, ping_interval_sec, ping_timeout_sec
        )
    finally:
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.send_str("1")  # best-effort EIO close
            with contextlib.suppress(Exception):
                await ws.close()
        await session.close()


async def _stream_frames(  # noqa: PLR0912
    ws: Any,
    shutdown: asyncio.Event,
    short_name: str,
    ping_interval_sec: float,
    ping_timeout_sec: float,
) -> AsyncIterator[CallEvent]:
    """Yield :class:`CallEvent` objects from the WebSocket frame stream."""
    deadline_sec = ping_interval_sec + ping_timeout_sec
    last_ping_time = time.monotonic()

    while True:
        if shutdown.is_set():
            logger.info("Shutdown requested: short_name=%s", short_name)
            return

        if time.monotonic() - last_ping_time > deadline_sec:
            logger.warning(
                "Ping timeout: short_name=%s elapsed_sec=%.1f",
                short_name,
                time.monotonic() - last_ping_time,
            )
            return

        try:
            frame = await _recv_str_or_shutdown(ws, shutdown, ping_interval_sec)
            if frame is None:
                logger.info("Shutdown requested: short_name=%s", short_name)
                return
            if frame.startswith("2"):
                await ws.send_str("3")
                last_ping_time = time.monotonic()
            elif frame.startswith("42"):
                call = _parse_sio_event(frame)
                if call is not None:
                    logger.debug(
                        "Call received: short_name=%s call_id=%s talkgroup=%d",
                        short_name,
                        call.id,
                        call.talkgroup_num,
                    )
                    yield call
            elif frame.startswith("41"):
                logger.warning(
                    "Server disconnect (41): short_name=%s", short_name
                )
                return
            elif frame.startswith("44"):
                logger.warning(
                    "Connect error (44): short_name=%s message=%s",
                    short_name,
                    frame[2:],
                )
                return
            else:
                logger.debug("Ignoring frame: %s", frame[:50])
        except WebSocketTimeout:
            continue
        except WebSocketClosed:
            logger.warning("WebSocket closed: short_name=%s", short_name)
            return
        except Exception as e:
            msg = "openmhz_transport_error"
            raise OpenMHzTransportError(msg) from e


async def _recv_str_or_shutdown(
    ws: Any,
    shutdown: asyncio.Event,
    timeout_sec: float,
) -> str | None:
    """Receive one websocket frame, returning None if shutdown wins."""
    if shutdown.is_set():
        return None

    recv_task = asyncio.create_task(ws.recv_str(timeout=timeout_sec))
    shutdown_task = asyncio.create_task(shutdown.wait())
    tasks = {recv_task, shutdown_task}

    try:
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    if shutdown_task in done and shutdown_task.result():
        await asyncio.gather(recv_task, return_exceptions=True)
        return None

    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.gather(shutdown_task, return_exceptions=True)
    return await recv_task


async def _await_or_shutdown[T](
    awaitable: Awaitable[T],
    shutdown: asyncio.Event,
) -> T:
    """Await one setup operation, cancelling it if shutdown wins."""
    operation_task = asyncio.ensure_future(awaitable)
    if shutdown.is_set():
        operation_task.cancel()
        await asyncio.gather(operation_task, return_exceptions=True)
        raise asyncio.CancelledError

    shutdown_task = asyncio.create_task(shutdown.wait())
    tasks = {operation_task, shutdown_task}

    try:
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    if shutdown_task in done and shutdown_task.result():
        await asyncio.gather(operation_task, return_exceptions=True)
        raise asyncio.CancelledError

    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.gather(shutdown_task, return_exceptions=True)
    return await operation_task
