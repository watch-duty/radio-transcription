from __future__ import annotations

import asyncio
import contextlib
import datetime
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from curl_cffi.curl import CurlError
from curl_cffi.requests import AsyncSession
from curl_cffi.requests.websockets import WebSocketClosed, WebSocketTimeout

from backend.pipeline.ingestion.collectors.openmhz._types import CallEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)

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


# Default profile fallback list. Confirmed working against api.openmhz.com on
# 2026-05-01 with curl_cffi 0.15.0. The 2026-04-29 Cloudflare rule update
# 403'd every chrome/edge desktop fingerprint plus safari17.0+/18+/26+ (macOS
# and iOS); the firefox family (133, 135, 144, 147) and older safari variants
# all still pass.
#
# Default order:
#   firefox147        — latest firefox; auto-replaced if/when newer pins exist
#   firefox133        — pinned older firefox, hedge against a future Cloudflare
#                       rule targeting just the latest firefox version
#   safari17_2_ios    — cross-vendor diversity (different fingerprint family),
#                       newer than the safari15.x line so less likely to be
#                       deprecated by a curl_cffi version prune
_DEFAULT_IMPERSONATE_PROFILES = "firefox147,firefox133,safari17_2_ios"
_PROFILE_CONNECT_TIMEOUT_SEC = 10.0
# libcurl's CURLE_HTTP_RETURNED_ERROR (22) — fires when the WS upgrade
# response is non-101 (i.e., the server rejected the upgrade with some HTTP
# error). curl_cffi 0.15.0 surfaces the libcurl error code on
# `CurlError.code`, which is a stable C API and far more robust than parsing
# the formatted exception message. The HTTP status (403) itself is NOT
# exposed structurally on CurlError in 0.15.0 (verified via probe), so we
# still substring-match "403" to distinguish a Cloudflare bot block (worth
# trying another fingerprint) from 401/404/5xx (won't be fixed by switching
# profiles).
#
# `curl-cffi>=0.15.0` in pyproject.toml is a floor that ensures the required
# profiles (firefox147 etc.) exist for fresh environments — it does NOT pin
# against future format drift. Strict version control in production comes
# from uv.lock. If a future curl_cffi version drops `.code` or changes the
# message format, this discriminator silently stops firing and we degrade to
# single-profile behavior — same as today's pre-PR baseline, not a regression.
_BLOCKED_LIBCURL_CODE = 22


async def _connect_with_fallback(
    ws_url: str,
    short_name: str,
) -> tuple[AsyncSession, Any, str]:
    """Try each impersonation profile until one upgrades successfully.

    Returns ``(session, ws, profile_name)``. Closes any session whose
    connect attempt failed. On the Cloudflare-403 signature or a
    per-attempt timeout, advances to the next profile; on any other
    error, re-raises immediately (transient/network failures aren't
    fixed by switching fingerprint).
    """
    raw = os.getenv(
        "OPENMHZ_IMPERSONATE_PROFILES", _DEFAULT_IMPERSONATE_PROFILES
    )
    profiles = [p.strip() for p in raw.split(",") if p.strip()]
    if not profiles:
        msg = "OPENMHZ_IMPERSONATE_PROFILES is empty"
        raise ValueError(msg)

    last_error: BaseException | None = None
    for profile in profiles:
        candidate = AsyncSession(impersonate=profile)
        # try/finally with `success` flag rather than `except Exception:` —
        # asyncio.CancelledError extends BaseException, not Exception, so a
        # task cancellation during ws_connect would otherwise leak the
        # candidate session.
        success = False
        try:
            ws = await asyncio.wait_for(
                candidate.ws_connect(ws_url),
                timeout=_PROFILE_CONNECT_TIMEOUT_SEC,
            )
            success = True
            return candidate, ws, profile
        except asyncio.TimeoutError as e:
            last_error = e
            logger.warning(
                "WS connect timeout (>%.1fs): short_name=%s impersonate=%s",
                _PROFILE_CONNECT_TIMEOUT_SEC,
                short_name,
                profile,
            )
        except CurlError as e:
            if (
                getattr(e, "code", 0) == _BLOCKED_LIBCURL_CODE
                and "403" in str(e)
            ):
                last_error = e
                logger.warning(
                    "WS profile blocked: short_name=%s impersonate=%s",
                    short_name,
                    profile,
                )
            else:
                # Non-403 CurlError (network, TLS, DNS, or 401/404/5xx during
                # the WS upgrade) — fingerprint switch won't help; let it
                # propagate after cleanup.
                raise
        finally:
            if not success:
                with contextlib.suppress(Exception):
                    await candidate.close()

    msg = (
        f"All {len(profiles)} impersonation profiles failed for "
        f"short_name={short_name}"
    )
    raise RuntimeError(msg) from last_error


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

    session, ws, profile = await _connect_with_fallback(ws_url, short_name)
    try:
        # --- EIO handshake ---
        open_frame = await ws.recv_str(timeout=10.0)
        open_data = _parse_eio_open(open_frame)
        sid = open_data["sid"]
        ping_interval_sec: float = open_data["pingInterval"] / 1000
        ping_timeout_sec: float = open_data["pingTimeout"] / 1000

        logger.info(
            "WebSocket connected: short_name=%s sid=%s impersonate=%s",
            short_name,
            sid,
            profile,
        )

        # --- SIO connect ---
        await ws.send_str("40")
        ack = await ws.recv_str(timeout=10.0)
        if not ack.startswith("40"):
            msg = f"Expected SIO connect ack (40...), got: {ack[:60]}"
            raise ValueError(msg)

        # --- Subscribe ---
        start_payload = json.dumps(
            ["start", {**_START_PAYLOAD_TEMPLATE, "shortName": short_name}]
        )
        await ws.send_str(f"42{start_payload}")
        logger.info("Subscribed to system: short_name=%s", short_name)

        yield _stream_frames(
            ws, shutdown, short_name, ping_interval_sec, ping_timeout_sec
        )
    finally:
        # `_connect_with_fallback` raises rather than returning None, so by
        # the time we reach here both `ws` and `session` are guaranteed bound.
        #
        # `contextlib.suppress(Exception)` does NOT catch BaseException
        # subclasses (asyncio.CancelledError, KeyboardInterrupt). Without
        # nested try/finally, a CancelledError raised by the worker shutting
        # down mid-`ws.send_str` would skip both `ws.close()` and
        # `session.close()`, leaking the curl handle. Each step gets its own
        # try/finally so subsequent cleanup steps are at least attempted on
        # any exception class.
        try:
            with contextlib.suppress(Exception):
                await ws.send_str("1")  # best-effort EIO close
        finally:
            try:
                with contextlib.suppress(Exception):
                    await ws.close()
            finally:
                with contextlib.suppress(Exception):
                    await session.close()


async def _stream_frames(
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
            frame = await ws.recv_str(timeout=ping_interval_sec)
        except WebSocketTimeout:
            continue
        except WebSocketClosed:
            logger.warning("WebSocket closed: short_name=%s", short_name)
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
            logger.warning("Server disconnect (41): short_name=%s", short_name)
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
