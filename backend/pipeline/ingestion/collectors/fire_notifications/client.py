from __future__ import annotations

import abc
import base64
import datetime
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from backend.pipeline.ingestion.collectors import aiohttp_requests, control_flow
from backend.pipeline.ingestion.failure_classifiers import http_status
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage import feed_store

if TYPE_CHECKING:
    import asyncio
    from zoneinfo import ZoneInfo

    import aiohttp

    from backend.pipeline.ingestion.collectors.failure_classification import (
        ItemFailure,
    )

logger = logging.getLogger(__name__)

_FN_POLL_HTTP_POLICY = http_status.HTTPStatusPolicy(
    exact={
        **http_status.DEFAULT_HTTP_STATUS_POLICY.exact,
    },
    default_4xx=(
        feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID
    ),
    default_5xx=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
    default_other_failure=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
)

_DOWNLOAD_MAX_RETRIES = 3
_DOWNLOAD_BACKOFF_BASE_SEC = 2.0


@dataclass(frozen=True)
class FireNotificationsFile:
    """Represents a parsed and validated file item from Fire Notifications."""

    uuid: str
    filename: str
    start_time: datetime.datetime
    size: int | None = None


class FireNotificationsClient(abc.ABC):
    """Abstract base class defining the interface for Fire Notifications clients.

    This permits swapping the concrete implementation (e.g. from HTTP Polling
    to a WebSocket or SSE client) without modifying the ingestion collector.
    """

    @abc.abstractmethod
    async def fetch_file_list(
        self,
        source_feed_id: str,
        feed_id: str,
        shutdown_event: asyncio.Event,
    ) -> list[FireNotificationsFile]:
        """Fetch and resolve new files from the external source."""

    @abc.abstractmethod
    async def download_audio(
        self,
        file_name: str,
        shutdown: asyncio.Event,
    ) -> bytes | ItemFailure:
        """Download raw audio bytes for a specific file."""


class FireNotificationsRestClient(FireNotificationsClient):
    """Concrete implementation of FireNotificationsClient using HTTP polling."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        url_base: str,
        s3_base_url: str,
        user: str,
        password: str,
        timezone: ZoneInfo,
    ) -> None:
        self.session = session
        self.url_base = url_base.rstrip("/")
        self.s3_base_url = s3_base_url.rstrip("/")
        self.auth_headers = self._build_auth_headers(user, password)
        self.timezone = timezone

    def _build_auth_headers(self, user: str, password: str) -> dict[str, str]:
        auth_str = f"{user}:{password}"
        encoded = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")
        return {"Authorization": f"Basic {encoded}"}

    def _parse_filename_timestamp(
        self, filename: str, channel_key: str
    ) -> datetime.datetime:
        """Extract and localize timestamp from Fire Notifications filename."""
        base = filename.removesuffix(".mp3")
        parts = base.split(" ")
        if len(parts) < 3:
            msg = f"Unexpected filename format: {filename}"
            raise ValueError(msg)

        date_str = parts[-2]
        time_str = parts[-1]

        dt_naive = datetime.datetime.strptime(
            f"{date_str} {time_str}", "%Y-%m-%d %H-%M-%S"
        )
        tz = self.timezone
        dt_aware = dt_naive.replace(tzinfo=tz)
        return dt_aware.astimezone(datetime.UTC)

    async def fetch_file_list(
        self,
        source_feed_id: str,
        feed_id: str,
        shutdown_event: asyncio.Event,
    ) -> list[FireNotificationsFile]:
        """Fetch, filter, and parse the file list from HTTP polling endpoint."""
        poll_url = f"{self.url_base}/{source_feed_id}"

        def validate_payload(payload: object) -> dict[str, Any]:
            if not isinstance(payload, dict):
                msg = "payload must be a dictionary"
                raise TypeError(msg)
            return cast("dict[str, Any]", payload)

        data = await aiohttp_requests.fetch_json_with_retries(
            self.session,
            poll_url,
            shutdown_event,
            retry_config=aiohttp_requests.RetryConfig(
                timeout_sec=10.0,
                max_attempts=3,
                base_delay_sec=1.0,
                sleep_func=control_flow.sleep_or_cancel,
            ),
            headers=self.auth_headers,
            log_label=f"Fire Notifications API feed {feed_id}",
            reason_prefix="fn_api_http",
            status_policy=_FN_POLL_HTTP_POLICY,
            validate_payload=validate_payload,
            invalid_payload_status_reason=(
                feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID
            ),
            invalid_payload_reason="fn_api_payload_malformed",
            transport_status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            transport_reason="source_unreachable",
        )

        files_raw = data.get("files")
        if files_raw is None:
            return []
        if not isinstance(files_raw, list):
            msg = "fn_api_payload_malformed"
            raise FeedFailure(
                feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                msg,
            )

        resolved_files = []
        seen_filenames = set()
        for f in files_raw:
            if not isinstance(f, dict):
                continue
            if f.get("type") != "file":
                continue
            name = f.get("name", "")
            if not name.endswith(".mp3"):
                continue

            if name in seen_filenames:
                # TODO(https://linear.app/watchduty/issue/GOO-634): Handle duplicate segments with the same timestamp better once the new API is available.
                # For now just select the first one seen.
                continue
            seen_filenames.add(name)

            file_uuid = f.get("uuid")
            if not file_uuid:
                continue

            try:
                start_time = self._parse_filename_timestamp(
                    name, source_feed_id
                )
            except ValueError:
                logger.warning(
                    "Failed to parse timestamp from filename: %s", name
                )
                continue

            raw_size = f.get("size")
            size = None
            if raw_size is not None:
                try:
                    size = int(raw_size)
                except (ValueError, TypeError):
                    logger.warning(
                        "Invalid non-numeric size ignored: %s", raw_size
                    )

            resolved_files.append(
                FireNotificationsFile(
                    uuid=str(file_uuid),
                    filename=str(name),
                    start_time=start_time,
                    size=size,
                )
            )

        # Sort files chronologically by start_time
        resolved_files.sort(key=lambda x: x.start_time)
        return resolved_files

    async def download_audio(
        self,
        file_name: str,
        shutdown: asyncio.Event,
    ) -> bytes | ItemFailure:
        """Download an item audio file from S3."""
        url = f"{self.s3_base_url}/{file_name}"
        result = await aiohttp_requests.download_item_media(
            self.session,
            url,
            shutdown,
            retry_config=aiohttp_requests.RetryConfig(
                timeout_sec=30.0,
                max_attempts=_DOWNLOAD_MAX_RETRIES,
                base_delay_sec=_DOWNLOAD_BACKOFF_BASE_SEC,
                sleep_func=control_flow.sleep_or_cancel,
            ),
            log_label="Fire Notifications item audio",
        )
        if isinstance(result, aiohttp_requests.DownloadedItem):
            return result.content
        return result
