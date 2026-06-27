"""FastAPI entrypoint for the Feed Audit Notification webhook relay."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Protocol

from fastapi import FastAPI, Request, Response, status

from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.feed_audit_webhook.pubsub import (
    InvalidPubSubMessage,
    extract_feed_audit_payload,
)
from backend.pipeline.feed_audit_webhook.settings import (
    FeedAuditWebhookSettings,
    load_settings,
)
from backend.pipeline.feed_audit_webhook.wd_client import (
    WatchDutyWebhookClient,
    WatchDutyWebhookError,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Mapping

setup_logging()
logger = logging.getLogger(__name__)

_INVALID_PUBSUB_MESSAGE_LOG_FIELDS = {
    "relay_event": "feed_audit_webhook_invalid_pubsub_message",
    "failure_class": "malformed_pubsub_message",
}
_CLIENT_NOT_INITIALIZED_LOG_FIELDS = {
    "relay_event": "feed_audit_webhook_client_not_initialized",
    "failure_class": "configuration_error",
}


class WebhookSender(Protocol):
    def send(self, payload: Mapping[str, Any]) -> object: ...


def create_app(
    *,
    settings: FeedAuditWebhookSettings | None = None,
    wd_client: WebhookSender | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        resolved_settings = (
            settings if settings is not None else load_settings()
        )
        app.state.settings = resolved_settings
        app.state.wd_client = wd_client or WatchDutyWebhookClient(
            webhook_url=resolved_settings.wd_audit_webhook_url,
            api_key=resolved_settings.wd_backend_api_key,
        )
        yield

    relay_app = FastAPI(title="Feed Audit Webhook Relay", lifespan=lifespan)

    @relay_app.post("/pubsub/feed-audit-notifications")
    async def receive_feed_audit_notification(
        envelope: dict[str, Any],
        request: Request,
    ) -> Response:
        """Receive a Pub/Sub push message for a Feed Audit Notification."""
        try:
            payload = extract_feed_audit_payload(envelope)
        except InvalidPubSubMessage:
            logger.warning(
                "Invalid Feed Audit Notification Pub/Sub message",
                extra={"json_fields": _INVALID_PUBSUB_MESSAGE_LOG_FIELDS},
            )
            return Response(status_code=status.HTTP_400_BAD_REQUEST)

        sender: WebhookSender | None = getattr(
            request.app.state,
            "wd_client",
            None,
        )
        if sender is None:
            logger.warning(
                "Feed audit webhook relay WD client is not initialized",
                extra={"json_fields": _CLIENT_NOT_INITIALIZED_LOG_FIELDS},
            )
            return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

        try:
            await asyncio.to_thread(sender.send, payload)
        except WatchDutyWebhookError:
            return Response(status_code=status.HTTP_502_BAD_GATEWAY)

        return Response(status_code=status.HTTP_204_NO_CONTENT)

    return relay_app


app = create_app()
