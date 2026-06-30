"""FastAPI entrypoint for the Feed Change Notification webhook relay."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Protocol

from fastapi import FastAPI, Request, Response, status

from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.feed_change_webhook.pubsub import (
    InvalidPubSubMessage,
    extract_feed_change_payload,
)
from backend.pipeline.feed_change_webhook.settings import (
    FeedChangeWebhookSettings,
    load_settings,
)
from backend.pipeline.feed_change_webhook.webhook_client import (
    WebhookClient,
    WebhookDeliveryError,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Mapping

setup_logging()
logger = logging.getLogger(__name__)

_INVALID_PUBSUB_MESSAGE_LOG_FIELDS = {
    "relay_event": "feed_change_webhook_invalid_pubsub_message",
    "failure_class": "malformed_pubsub_message",
}
_CLIENT_NOT_INITIALIZED_LOG_FIELDS = {
    "relay_event": "feed_change_webhook_client_not_initialized",
    "failure_class": "configuration_error",
}
_UNHANDLED_DELIVERY_ERROR_LOG_FIELDS = {
    "relay_event": "feed_change_webhook_unhandled_delivery_error",
    "failure_class": "unexpected_delivery_error",
}


class WebhookSender(Protocol):
    def send(self, payload: Mapping[str, Any]) -> object: ...


def create_app(
    *,
    settings: FeedChangeWebhookSettings | None = None,
    webhook_client: WebhookSender | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        resolved_settings = (
            settings if settings is not None else load_settings()
        )
        app.state.settings = resolved_settings
        app.state.webhook_client = webhook_client or WebhookClient(
            webhook_url=resolved_settings.webhook_url,
            api_key=resolved_settings.webhook_api_key,
        )
        yield

    relay_app = FastAPI(title="Feed Change Webhook Relay", lifespan=lifespan)

    @relay_app.post("/pubsub/feed-change-notifications")
    async def receive_feed_change_notification(
        envelope: dict[str, Any],
        request: Request,
    ) -> Response:
        """Receive a Pub/Sub push message for a Feed Change Notification."""
        try:
            payload = extract_feed_change_payload(envelope)
        except InvalidPubSubMessage as exc:
            logger.warning(
                "Invalid Feed Change Notification Pub/Sub message",
                extra={
                    "json_fields": {
                        **_INVALID_PUBSUB_MESSAGE_LOG_FIELDS,
                        "reason": exc.reason,
                        "path": exc.path,
                    }
                },
            )
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        sender: WebhookSender | None = getattr(
            request.app.state,
            "webhook_client",
            None,
        )
        if sender is None:
            logger.warning(
                "Feed change webhook relay client is not initialized",
                extra={"json_fields": _CLIENT_NOT_INITIALIZED_LOG_FIELDS},
            )
            return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

        try:
            await asyncio.to_thread(sender.send, payload)
        except WebhookDeliveryError:
            # NACK every destination delivery failure, including non-retryable
            # 4xx responses, so Pub/Sub retains the message for retry/DLQ
            # handling instead of acknowledging and dropping a misconfigured
            # route.
            return Response(status_code=status.HTTP_502_BAD_GATEWAY)
        except Exception:
            logger.exception(
                "Unexpected Feed Change Notification relay failure",
                extra={"json_fields": _UNHANDLED_DELIVERY_ERROR_LOG_FIELDS},
            )
            return Response(status_code=status.HTTP_502_BAD_GATEWAY)

        return Response(status_code=status.HTTP_204_NO_CONTENT)

    return relay_app


app = create_app()
