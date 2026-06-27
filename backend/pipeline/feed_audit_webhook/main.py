"""FastAPI entrypoint for the Feed Audit Notification webhook relay."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import inspect
from typing import Any

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

setup_logging()
logger = logging.getLogger(__name__)

DeliveryHandler = Callable[[dict[str, Any]], object | Awaitable[object]]


def create_app(
    *,
    settings: FeedAuditWebhookSettings | None = None,
    delivery_handler: DeliveryHandler | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        app.state.settings = (
            settings if settings is not None else load_settings()
        )
        if delivery_handler is not None:
            app.state.delivery_handler = delivery_handler
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
            logger.warning("Invalid Feed Audit Notification Pub/Sub message")
            return Response(status_code=status.HTTP_400_BAD_REQUEST)

        handler: DeliveryHandler | None = getattr(
            request.app.state,
            "delivery_handler",
            None,
        )
        if handler is None:
            logger.warning(
                "Feed audit webhook relay delivery is not initialized"
            )
            return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

        result = handler(payload)
        if inspect.isawaitable(result):
            await result
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    return relay_app


app = create_app()
