"""FastAPI entrypoint for the Feed Audit Notification webhook relay."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status

from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.feed_audit_webhook.settings import (
    FeedAuditWebhookSettings,
    load_settings,
)

setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    app.state.settings = load_settings()
    yield


app = FastAPI(title="Feed Audit Webhook Relay", lifespan=lifespan)


@app.post("/pubsub/feed-audit-notifications")
async def receive_feed_audit_notification(
    _request: Request,
) -> Response:
    """Receive a Pub/Sub push message for a Feed Audit Notification."""
    _settings: FeedAuditWebhookSettings = _request.app.state.settings
    logger.warning("Feed audit webhook relay delivery is not initialized")
    return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
