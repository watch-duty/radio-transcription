"""Regression test for Feed Change Notification routing."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import requests

from backend.pipeline.common.feed_change_notification_contract import (
    FEED_CHANGE_NOTIFICATION_EVENT_TYPE,
    FEED_CHANGE_NOTIFICATION_SCHEMA_VERSION,
    FeedChangeNotificationPayload,
)
from regression_tests.setup_echo_util import _delete_feed

FEED_CHANGE_NOTIFICATION_POLL_TIMEOUT_SEC = 300
POLL_INTERVAL_SEC = 5


def _run_gcloud_logging_read(query: str, project: str) -> list[dict[str, Any]]:
    cmd = [
        "gcloud",
        "logging",
        "read",
        query,
        f"--project={project}",
        "--freshness=30m",
        "--format=json",
        "--limit=10",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        sys.stdout.write(
            f"WARNING: gcloud logging read failed: {result.stderr.strip()}\n"
        )
        return []

    try:
        logs = json.loads(result.stdout.strip() or "[]")
    except json.JSONDecodeError as exc:
        sys.stdout.write(
            f"WARNING: gcloud logging read returned invalid JSON: {exc}\n"
        )
        return []

    if not isinstance(logs, list):
        return []
    return [entry for entry in logs if isinstance(entry, dict)]


def _poll_feed_change_producer_log(
    *,
    feed_id: str,
    project: str,
    proof_start: str,
) -> FeedChangeNotificationPayload:
    deadline = time.time() + FEED_CHANGE_NOTIFICATION_POLL_TIMEOUT_SEC
    query = (
        f'jsonPayload.event_type="{FEED_CHANGE_NOTIFICATION_EVENT_TYPE}" '
        f"AND jsonPayload.schema_version={FEED_CHANGE_NOTIFICATION_SCHEMA_VERSION} "
        f'AND jsonPayload.feed_id="{feed_id}" '
        'AND jsonPayload.action="feed.created" '
        f'AND timestamp>="{proof_start}"'
    )

    while time.time() < deadline:
        logs = _run_gcloud_logging_read(query, project)
        if logs:
            latest_log = sorted(
                logs,
                key=lambda entry: str(entry.get("timestamp", "")),
            )[-1]
            payload = latest_log.get("jsonPayload")
            if isinstance(payload, dict):
                return FeedChangeNotificationPayload.model_validate(payload)
        time.sleep(POLL_INTERVAL_SEC)

    msg = (
        "Timed out waiting for Feed Change Notification producer log "
        f"for feed_id={feed_id} proof_start={proof_start}"
    )
    raise AssertionError(msg)


def _poll_feed_change_delivery_log(
    *,
    event_id: str,
    environment: str,
    project: str,
) -> None:
    deadline = time.time() + FEED_CHANGE_NOTIFICATION_POLL_TIMEOUT_SEC
    query = (
        'resource.type="cloud_run_revision" '
        f'AND resource.labels.service_name="feed-change-webhook-{environment}" '
        'AND jsonPayload.relay_event="feed_change_webhook_delivery" '
        f'AND jsonPayload.event_id="{event_id}"'
    )

    while time.time() < deadline:
        logs = _run_gcloud_logging_read(query, project)
        for entry in logs:
            payload = entry.get("jsonPayload")
            if not isinstance(payload, dict):
                continue
            status_code = payload.get("webhook_status_code")
            if isinstance(status_code, int) and 200 <= status_code < 300:
                return
        time.sleep(POLL_INTERVAL_SEC)

    msg = (
        "Timed out waiting for Feed Change Notification relay delivery log "
        f"for event_id={event_id}"
    )
    raise AssertionError(msg)


def _create_proof_feed(
    *,
    fe_proxy_api_url: str,
    id_token: str,
    proof_id: str,
) -> str:
    headers = {"Authorization": f"Bearer {id_token}"}
    response = requests.post(
        f"{fe_proxy_api_url}/api/v1/feeds",
        headers=headers,
        json={
            "name": f"ops-proof-feed-change-notification-{proof_id}",
            "sourceType": "echo",
            "sourceFeedId": f"ops-proof-feed-change-{proof_id}",
            "tags": [
                {
                    "key": "ops-proof",
                    "value": "feed-change-notification",
                }
            ],
        },
        timeout=15,
    )
    response.raise_for_status()
    feed = response.json()
    return str(feed["id"])


def test_feed_change_notification_route(
    id_token: str,
    fe_proxy_api_url: str,
    gcp_project: str,
) -> None:
    """Create an audited feed and verify the notification reaches the relay."""
    environment = os.environ.get("REGRESSION_TEST_ENVIRONMENT", "dev")
    proof_start = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    proof_id = (
        f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:12]}"
    )
    feed_id = ""

    try:
        feed_id = _create_proof_feed(
            fe_proxy_api_url=fe_proxy_api_url,
            id_token=id_token,
            proof_id=proof_id,
        )
        sys.stdout.write(
            f"Created feed-change route proof feed feed_id={feed_id}.\n"
        )

        producer_payload = _poll_feed_change_producer_log(
            feed_id=feed_id,
            project=gcp_project,
            proof_start=proof_start,
        )
        sys.stdout.write(
            "Found Feed Change Notification producer log "
            f"event_id={producer_payload.event_id}.\n"
        )

        _poll_feed_change_delivery_log(
            event_id=producer_payload.event_id,
            environment=environment,
            project=gcp_project,
        )
        sys.stdout.write(
            "Feed Change Notification route proof completed "
            f"event_id={producer_payload.event_id} feed_id={feed_id}.\n"
        )
    finally:
        if feed_id:
            _delete_feed(fe_proxy_api_url, id_token, feed_id)
