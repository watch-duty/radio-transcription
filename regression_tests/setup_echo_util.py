"""Setup and cleanup utility for the Echo pipeline integration tests."""

import sys
import time

import pytest
import requests
from google.cloud import storage

from regression_tests.constants_util import PipelineConstants


class EchoTestConfig:
    """Structured resource object yielded by the setup_echo fixture."""

    def __init__(
        self,
        feed_id: str,
        rule_id: str,
        bucket_name: str,
    ) -> None:
        self.feed_id = feed_id
        self.rule_id = rule_id
        self.bucket_name = bucket_name


def _delete_feed(fe_proxy_api_url: str, id_token: str, feed_id: str) -> None:
    """Deactivate and delete the Echo feed"""
    headers = {
        "Authorization": f"Bearer {id_token}",
    }
    sys.stdout.write(f"Deactivating and deleting feed {feed_id}.\n")
    try:
        # Feeds cannot be active while before deletion.
        deactivate_res = requests.post(
            f"{fe_proxy_api_url}/api/v1/feeds/{feed_id}/deactivate",
            headers=headers,
            timeout=15,
        )
        deactivate_res.raise_for_status()

        delete_res = requests.delete(
            f"{fe_proxy_api_url}/api/v1/feeds/{feed_id}",
            headers=headers,
            timeout=15,
        )
        delete_res.raise_for_status()
        sys.stdout.write(f"Feed {feed_id} successfully deleted.\n")
        # Give feed time to be removed from the ingestion cycle.
        time.sleep(5)
    except Exception as e:
        sys.stdout.write(f"WARNING: Failed to delete feed: {e}\n")
        raise


def _delete_existing_feed_by_channel_name(
    fe_proxy_api_url: str, id_token: str, channel_name: str
) -> None:
    """Find and delete existing feeds matching the given channel name."""
    headers = {"Authorization": f"Bearer {id_token}"}
    try:
        sys.stdout.write(
            f"Checking for existing feed for channel: {channel_name}\n"
        )
        matching_feed_ids = []
        next_token = None
        while True:
            params = {"limit": 100}
            if next_token:
                params["nextToken"] = next_token

            res = requests.get(
                f"{fe_proxy_api_url}/api/v1/feeds",
                headers=headers,
                params=params,
                timeout=15,
            )
            res.raise_for_status()
            data = res.json()
            feeds = data if isinstance(data, list) else data.get("feeds", [])
            for feed in feeds:
                if feed.get("sourceFeedId") == channel_name:
                    feed_id = feed.get("id")
                    if feed_id:
                        matching_feed_ids.append(str(feed_id))

            next_token = (
                None
                if isinstance(data, list)
                else data.get("nextToken") or data.get("next_token")
            )
            if not next_token:
                break

        for feed_id in matching_feed_ids:
            sys.stdout.write(
                f"WARNING: Feed '{channel_name}' already exists "
                f"with ID: {feed_id}.\n"
                "There may be another ongoing test or the previous test "
                "did not properly teardown.\n"
                "Deleting feed to ensure a clean state.\n"
            )
            _delete_feed(fe_proxy_api_url, id_token, feed_id)
    except Exception as e:
        sys.stdout.write(
            f"WARNING: Failed to check/delete existing feed: {e}\n"
        )
        raise


def _delete_rule(fe_proxy_api_url: str, id_token: str, rule_id: str) -> None:
    """Delete the keyword rule, then sleep to let it propagate in the DB."""
    gateway_headers = {"Authorization": f"Bearer {id_token}"}
    sys.stdout.write(f"Deleting rule {rule_id}.\n")
    try:
        res = requests.delete(
            f"{fe_proxy_api_url}/api/v1/rules/{rule_id}",
            headers=gateway_headers,
            timeout=15,
        )
        res.raise_for_status()
        sys.stdout.write(f"Rule {rule_id} successfully deleted.\n")
        # Give rule time to be removed from the cache.
        time.sleep(60)
    except Exception as e:
        sys.stdout.write(f"WARNING: Failed to delete rule: {e}\n")
        raise


def _delete_existing_rule_by_name(
    fe_proxy_api_url: str, id_token: str, rule_name: str
) -> None:
    """Find and delete existing rules matching the given rule name."""
    headers = {"Authorization": f"Bearer {id_token}"}
    try:
        # This is intentionally simple: the rules API is not paginated today,
        # and regression environments should not accumulate many rules.
        res = requests.get(
            f"{fe_proxy_api_url}/api/v1/rules", headers=headers, timeout=15
        )
        if res.status_code == 200:
            rule_list = res.json()
            for rule in rule_list:
                if rule.get("ruleName") == rule_name:
                    rule_id = rule.get("ruleId")
                    if not rule_id:
                        continue
                    sys.stdout.write(
                        f"WARNING: Rule '{rule_name}' already exists "
                        f"with ID: {rule_id}.\n"
                        "There may be another ongoing test or previous "
                        "teardown did not finish.\n"
                        "Deleting rule to ensure a clean state.\n"
                    )
                    _delete_rule(fe_proxy_api_url, id_token, rule_id)
    except Exception as e:
        sys.stdout.write(
            f"WARNING: Failed to check/delete existing rule: {e}\n"
        )


def _create_echo_feed(
    fe_proxy_api_url: str, id_token: str, feed_name: str, channel_name: str
) -> str:
    """Create the Echo feed configuration."""
    headers = {
        "Authorization": f"Bearer {id_token}",
    }
    payload = {
        "name": feed_name,
        "sourceType": "echo",
        "sourceFeedId": channel_name,
    }
    sys.stdout.write("Creating Echo feed configuration.\n")
    res = requests.post(
        f"{fe_proxy_api_url}/api/v1/feeds",
        headers=headers,
        json=payload,
        timeout=15,
    )
    res.raise_for_status()
    feed = res.json()
    feed_id = feed["id"]
    sys.stdout.write(f"Successfully created feed with ID: {feed_id}\n")
    return feed_id


def _create_fire_rule(
    fe_proxy_api_url: str, id_token: str, rule_name: str, feed_id: str
) -> str:
    """Create basic rule to alert on the word 'fire'."""
    headers = {"Authorization": f"Bearer {id_token}"}
    rule_payload = {
        "ruleName": rule_name,
        "description": "Rule to alert on the word fire in pipeline test",
        "isActive": True,
        "scope": {"level": "FEED_SPECIFIC", "targetFeeds": [feed_id]},
        "conditions": {
            "evaluationType": "KEYWORD_MATCH",
            "operator": "ANY",
            "keywords": ["fire"],
            "caseSensitive": False,
        },
    }
    sys.stdout.write("Creating fire rule.\n")
    res = requests.post(
        f"{fe_proxy_api_url}/api/v1/rules",
        headers=headers,
        json=rule_payload,
        timeout=15,
    )
    res.raise_for_status()
    rule = res.json()
    rule_id = rule["ruleId"]
    sys.stdout.write(f"Successfully created rule with ID: {rule_id}\n")
    # Sleep to let cache expire
    sys.stdout.write("Sleep to reset evaluation rule cache.\n")
    time.sleep(65)
    return rule_id


@pytest.fixture(scope="module")
def echo_setup(
    id_token: str,
    fe_proxy_api_url: str,
    gcp_project: str,
    gcp_region: str,
    gcp_echo_bucket: str,
) -> EchoTestConfig:
    """Fixture that manages setup of resources for the Echo test."""
    feed_name = "regression-test-echo-feed"

    storage_client = storage.Client()

    # Bucket creation is required for registering feed without failure
    bucket = storage_client.bucket(gcp_echo_bucket)
    if not bucket.exists():
        sys.stdout.write(
            f"Bucket {gcp_echo_bucket} does not exist. Creating it...\n"
        )
        bucket = storage_client.create_bucket(
            gcp_echo_bucket, location=gcp_region
        )
    else:
        sys.stdout.write(
            f"WARNING: GCS bucket {gcp_echo_bucket} exists.\n"
            "There may be another ongoing test or the previous test "
            "did not properly teardown.\n"
            f"Proceeding test with existing GCS bucket {gcp_echo_bucket}...\n"
        )

    # Clean up any existing feed/rule for idempotency
    _delete_existing_feed_by_channel_name(
        fe_proxy_api_url, id_token, PipelineConstants.ECHO_CHANNEL_NAME
    )
    _delete_existing_rule_by_name(
        fe_proxy_api_url, id_token, PipelineConstants.RULE_NAME
    )

    # Register feed and rule
    feed_id = _create_echo_feed(
        fe_proxy_api_url,
        id_token,
        feed_name,
        PipelineConstants.ECHO_CHANNEL_NAME,
    )
    rule_id = _create_fire_rule(
        fe_proxy_api_url, id_token, PipelineConstants.RULE_NAME, feed_id
    )

    return EchoTestConfig(
        feed_id=feed_id,
        rule_id=rule_id,
        bucket_name=gcp_echo_bucket,
    )
