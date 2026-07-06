"""Regression test for the backend audio transcription/evaluation pipeline."""

import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

# Import the refactored yield fixture and resource model
from google.cloud import storage

from regression_tests.constants_util import PipelineConstants
from regression_tests.setup_echo_util import EchoTestConfig

# Disable mutual TLS (mTLS) check for local development workstations
os.environ["GOOGLE_API_USE_CLIENT_CERTIFICATE"] = "false"

AUDIO_SEGMENT_POLL_TIMEOUT_SEC = 300
NOTIFICATION_POLL_TIMEOUT_SEC = 300


def _poll_notification_log(
    segment_id: str,
    project: str,
    start_time: float,
) -> None:
    """Poll Cloud Logging to verify if a notification was sent or not."""
    environment = os.environ.get("REGRESSION_TEST_ENVIRONMENT", "dev")
    service_name = f"notification-pipeline-{environment}"

    while time.time() - start_time < NOTIFICATION_POLL_TIMEOUT_SEC:
        query = (
            'resource.type="cloud_run_revision" '
            f'AND resource.labels.service_name="{service_name}" '
            'AND textPayload:"Sending payload:" '
            f'AND textPayload:"{segment_id}"'
        )
        cmd = [
            "gcloud",
            "logging",
            "read",
            query,
            f"--project={project}",
            "--format=json",
            "--limit=1",
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
            msg = (
                "gcloud logging read failed; cannot verify notification "
                f"log: {result.stderr.strip()}"
            )
            raise AssertionError(msg)

        try:
            logs = json.loads(result.stdout.strip())
            if logs:
                return
        except json.JSONDecodeError as exc:
            msg = f"gcloud logging read returned invalid JSON: {exc}"
            raise AssertionError(msg) from exc

        time.sleep(5)

    msg = (
        f"Timed out after {NOTIFICATION_POLL_TIMEOUT_SEC} seconds "
        f"waiting for notification for segment {segment_id}"
    )
    raise AssertionError(msg)


def _upload_audio_file(
    bucket_name: str,
    channel_name: str,
    local_mp3_path: Path,
) -> str:
    """Upload a single MP3 file to GCS with a generated timestamped filename."""
    sys.stdout.write(f"Reading pre-recorded MP3 file at {local_mp3_path}.\n")
    with open(local_mp3_path, "rb") as f:
        mp3_content = f.read()

    # Initialize GCS client and bucket reference internally
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    now = datetime.now(UTC)
    fn = (
        f"{channel_name}/{now.strftime('%Y%m%d')}/"
        f"{channel_name}_{now.strftime('%Y%m%d_%H%M%S')}.mp3"
    )

    sys.stdout.write(f"Uploading {fn}...\n")
    bucket.blob(fn).upload_from_string(mp3_content, content_type="audio/mpeg")
    return fn


def _poll_audio_segment(
    fe_proxy_api_url: str,
    headers: dict,
    feed_id: str,
    bucket_name: str,
    filename: str,
    start_time: float,
) -> dict:
    """Poll the audio segments API until the segment is found and processed."""
    while time.time() - start_time < AUDIO_SEGMENT_POLL_TIMEOUT_SEC:
        res = requests.get(
            f"{fe_proxy_api_url}/api/v1/audioSegments/{feed_id}",
            headers=headers,
            timeout=15,
        )
        if res.status_code == 200:
            data = res.json()
            segments = data.get("segments", [])
            for s in segments:
                if s["externalAudioSegmentId"] == f"{bucket_name}/{filename}":
                    ann_types = [
                        ann["type"] for ann in s.get("annotations", [])
                    ]
                    if "TRANSCRIPT" in ann_types and "EVALUATION" in ann_types:
                        return s

        time.sleep(5)

    msg = (
        f"Timed out after {AUDIO_SEGMENT_POLL_TIMEOUT_SEC} seconds "
        f"waiting for segment '{filename}' to be processed."
    )
    raise AssertionError(msg)


def test_echo_pipeline_e2e_fire(
    id_token: str,
    fe_proxy_api_url: str,
    gcp_project: str,
    echo_setup: EchoTestConfig,
) -> None:
    """E2E test validating ingestion, transcription, rules and notification for a fire event."""
    feed_id = echo_setup.feed_id
    rule_id = echo_setup.rule_id
    bucket_name = echo_setup.bucket_name
    channel_name = PipelineConstants.ECHO_CHANNEL_NAME

    test_dir = Path(__file__).parent
    fire_mp3_path = (test_dir / "test_audio/has_fire.mp3").resolve()

    # Upload MP3 file to trigger ingestion on the Echo feed
    fire_audio_file = _upload_audio_file(
        bucket_name, channel_name, fire_mp3_path
    )

    gateway_headers = {"Authorization": f"Bearer {id_token}"}

    # Poll the transcripts/segments API and validate results recursively
    sys.stdout.write("Polling Audio Segments API for results...\n")
    fire_segment = _poll_audio_segment(
        fe_proxy_api_url=fe_proxy_api_url,
        headers=gateway_headers,
        feed_id=feed_id,
        bucket_name=bucket_name,
        filename=fire_audio_file,
        start_time=time.time(),
    )

    sys.stdout.write(f"Segment found: {json.dumps(fire_segment, indent=2)}\n")

    # Validate annotations
    ann_types = [ann["type"] for ann in fire_segment.get("annotations", [])]
    assert "TRANSCRIPT" in ann_types, "Segment missing TRANSCRIPT"
    assert "EVALUATION" in ann_types, "Segment missing EVALUATION"

    fire_transcript = next(
        ann
        for ann in fire_segment["annotations"]
        if ann["type"] == "TRANSCRIPT"
    )
    fire_eval = next(
        ann
        for ann in fire_segment["annotations"]
        if ann["type"] == "EVALUATION"
    )

    assert "fire" in fire_transcript["data"]["text"].lower(), (
        f"Transcript does not contain expected 'fire': {fire_transcript['data']['text']}"
    )
    assert rule_id in fire_eval["data"]["decisions"], (
        f"Expected decision {rule_id} in decisions: {fire_eval['data']['decisions']}"
    )

    # Validate notification dispatch in Cloud Run service logs
    sys.stdout.write("Verifying notification delivery logs...\n")
    _poll_notification_log(
        segment_id=fire_segment["id"],
        project=gcp_project,
        start_time=time.time(),
    )
    sys.stdout.write("Fire audio validation completed successfully.\n")


def test_echo_pipeline_e2e_no_fire(
    id_token: str,
    fe_proxy_api_url: str,
    gcp_project: str,
    echo_setup: EchoTestConfig,
) -> None:
    """E2E test validating ingestion, transcription, rules and NO notification for a non-fire event."""
    feed_id = echo_setup.feed_id
    bucket_name = echo_setup.bucket_name
    channel_name = PipelineConstants.ECHO_CHANNEL_NAME

    # Define path to our pre-recorded no_fire.mp3
    test_dir = Path(__file__).parent
    no_fire_mp3_path = (test_dir / "test_audio/no_fire.mp3").resolve()

    # Upload MP3 file to trigger ingestion
    no_fire_audio_file = _upload_audio_file(
        bucket_name, channel_name, no_fire_mp3_path
    )

    headers = {"Authorization": f"Bearer {id_token}"}
    # Poll the transcripts/segments API and validate results recursively
    sys.stdout.write("Polling Audio Segments API for results...\n")
    seg_no_fire = _poll_audio_segment(
        fe_proxy_api_url=fe_proxy_api_url,
        headers=headers,
        feed_id=feed_id,
        bucket_name=bucket_name,
        filename=no_fire_audio_file,
        start_time=time.time(),
    )

    sys.stdout.write(f"Segment found: {json.dumps(seg_no_fire, indent=2)}\n")

    # Validate annotations
    ann_types = [ann["type"] for ann in seg_no_fire.get("annotations", [])]
    assert "TRANSCRIPT" in ann_types, "Segment missing TRANSCRIPT"
    assert "EVALUATION" in ann_types, "Segment missing EVALUATION"

    no_fire_transcript = next(
        ann for ann in seg_no_fire["annotations"] if ann["type"] == "TRANSCRIPT"
    )
    no_fire_eval = next(
        ann for ann in seg_no_fire["annotations"] if ann["type"] == "EVALUATION"
    )

    assert "fire" not in no_fire_transcript["data"]["text"].lower(), (
        f"Transcript contains unexpected 'fire': {no_fire_transcript['data']['text']}"
    )
    rule_id = echo_setup.rule_id
    assert rule_id not in no_fire_eval["data"]["decisions"], (
        f"Expected rule {rule_id} to not be matched, but found it in: {no_fire_eval['data']['decisions']}"
    )

    sys.stdout.write("No-fire audio validation completed successfully.\n")
