#!/usr/bin/env python3
"""
Standalone Production CLI Runner for the Contextual Audio Transcription Pipeline.
Supports both pure stateless direct model calls and stateful context-cached ADK agent sessions.
Provides robust local/remote GCS checkpointing and rate-limiting to defeat API quotas.
"""

import argparse
import asyncio
import json
import os
import random
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

# Smart Logger Fallback to guarantee execution safety across various venv setups
try:
    from loguru import logger
except ImportError:
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)],
    )
    logger = logging.getLogger("transcribe_pipeline")
    # Add custom loguru methods to standard logger for compatibility
    logger.success = logger.info
    logger.warning = logger.warning

# Core GCP & Model Imports
try:
    import pandas as pd
    from google import adk
    from google.adk.client import Runner
    from google.adk.models import Gemini
    from google.adk.runners import GetSessionConfig, RunConfig
    from google.adk.sessions import VertexAiSessionService
    from google.api_core import retry as api_retry
    from google.api_core import retry_async as api_retry_async
    from google.api_core.exceptions import ClientError, GoogleAPICallError
    from google.cloud import storage
    from google.genai import Client as GenAiClient
    from google.genai import types
    from google.genai import types as genai_types
    from tqdm.asyncio import tqdm
except ImportError as err:
    logger.error(
        f"Missing required dependencies: {err}. Please ensure you are running in the correct environment (e.g. `uv run`)."
    )
    sys.exit(1)


# -----------------------------------------------------------------------------
# Core Exceptions
# -----------------------------------------------------------------------------
class SessionExpiredError(Exception):
    """Custom exception raised when a resumed session has expired or was deleted."""

    pass


class FatalPipelineError(Exception):
    """Custom exception raised when an unrecoverable error occurs that should abort the entire pipeline."""

    pass


# -----------------------------------------------------------------------------
# Global Thread & Rate-Limiting Locks
# -----------------------------------------------------------------------------
session_quota_lock = asyncio.Lock()
checkpoint_write_lock = asyncio.Lock()

# Foundational enterprise AsyncIO Retry Policy protecting all streaming network channels
custom_async_retry = api_retry_async.AsyncRetry(
    predicate=api_retry.if_exception_type(
        (ClientError, GoogleAPICallError, asyncio.TimeoutError, ConnectionError)
    ),
    initial=1.0,
    maximum=60.0,
    multiplier=2.0,
    deadline=300.0,
)

# Shared Global Constants (loaded during runtime)
SYSTEM_PROMPT = """\
Your primary task is to produce a strict, verbatim transcription of the spoken audio. Your absolute highest priority is to transcribe only what you hear with high acoustic certainty. Do not add, invent, or infer any speech that is not clearly audible. The audio may originate from VHF/UHF radio traffic and can include mic clicks, RF static, radio hum, and potentially unintelligible speech. When the audio is unequivocally confirmed as fire-related dispatch, speakers often use heavy jargon, and specific formatting rules apply.

EXPECTED TERMINOLOGY:
These are terms and unit identifiers commonly used in fire-related dispatch. These terms and formatting rules apply exclusively to audio that is unequivocally confirmed as fire-related dispatch. If these exact terms are clearly heard in the audio, transcribe them as listed. Do not invent or infer the use of these terms if they are not genuinely spoken.
copy, received, affirmative, affirm, proceed, responding, responding to, en-route, on-scene, in the area, available, returning, in service, got a caller, caller advising, in quarters, arrived, go ahead, back at, engine, tanker, brush, brush truck, tender, battalion, squad, ladder, tower, tower-ladder, medic, ambulance, k, branch, chopper, copter, AIQ, AOR, DO, IC, ICP, LAT, RP, SEAT, TAC, VFIRE, VLAT, patrol, rescue, station, personnel, air attack, air tactics, helispot, lead plane, strike team, control, being toned, box alarm, cancel the balance, chaparral, exposure protection, fire attack, fire boss, forward progress stopped, forward rate of spread stopped, heavy timber, left flank, light flashy fuels, rate of spread, right flank, structure defense, structure protection, structures threatened, terrain driven, wind driven, clear, clear and in service, code 1, code 2, code 3, code 4, code 33, medical call, fire alarm, commercial fire alarm, breathing problem, cardiac, heart problem, diabetic shock, mvc, trespass, harassment, 10-4, 10-7, 10-8, 10-9, 10-15, 10-20, 10-22, 10-23, 10-91, 10-97.

CRITICAL RULES:
1. Output the transcript strictly and precisely as spoken in the audio, with no newlines. Do not add, invent, or infer any speech that is not clearly audible.
2. When transcribing numbers, write the digits grouped together (e.g., 100, 6333).
3. If the audio contains a unit identifier, format it as the unit type followed by digits (e.g., Engine 41, Battalion 2). Apply this rule strictly only if the unit identifier is clearly spoken AND the context is unequivocally fire-related dispatch.
4. Transcribe only the duration of speech present. Do not extend the transcription with additional words or phrases that were not spoken, even if contextually plausible.

QUALITY GATE: Your absolute highest priority is to transcribe only what you hear with high acoustic certainty.
    *   If the audio contains clear speech that is not fire-related dispatch, you MUST transcribe it verbatim, exactly as heard, without applying any fire-specific formatting or jargon, and without attempting to interpret it as fire dispatch traffic.
    *   If a portion of audio is obscured, noisy, ambiguous, or contains speech that cannot be confidently identified, you MUST replace that specific portion with [UNINTELLIGIBLE].
    *   Do not attempt to infer, guess, or invent speech to fit any expected context or terminology list.
    *   Do not attempt to phonetically guess ambiguous noise.
    *   If the entire audio segment does not contain any discernible speech, output only [UNINTELLIGIBLE].

TASK:
Transcribe the attached audio. Output strictly the transcript.
"""

SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
]


# -----------------------------------------------------------------------------
# ConfiguredGemini Subclass (Prevents Local Default Credentials Resolution Errors)
# -----------------------------------------------------------------------------
class ConfiguredGemini(Gemini):
    """
    Subclass of ADK Gemini Model Wrapper to explicitly bind GCP Project and Location
    to the underlying GenAI client, bypassing ADK's location resolution bugs.
    """

    def __init__(self, *args, project: str, location: str, **kwargs):
        self._explicit_project = project
        self._explicit_location = location
        super().__init__(*args, **kwargs)

    @property
    def api_client(self):
        # Override the non-data descriptor client property to force location binding
        if getattr(self, "_api_client_override", None) is None:
            self._api_client_override = GenAiClient(
                project=self._explicit_project,
                location=self._explicit_location,
                vertexai=True,
            )
        return self._api_client_override


# -----------------------------------------------------------------------------
# Checkpoint and GCS Helpers
# -----------------------------------------------------------------------------
def get_gcs_checkpoint_blob(
    storage_client, consistent_output_uri, checkpoint_file
):
    """Derives GCS checkpoint path consistently for both load and upload."""
    out_bucket = consistent_output_uri.replace("gs://", "").split("/")[0]
    out_path = consistent_output_uri.replace("gs://", "").split("/")[1:]
    checkpoint_blob_path = "/".join(out_path[:-1]) + "/" + checkpoint_file
    return storage_client.bucket(out_bucket).blob(checkpoint_blob_path)


def load_gcs_checkpoint(
    storage_client, consistent_output_uri, checkpoint_file
) -> tuple[dict[str, str], dict[str, str]]:
    blob = get_gcs_checkpoint_blob(
        storage_client, consistent_output_uri, checkpoint_file
    )
    records = {}

    if blob.exists():
        logger.info(
            f"Found existing checkpoint on GCS: {blob.name}. Downloading..."
        )
        blob.download_to_filename(checkpoint_file)
    else:
        logger.info("No remote checkpoint found on GCS.")

    if os.path.exists(checkpoint_file):
        logger.info(f"Loading checkpoint from local file: {checkpoint_file}")
        with open(checkpoint_file, "r") as f:
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    if not record.get("error"):
                        records[record["audio_filepath"]] = record

        # Rewrite the local checkpoint file to be clean of errors and duplicates
        with open(checkpoint_file, "w") as f:
            for record in records.values():
                f.write(json.dumps(record) + "\n")

        logger.info(f"Loaded {len(records)} completed records.")
    else:
        logger.info("No local or remote checkpoint found. Starting fresh.")

    completed_transcripts = {
        filepath: rec["transcript"] for filepath, rec in records.items()
    }
    channel_sessions = {}
    for rec in records.values():
        if rec.get("session_id") and rec.get("example_id"):
            channel_sessions[rec["example_id"]] = rec["session_id"]

    return completed_transcripts, channel_sessions


async def upload_checkpoint_to_gcs(
    storage_client, consistent_output_uri, checkpoint_file
) -> None:
    """Synchronizes the local checkpoint file to GCS safely using CPython thread offloading."""
    async with checkpoint_write_lock:
        if os.path.exists(checkpoint_file):
            try:
                blob = get_gcs_checkpoint_blob(
                    storage_client, consistent_output_uri, checkpoint_file
                )
                await asyncio.to_thread(
                    blob.upload_from_filename, checkpoint_file
                )
            except Exception as sync_err:
                logger.warning(
                    f"Failed to backup checkpoint to GCS: {sync_err}"
                )


# -----------------------------------------------------------------------------
# Channel Processor
# -----------------------------------------------------------------------------
async def process_single_channel(
    channel_id: str,
    segment_entries: list[dict],
    completed_records: dict[str, str],
    channel_sessions: dict[str, str],
    semaphore: asyncio.Semaphore,
    pbar: tqdm,
    args,
    storage_client,
    session_service,
    audio_agent,
) -> list[dict]:
    """Processes a channel using either stateful sessions or pure stateless requests."""
    results = []

    try:
        async with semaphore:
            channel_user_id = f"{args.user_id}_{channel_id}"

            # ----------------------------------------------------------------
            # PATH A: Pure Stateless Execution (Air-Tight Direct Model Calls)
            # ----------------------------------------------------------------
            if not args.use_stateful_sessions:
                stateless_client = GenAiClient(
                    project=args.gcp_project,
                    location=args.gcp_location,
                    vertexai=True,
                )

                stateless_config = genai_types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    temperature=0.0,
                    max_output_tokens=512,
                    safety_settings=[
                        genai_types.SafetySetting(
                            category=s["category"],
                            threshold=s["threshold"],
                        )
                        for s in SAFETY_SETTINGS
                    ],
                )

                for turn_index, entry in enumerate(segment_entries, start=1):
                    uri = entry["audio_filepath"]

                    if uri in completed_records:
                        cached_transcript = completed_records[uri]
                        logger.info(
                            f"[CACHE HIT] Restored transcript for {uri} (Turn {turn_index}/{len(segment_entries)})"
                        )
                        results.append(
                            {
                                "example_id": channel_id,
                                "audio_filepath": uri,
                                "transcript": cached_transcript,
                                "error": None,
                            }
                        )
                        pbar.update(1)
                        continue

                    logger.debug(
                        f"[API CALL] Sending pure stateless request for {uri}..."
                    )
                    req_start_time = time.time()

                    try:
                        contents = [
                            genai_types.Part.from_uri(
                                file_uri=uri, mime_type="audio/flac"
                            )
                        ]

                        response = await asyncio.to_thread(
                            stateless_client.models.generate_content,
                            model=f"projects/{args.gcp_project}/locations/{args.gcp_location}/publishers/google/models/{args.model_id}",
                            contents=contents,
                            config=stateless_config,
                        )

                        usage = getattr(response, "usage_metadata", None)
                        prompt_tokens = (
                            getattr(usage, "prompt_token_count", 0)
                            if usage
                            else 0
                        )
                        cached_tokens = (
                            getattr(usage, "cached_content_token_count", 0)
                            if usage
                            else 0
                        )
                        output_tokens = (
                            getattr(usage, "candidates_token_count", 0)
                            if usage
                            else 0
                        )

                        transcript = None
                        final_error_msg = None

                        if response.candidates:
                            cand = response.candidates[0]
                            text = (
                                "".join(
                                    [
                                        p.text
                                        for p in cand.content.parts
                                        if p.text
                                    ]
                                ).strip()
                                if cand.content and cand.content.parts
                                else ""
                            )

                            f_reason = cand.finish_reason
                            if (
                                f_reason
                                and "STOP" not in str(f_reason)
                                and f_reason != 1
                            ):
                                final_error_msg = f"Stateless run failed with finish reason: {f_reason}"
                            else:
                                transcript = text
                        else:
                            final_error_msg = (
                                "Stateless run returned no candidates"
                            )

                    except Exception as err:
                        transcript = None
                        final_error_msg = (
                            f"Stateless run raised exception: {err}"
                        )

                    req_duration = time.time() - req_start_time

                    if transcript is not None:
                        if transcript == "":
                            logger.success(
                                f"[API SUCCESS] Pure ambient static/silence confirmed for {uri} in {req_duration:.2f}s (Turn {turn_index}/{len(segment_entries)})"
                            )
                        else:
                            logger.success(
                                f"[API SUCCESS] Final transcript acquired for {uri} in {req_duration:.2f}s (Turn {turn_index}/{len(segment_entries)})"
                            )
                        metrics = {
                            "turn_index": turn_index,
                            "latency": req_duration,
                            "prompt_tokens": prompt_tokens,
                            "cached_tokens": cached_tokens,
                            "output_tokens": output_tokens,
                            "is_fallback": False,
                        }
                        result_dict = {
                            "example_id": channel_id,
                            "audio_filepath": uri,
                            "transcript": transcript,
                            "error": None,
                            "session_id": None,
                            "metrics": metrics,
                        }
                        results.append(result_dict)

                        async with checkpoint_write_lock:
                            with open(args.checkpoint_file, "a") as f:
                                f.write(json.dumps(result_dict) + "\n")
                    else:
                        logger.error(
                            f"[API ERROR] {uri} - Final failure: {final_error_msg}"
                        )
                        result_dict = {
                            "example_id": channel_id,
                            "audio_filepath": uri,
                            "transcript": None,
                            "error": final_error_msg,
                            "session_id": None,
                        }
                        results.append(result_dict)
                        async with checkpoint_write_lock:
                            with open(args.checkpoint_file, "a") as f:
                                f.write(json.dumps(result_dict) + "\n")

                    pbar.update(1)

                try:
                    await upload_checkpoint_to_gcs(
                        storage_client,
                        args.consistent_output_uri,
                        args.checkpoint_file,
                    )
                except Exception as sync_err:
                    logger.warning(
                        f"Failed to backup checkpoint to GCS for channel {channel_id}: {sync_err}"
                    )

                return results

            # ----------------------------------------------------------------
            # PATH B: Stateful Caching Execution (Active Session ADK Runner)
            # ----------------------------------------------------------------
            session_id = channel_sessions.get(channel_id)
            is_resumed = session_id is not None
            new_session = None

            # Attempt loop: Attempt 0 (resumed session), Attempt 1 (new session fallback)
            for session_attempt in range(2):
                if is_resumed:
                    logger.debug(
                        f"Resuming existing session {session_id} for channel {channel_id} "
                        f"({len(segment_entries)} total segments)"
                    )
                else:
                    session_id = None
                    logger.info(
                        f"Starting brand new agent session for {channel_id}..."
                    )
                    for retry in range(args.max_retries):
                        try:
                            # 100% Fully staggered, rate-limited session creation to completely defeat 429 Quota errors
                            async with session_quota_lock:
                                new_session = (
                                    await session_service.create_session(
                                        user_id=channel_user_id,
                                        app_name=args.agent_engine_id,
                                        display_name=channel_id,
                                    )
                                )
                                await asyncio.sleep(
                                    0.5
                                )  # Minimal 500ms Quota stagger
                            if not new_session or not new_session.id:
                                raise ValueError(
                                    f"Invalid session created: {new_session}"
                                )
                            session_id = new_session.id
                            break
                        except Exception as e:
                            logger.warning(
                                f"Attempt {retry + 1} failed to create session for {channel_id}: {e}"
                            )
                            if isinstance(
                                e, (ClientError, GoogleAPICallError)
                            ) and (
                                e.code in (403, 404)
                                or "PERMISSION_DENIED" in str(e)
                                or "NOT_FOUND" in str(e)
                            ):
                                logger.error(
                                    f"FATAL SESSION ERROR: {e}. Aborting pipeline immediately."
                                )
                                raise FatalPipelineError(
                                    f"Pipeline aborted due to fatal unrecoverable session error: {e}"
                                ) from e

                            jitter_sleep = (2**retry) + random.uniform(1.0, 3.0)
                            await asyncio.sleep(jitter_sleep)

                    if not session_id:
                        error_msg = f"Failed to create Vertex AI session for channel {channel_id} after {args.max_retries} retries."
                        logger.error(error_msg)
                        for entry in segment_entries:
                            results.append(
                                {
                                    "example_id": channel_id,
                                    "audio_filepath": entry["audio_filepath"],
                                    "transcript": None,
                                    "error": error_msg,
                                }
                            )
                            pbar.update(1)
                        return results

                # Fully isolate the concurrent ADK Runner instance
                channel_runner = Runner(
                    agent=audio_agent,
                    app_name=args.agent_engine_id,
                    session_service=session_service,
                )

                run_config = RunConfig(
                    get_session_config=GetSessionConfig(
                        create_if_not_exists=False,
                        num_recent_events=args.num_recent_events,
                    )
                )

                session_expired = False
                results = []

                try:
                    for turn_index, entry in enumerate(
                        segment_entries, start=1
                    ):
                        uri = entry["audio_filepath"]

                        if uri in completed_records:
                            cached_transcript = completed_records[uri]
                            logger.info(
                                f"[CACHE HIT] Restored transcript for {uri} (Turn {turn_index}/{len(segment_entries)})"
                            )
                            results.append(
                                {
                                    "example_id": channel_id,
                                    "audio_filepath": uri,
                                    "transcript": cached_transcript,
                                    "error": None,
                                }
                            )
                            pbar.update(1)
                            continue

                        audio_part = types.Part.from_uri(
                            file_uri=uri, mime_type="audio/flac"
                        )
                        message = types.Content(role="user", parts=[audio_part])

                        final_error_msg = "Unknown error"
                        transcript = None
                        f_reason = None

                        prompt_tokens = 0
                        cached_tokens = 0
                        output_tokens = 0
                        is_fallback = False

                        @custom_async_retry
                        async def execute_streaming_turn():
                            streamed_text_chunks = []
                            finish_reason = None
                            embedded_error_msg = None
                            st_prompt = 0
                            st_cached = 0
                            st_output = 0

                            async for event in channel_runner.run_async(
                                user_id=channel_user_id,
                                session_id=session_id,
                                new_message=message,
                                run_config=run_config,
                            ):
                                if (
                                    hasattr(event, "error_message")
                                    and event.error_message
                                ):
                                    embedded_error_msg = event.error_message
                                if (
                                    hasattr(event, "finish_reason")
                                    and event.finish_reason
                                ):
                                    finish_reason = event.finish_reason

                                if getattr(event, "content", None) and getattr(
                                    event.content, "parts", None
                                ):
                                    for part in event.content.parts:
                                        if hasattr(part, "text") and part.text:
                                            streamed_text_chunks.append(
                                                part.text
                                            )

                                if event.is_final_response():
                                    if (
                                        hasattr(event, "raw_response")
                                        and event.raw_response.candidates
                                    ):
                                        finish_reason = (
                                            finish_reason
                                            or event.raw_response.candidates[
                                                0
                                            ].finish_reason
                                        )

                                    usage = getattr(
                                        event, "usage_metadata", None
                                    )
                                    if usage:
                                        st_prompt = (
                                            getattr(
                                                usage, "prompt_token_count", 0
                                            )
                                            or 0
                                        )
                                        st_cached = (
                                            getattr(
                                                usage,
                                                "cached_content_token_count",
                                                0,
                                            )
                                            or 0
                                        )
                                        st_output = (
                                            getattr(
                                                usage,
                                                "candidates_token_count",
                                                0,
                                            )
                                            or 0
                                        )
                                        logger.debug(
                                            f"{Path(uri).name} (Turn {turn_index}): "
                                            f"Prompt Tokens: {st_prompt} | "
                                            f"Cached Tokens: {st_cached} | "
                                            f"Output Tokens: {st_output}"
                                        )

                            if embedded_error_msg or (
                                finish_reason
                                and "STOP"
                                not in str(
                                    getattr(
                                        finish_reason, "name", finish_reason
                                    )
                                )
                                and "UNSPECIFIED"
                                not in str(
                                    getattr(
                                        finish_reason, "name", finish_reason
                                    )
                                )
                                and finish_reason != 0
                            ):
                                logger.warning(
                                    f"[API STREAM WARNING] {uri}: {embedded_error_msg or 'No embedded message'} (Reason: {finish_reason})"
                                )
                                full_text_so_far = "".join(streamed_text_chunks)
                                logger.warning(
                                    f"[DEBUG] Raw text generated before failure: {repr(full_text_so_far[:200])}"
                                )

                            full_text = "".join(streamed_text_chunks).strip()
                            return (
                                full_text,
                                finish_reason,
                                embedded_error_msg,
                                st_prompt,
                                st_cached,
                                st_output,
                            )

                        logger.debug(
                            f"[API CALL] Sending streaming request for {uri} in session {session_id}..."
                        )
                        req_start_time = time.time()
                        try:
                            (
                                res_text,
                                f_reason,
                                emb_err,
                                prompt_tokens,
                                cached_tokens,
                                output_tokens,
                            ) = await asyncio.wait_for(
                                execute_streaming_turn(),
                                timeout=args.socket_timeout,
                            )
                            transcript = res_text
                            if emb_err:
                                final_error_msg = emb_err
                            elif (
                                f_reason
                                and "STOP" not in str(f_reason)
                                and "UNSPECIFIED" not in str(f_reason)
                                and f_reason != 0
                            ):
                                final_error_msg = (
                                    f"Failed with finish reason: {f_reason}"
                                )
                            else:
                                final_error_msg = None
                        except Exception as stream_err:
                            # Check if session expired
                            is_404 = (
                                (
                                    isinstance(stream_err, ClientError)
                                    and stream_err.code == 404
                                )
                                or (
                                    isinstance(stream_err, GoogleAPICallError)
                                    and stream_err.code == 404
                                )
                                or (
                                    "404" in str(stream_err)
                                    and (
                                        "not found" in str(stream_err).lower()
                                        or "does not exist"
                                        in str(stream_err).lower()
                                    )
                                )
                            )
                            if is_resumed and is_404:
                                logger.warning(
                                    f"⚠️ Session {session_id} has expired or was deleted (404 NOT_FOUND). "
                                    f"Wiping channel cache and starting channel {channel_id} fresh in a new session!"
                                )
                                session_expired = True
                                break

                            # Check if fatal
                            is_fatal = (
                                isinstance(
                                    stream_err,
                                    (ClientError, GoogleAPICallError),
                                )
                                and (
                                    stream_err.code in (403, 400)
                                    or (
                                        stream_err.code == 404
                                        and not is_resumed
                                    )
                                )
                            ) or (
                                any(
                                    fatal_keyword in str(stream_err).upper()
                                    for fatal_keyword in (
                                        "403",
                                        "PERMISSION_DENIED",
                                        "INVALID_ARGUMENT",
                                    )
                                )
                            )
                            if is_fatal:
                                logger.error(
                                    f"FATAL TURN ERROR: {stream_err}. Aborting pipeline immediately."
                                )
                                raise FatalPipelineError(
                                    f"Pipeline aborted due to fatal unrecoverable turn error: {stream_err}"
                                ) from stream_err

                            transcript = None
                            final_error_msg = f"Streaming lane failed after upstream backoff: {stream_err}"
                            logger.warning(
                                f"[API STREAM FAILED] {uri}: {final_error_msg}"
                            )

                        req_duration = time.time() - req_start_time

                        # 🌿 Stateful Fallback
                        is_blocked_or_empty = (
                            transcript is None
                            or transcript == ""
                            or (
                                f_reason
                                and any(
                                    br in str(f_reason).upper()
                                    for br in (
                                        "RECITATION",
                                        "MAX_TOKENS",
                                        "SAFETY",
                                    )
                                )
                            )
                        )

                        if is_blocked_or_empty:
                            logger.warning(
                                f"[STATELESS FALLBACK] Stateful turn {turn_index} on {Path(uri).name} failed "
                                f"(Transcript: {repr(transcript)}, Finish Reason: {f_reason}, Error: {final_error_msg}). "
                                f"Executing stateless fallback with clean context..."
                            )
                            try:
                                fallback_client = GenAiClient(
                                    project=args.gcp_project,
                                    location=args.gcp_location,
                                    vertexai=True,
                                )

                                fallback_config = (
                                    genai_types.GenerateContentConfig(
                                        system_instruction=SYSTEM_PROMPT,
                                        temperature=0.0,
                                        max_output_tokens=512,
                                    )
                                )

                                fallback_contents = [
                                    genai_types.Part.from_uri(
                                        file_uri=uri, mime_type="audio/flac"
                                    )
                                ]

                                response = await asyncio.to_thread(
                                    fallback_client.models.generate_content,
                                    model=f"projects/{args.gcp_project}/locations/{args.gcp_location}/publishers/google/models/{args.model_id}",
                                    contents=fallback_contents,
                                    config=fallback_config,
                                )

                                usage = getattr(
                                    response, "usage_metadata", None
                                )
                                prompt_tokens = (
                                    getattr(usage, "prompt_token_count", 0)
                                    if usage
                                    else 0
                                )
                                cached_tokens = (
                                    getattr(
                                        usage, "cached_content_token_count", 0
                                    )
                                    if usage
                                    else 0
                                )
                                output_tokens = (
                                    getattr(usage, "candidates_token_count", 0)
                                    if usage
                                    else 0
                                )
                                is_fallback = True

                                if response.candidates:
                                    cand = response.candidates[0]
                                    fallback_text = (
                                        "".join(
                                            [
                                                p.text
                                                for p in cand.content.parts
                                                if p.text
                                            ]
                                        ).strip()
                                        if cand.content and cand.content.parts
                                        else ""
                                    )

                                    if fallback_text or (
                                        cand.finish_reason
                                        and "STOP" in str(cand.finish_reason)
                                    ):
                                        logger.success(
                                            f"[STATELESS FALLBACK SUCCESS] Successfully recovered transcript statelessly for {Path(uri).name}: '{fallback_text}'"
                                        )
                                        transcript = fallback_text
                                        final_error_msg = None
                                    else:
                                        logger.error(
                                            f"[STATELESS FALLBACK FAILED] Stateless fallback returned empty transcript for {Path(uri).name} (Reason: {cand.finish_reason})"
                                        )
                                        transcript = None
                                        final_error_msg = f"Stateless fallback returned empty transcript (Reason: {cand.finish_reason})"
                                else:
                                    logger.error(
                                        f"[STATELESS FALLBACK FAILED] Stateless fallback returned no candidates for {Path(uri).name}"
                                    )
                                    transcript = None
                                    final_error_msg = "Stateless fallback returned no candidates"
                            except Exception as fallback_err:
                                logger.error(
                                    f"[STATELESS FALLBACK CRITICAL] Stateless fallback raised exception: {fallback_err}"
                                )
                                transcript = None
                                final_error_msg = f"Stateless fallback raised exception: {fallback_err}"

                        # Output Processing
                        if transcript is not None:
                            if transcript == "":
                                logger.success(
                                    f"[API SUCCESS] Pure ambient static/silence confirmed for {uri} in {req_duration:.2f}s (Turn {turn_index}/{len(segment_entries)})"
                                )
                            else:
                                logger.success(
                                    f"[API SUCCESS] Final transcript acquired for {uri} in {req_duration:.2f}s (Turn {turn_index}/{len(segment_entries)})"
                                )
                            metrics = {
                                "turn_index": turn_index,
                                "latency": req_duration,
                                "prompt_tokens": prompt_tokens,
                                "cached_tokens": cached_tokens,
                                "output_tokens": output_tokens,
                                "is_fallback": is_fallback,
                            }
                            result_dict = {
                                "example_id": channel_id,
                                "audio_filepath": uri,
                                "transcript": transcript,
                                "error": None,
                                "session_id": session_id,
                                "metrics": metrics,
                            }
                            results.append(result_dict)

                            async with checkpoint_write_lock:
                                with open(args.checkpoint_file, "a") as f:
                                    f.write(json.dumps(result_dict) + "\n")
                        else:
                            logger.error(
                                f"[API ERROR] {uri} - Final failure: {final_error_msg}"
                            )
                            result_dict = {
                                "example_id": channel_id,
                                "audio_filepath": uri,
                                "transcript": None,
                                "error": final_error_msg,
                                "session_id": session_id,
                            }
                            results.append(result_dict)
                            async with checkpoint_write_lock:
                                with open(args.checkpoint_file, "a") as f:
                                    f.write(json.dumps(result_dict) + "\n")

                        pbar.update(1)

                    if session_expired:
                        for entry in segment_entries:
                            completed_records.pop(entry["audio_filepath"], None)
                        pbar.update(-len(results))
                        is_resumed = False
                        continue

                    try:
                        await upload_checkpoint_to_gcs(
                            storage_client,
                            args.consistent_output_uri,
                            args.checkpoint_file,
                        )
                    except Exception as sync_err:
                        logger.warning(
                            f"Failed to backup checkpoint to GCS for channel {channel_id}: {sync_err}"
                        )

                    break

                finally:
                    if session_id and not session_expired:
                        all_segments_succeeded = len(results) == len(
                            segment_entries
                        ) and all(
                            r.get("transcript") is not None for r in results
                        )
                        if all_segments_succeeded:
                            logger.info(
                                f"Purging completed session {session_id} for channel {channel_id}..."
                            )
                            for purge_retry in range(args.max_retries):
                                try:
                                    async with session_quota_lock:
                                        await session_service.delete_session(
                                            session_id=session_id,
                                            user_id=channel_user_id,
                                            app_name=args.agent_engine_id,
                                        )
                                        await asyncio.sleep(0.2)
                                    break
                                except Exception as purge_err:
                                    logger.warning(
                                        f"Purge attempt {purge_retry + 1} failed for session {session_id}: {purge_err}"
                                    )
                                    await asyncio.sleep(
                                        (2**purge_retry)
                                        + random.uniform(0.5, 1.5)
                                    )
    except FatalPipelineError:
        raise
    except Exception as fatal_err:
        processed_filepaths = {r["audio_filepath"] for r in results}
        unprocessed_entries = [
            e
            for e in segment_entries
            if e["audio_filepath"] not in processed_filepaths
        ]

        logger.error(
            f"🚨 FATAL CHANNEL CRASH for {channel_id}: {fatal_err}. "
            f"Marking {len(unprocessed_entries)} remaining segments as failed and continuing other channels!"
        )

        for entry in unprocessed_entries:
            results.append(
                {
                    "example_id": channel_id,
                    "audio_filepath": entry["audio_filepath"],
                    "transcript": None,
                    "error": f"Fatal channel crash: {fatal_err}",
                    "session_id": session_id,
                }
            )
            pbar.update(1)

    return results


# -----------------------------------------------------------------------------
# CLI Entrypoint & Main
# -----------------------------------------------------------------------------
async def main_async(args):
    # Initialize GCP storage client
    storage_client = storage.Client(project=args.gcp_project)

    completed_records = {}
    channel_sessions = {}

    if args.overwrite:
        logger.info("Overwrite mode active. Wiping outputs.")
        out_bucket_name = args.consistent_output_uri.replace("gs://", "").split(
            "/"
        )[0]
        out_blob_path = "/".join(
            args.consistent_output_uri.replace("gs://", "").split("/")[1:]
        )
        out_blob = storage_client.bucket(out_bucket_name).blob(out_blob_path)
        if out_blob.exists():
            out_blob.delete()
        if os.path.exists(args.checkpoint_file):
            os.remove(args.checkpoint_file)
        checkpoint_blob = get_gcs_checkpoint_blob(
            storage_client, args.consistent_output_uri, args.checkpoint_file
        )
        if checkpoint_blob.exists():
            checkpoint_blob.delete()
            logger.info("Deleted remote GCS checkpoint file.")
    else:
        logger.info("Overwrite mode inactive. Resuming from GCS checkpoint...")
        completed_records, channel_sessions = load_gcs_checkpoint(
            storage_client, args.consistent_output_uri, args.checkpoint_file
        )

    # Ingestion & Manifest Processing
    m_bucket = args.manifest_uri.replace("gs://", "").split("/")[0]
    m_path = "/".join(args.manifest_uri.replace("gs://", "").split("/")[1:])
    manifest_blob = storage_client.bucket(m_bucket).blob(m_path)
    if not manifest_blob.exists():
        raise FileNotFoundError(f"Manifest not found at {args.manifest_uri}")

    content = manifest_blob.download_as_text().strip().split("\n")
    channels = defaultdict(list)
    total_segments = 0

    for line in content:
        if line.strip():
            entry = json.loads(line)
            parent_audio_unit = Path(entry["audio_filepath"]).parent.name
            entry["example_id"] = parent_audio_unit
            channels[parent_audio_unit].append(entry)
            total_segments += 1

    # Strict Chronological Sorting
    for ch in channels:

        def get_sort_key(x):
            return (
                x.get("offset", 0),
                x.get("start_time", 0),
                x.get("audio_filepath", ""),
            )

        channels[ch].sort(key=get_sort_key)

    active_channels = {}
    missing_total = 0
    for cid, entries in channels.items():
        missing = [
            e for e in entries if e["audio_filepath"] not in completed_records
        ]
        if missing:
            active_channels[cid] = entries
            missing_total += len(missing)

    if not active_channels:
        logger.info("Everything is already complete.")
    else:
        active_segments_count = sum(
            len(entries) for entries in active_channels.values()
        )
        logger.info(
            f"Resuming {len(active_channels)} active independent audio recording units. "
            f"Processing {missing_total} pending segments."
        )

        # Initialize ADK Service & Agent Client
        session_service = VertexAiSessionService(
            project=args.gcp_project,
            location=args.control_plane_location,
        )

        audio_agent = ConfiguredGemini(
            model=f"publishers/google/models/{args.model_id}",
            project=args.gcp_project,
            location=args.gcp_location,
            system_instruction=SYSTEM_PROMPT,
            generation_config=genai_types.GenerateContentConfig(
                temperature=0.0,
                max_output_tokens=512,
                thinking_config=genai_types.ThinkingConfig(thinking_budget=0),
            ),
            safety_settings=[
                genai_types.SafetySetting(
                    category=s["category"],
                    threshold=s["threshold"],
                )
                for s in SAFETY_SETTINGS
            ],
        )

        semaphore = asyncio.Semaphore(args.concurrency_limit)
        with tqdm(
            total=active_segments_count, desc="Processing Transcriptions"
        ) as pbar:
            tasks = [
                asyncio.create_task(
                    process_single_channel(
                        cid,
                        entries,
                        completed_records,
                        channel_sessions,
                        semaphore,
                        pbar,
                        args,
                        storage_client,
                        session_service,
                        audio_agent,
                    )
                )
                for cid, entries in active_channels.items()
            ]

            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_EXCEPTION
            )

            failure_exception = None
            for task in done:
                if task.exception():
                    failure_exception = task.exception()
                    break

            if failure_exception:
                logger.error(
                    f"🚨 FAIL-FAST TRIGGERED: A channel encountered a fatal error: {failure_exception}. "
                    "Cancelling all other active channels immediately!"
                )
                for pending_task in pending:
                    pending_task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                raise failure_exception

    # Final Validation and Upload
    final_successes = {}
    final_errors = []

    if os.path.exists(args.checkpoint_file):
        with open(args.checkpoint_file, "r") as f:
            lines = f.readlines()

        final_ndjson = "".join(lines)
        out_bucket = args.consistent_output_uri.replace("gs://", "").split("/")[
            0
        ]
        out_path = "/".join(
            args.consistent_output_uri.replace("gs://", "").split("/")[1:]
        )
        storage_client.bucket(out_bucket).blob(out_path).upload_from_string(
            final_ndjson
        )
        logger.info(
            f"Results successfully saved to GCS: {args.consistent_output_uri}"
        )

        for line in lines:
            if line.strip():
                record = json.loads(line)
                if record.get("error"):
                    final_errors.append(record)
                else:
                    final_successes[record["audio_filepath"]] = record[
                        "transcript"
                    ]

    logger.info(
        f"Pipeline Complete. Total Successful Transcripts: {len(final_successes)}"
    )

    if final_errors:
        logger.error(
            f"⚠️ WARNING: {len(final_errors)} segments failed! Please re-run the script to retry the failures."
        )
    else:
        logger.success(
            "🎉 All segments processed successfully with zero errors!"
        )


def main():
    parser = argparse.ArgumentParser(
        description="CLI Runner for the Contextual Audio Transcription Pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Required Arguments
    parser.add_argument(
        "--manifest-uri",
        required=True,
        help="GCS URI of the batch manifest (e.g., gs://bucket/path/batch_manifest.jsonl)",
    )
    parser.add_argument(
        "--output-uri",
        required=True,
        help="GCS URI where predictions.jsonl will be saved (e.g., gs://bucket/path/predictions.jsonl)",
    )
    parser.add_argument(
        "--experiment-name",
        required=True,
        help="Experiment subdirectory name (e.g., bcfy_calls_v1)",
    )

    # Toggle and Calibration Settings
    parser.add_argument(
        "--use-stateful-sessions",
        action="store_true",
        default=False,
        help="Use stateful, context-cached ADK agent sessions. If omitted, runs in pure stateless mode.",
    )
    parser.add_argument(
        "--num-recent-events",
        type=int,
        default=100,
        help="Stateful lookback context window size (number of events). Only active when --use-stateful-sessions is enabled.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite existing outputs and GCS checkpoints to start a brand new run.",
    )

    # Execution Settings
    parser.add_argument(
        "--concurrency-limit",
        type=int,
        default=5,
        help="Maximum number of parallel channel lanes.",
    )
    parser.add_argument(
        "--model-id",
        default="gemini-3.1-flash-lite",
        help="Gemini model ID.",
    )

    # Infrastructure Settings
    parser.add_argument(
        "--gcp-project",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCP Project ID. Defaults to GCP_PROJECT_ID environment variable.",
    )
    parser.add_argument(
        "--gcp-location",
        default="us",
        help="GCP region location for model serving.",
    )
    parser.add_argument(
        "--control-plane-location",
        default="global",
        help="GCP location for Reasoning Engine and Agent Sessions control plane.",
    )
    parser.add_argument(
        "--agent-engine-id",
        default=os.environ.get("AGENT_ENGINE_ID"),
        help="Vertex AI Reasoning Engine App ID. Defaults to AGENT_ENGINE_ID environment variable.",
    )

    # Operational Thresholds
    parser.add_argument(
        "--checkpoint-file",
        default="interim_backup_predictions.jsonl",
        help="Local checkpoint backup file name.",
    )
    parser.add_argument(
        "--user-id",
        default="radio_transcription_worker",
        help="User ID namespace for session tracking.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum retries for rate-limiting and connection errors.",
    )
    parser.add_argument(
        "--socket-timeout",
        type=float,
        default=180.0,
        help="Socket read timeout in seconds.",
    )

    args = parser.parse_args()

    # Integrity Validations
    if not args.gcp_project:
        logger.error(
            "GCP Project ID must be provided via --gcp-project or the GCP_PROJECT_ID env variable."
        )
        sys.exit(1)
    if args.use_stateful_sessions and not args.agent_engine_id:
        logger.error(
            "AGENT_ENGINE_ID must be provided via --agent-engine-id or the AGENT_ENGINE_ID env variable when stateful sessions are active."
        )
        sys.exit(1)

    # Map output URI dynamically to maintain consistency with the notebook logic
    args.consistent_output_uri = args.output_uri

    logger.info("--------------------------------------------------")
    logger.info("🚀 Contextual Transcription Pipeline CLI Initiated")
    logger.info(f"Model ID:              {args.model_id}")
    logger.info(f"Project ID:            {args.gcp_project}")
    logger.info(
        f"Execution Mode:        {'Stateful (Context-Cached)' if args.use_stateful_sessions else 'Pure Stateless (Direct Model)'}"
    )
    if args.use_stateful_sessions:
        logger.info(f"Context Lookback Size: {args.num_recent_events} events")
        logger.info(f"Reasoning Engine ID:   {args.agent_engine_id}")
    logger.info(f"Concurrency Limit:     {args.concurrency_limit}")
    logger.info(f"Manifest GCS Path:     {args.manifest_uri}")
    logger.info(f"Output GCS Path:       {args.consistent_output_uri}")
    logger.info("--------------------------------------------------")

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.warning("\nExecution interrupted by user. Gracefully exiting.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Pipeline crashed with unhandled exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
