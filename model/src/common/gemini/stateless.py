import asyncio
import json
from collections import deque
from pathlib import Path
from typing import Any

from google.api_core import retry as api_retry
from google.api_core import retry_async as api_retry_async
from google.api_core.exceptions import ClientError, GoogleAPICallError
from google.genai import Client as GenAiClient
from google.genai import types
from loguru import logger
from tqdm.asyncio import tqdm

# Async Retry Policy protecting GCS streaming network channels
custom_async_retry = api_retry_async.AsyncRetry(
    predicate=api_retry.if_exception_type(
        (ClientError, GoogleAPICallError, asyncio.TimeoutError, ConnectionError)
    ),
    initial=1.0,
    maximum=30.0,
    multiplier=2.0,
    deadline=120.0,
)


def _write_checkpoint_sync(file_path: str, record: dict) -> None:
    """Writes a single prediction record to the local checkpoint file.

    Executed in a background thread to prevent blocking the async event loop.
    """
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


async def process_single_channel_stateless(
    channel_id: str,
    segments: list[dict],
    completed_records: dict[str, str],
    genai_client: GenAiClient,
    model_path: str,
    system_prompt: str,
    safety_settings: list,
    generation_config: dict,
    context_size: int,
    concurrency_limit: int,
    checkpoint_file: str,
    checkpoint_write_lock: asyncio.Lock,
    semaphore: asyncio.Semaphore,
    pbar: tqdm,
) -> list[dict]:
    """Decodes a single radio channel chronologically using a stateless sliding window.

    This function maintains conversational history as an in-memory queue of raw
    GCS audio URIs, bypassing Vertex AI cloud session states entirely.
    """
    results = []
    max_history_turns = max(0, context_size - 1)
    history_buffer = deque(maxlen=max_history_turns)

    # Initialize history buffer from already completed segments to support resumption
    for entry in segments:
        uri = entry["audio_filepath"]
        if uri in completed_records:
            history_buffer.append(uri)

    async with semaphore:
        for entry in segments:
            uri = entry["audio_filepath"]

            # If already processed in a previous run, skip API call but update history
            if uri in completed_records:
                result_dict = {
                    "example_id": channel_id,
                    "audio_filepath": uri,
                    "transcript": completed_records[uri],
                    "error": None,
                }
                results.append(result_dict)
                history_buffer.append(uri)
                pbar.update(1)
                continue

            # Build the stateless, multi-modal prompt context
            parts = []
            if len(history_buffer) > 0:
                parts.append(
                    types.Part.from_text(
                        text="--- HISTORICAL AUDIO CONTEXT (FOR REFERENCE ONLY) ---"
                    )
                )
                for past_audio_uri in history_buffer:
                    parts.append(
                        types.Part.from_uri(
                            file_uri=past_audio_uri, mime_type="audio/flac"
                        )
                    )

            parts.append(
                types.Part.from_text(
                    text="--- NEW TARGET AUDIO SEGMENT (TRANSCRIBE ONLY THIS SEGMENT VERBATIM) ---"
                )
            )
            parts.append(
                types.Part.from_uri(file_uri=uri, mime_type="audio/flac")
            )

            contents = [types.Content(role="user", parts=parts)]

            transcript = None
            error_msg = None

            # Direct stateless generation with native SDK async retry
            @custom_async_retry
            async def execute_stateless_call_with_retry(
                contents_payload: list = contents,
            ) -> Any:
                return await genai_client.aio.models.generate_content(
                    model=model_path,
                    contents=contents_payload,
                    config=types.GenerateContentConfig(
                        system_instruction=system_prompt.strip(),
                        safety_settings=safety_settings,
                        temperature=generation_config["temperature"],
                        max_output_tokens=generation_config[
                            "max_output_tokens"
                        ],
                        thinking_config=types.ThinkingConfig(thinking_budget=0),
                    ),
                )

            try:
                response = await asyncio.wait_for(
                    execute_stateless_call_with_retry(contents), timeout=60.0
                )
                if response.candidates and response.candidates[0].content.parts:
                    transcript = response.text.strip()
                else:
                    finish_reason = getattr(
                        response.candidates[0], "finish_reason", "UNKNOWN"
                    )
                    error_msg = f"Empty response. Reason: {finish_reason}"
            except Exception as e:
                error_msg = f"{type(e).__name__}: {e!s}"

            if transcript is not None:
                result_dict = {
                    "example_id": channel_id,
                    "audio_filepath": uri,
                    "transcript": transcript,
                    "error": None,
                }
                results.append(result_dict)

                # Append result to local checkpoint file safely in a background thread
                async with checkpoint_write_lock:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None,
                        _write_checkpoint_sync,
                        checkpoint_file,
                        result_dict,
                    )

                # Update rolling history for the next turn
                history_buffer.append(uri)
            else:
                results.append(
                    {
                        "example_id": channel_id,
                        "audio_filepath": uri,
                        "transcript": "",
                        "error": error_msg,
                    }
                )
                logger.error(
                    f"Stateless Turn Failed for segment: {Path(uri).name} | Error: {error_msg}"
                )

            pbar.update(1)

    return results
