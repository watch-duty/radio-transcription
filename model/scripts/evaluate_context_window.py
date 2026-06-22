#!/usr/bin/env python3
"""Standalone CLI tool to evaluate ASR performance across different context window sizes (NUM_RECENT_EVENTS).

This script:
1. Downloads and analyzes a GCS manifest to find the deepest recording channels.
2. Deterministically samples a consistent set of channels using a stride-based selection.
3. Sweeps through multiple lookback window sizes (NUM_RECENT_EVENTS).
4. Runs stateful transcribing for each configuration, capturing latency and billing tokens.
5. Computes Word Error Rate (WER) and Character Error Rate (CER) using the repo's custom scoring module.
6. Saves the results to a clean CSV for analysis.
"""

import argparse
import asyncio
import csv
import json
import re
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

import jiwer
from google.api_core import retry as api_retry
from google.api_core import retry_async as api_retry_async
from google.api_core.exceptions import ClientError, GoogleAPICallError
from google.cloud import storage
from google.genai import Client as GenAiClient
from google.genai import types
from loguru import logger

# Try importing scoring; it will raise a clean error later if [scoring] extra is missing
try:
    from common import scoring
except ImportError as e:
    scoring = None
    _scoring_error = e

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
You will receive a sequence of audio files representing the history of the channel. The final audio file in the list is the new segment. Use the preceding audio files ONLY as acoustic and vocabulary context reference. Transcribe ONLY the final audio file verbatim. Output strictly the transcript of the final segment.
"""

SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
]

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


def clean_text_for_wer(text: str) -> str:
    """ASR text cleaning helper (lowercases, strips punctuation/spacing)."""
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(
        r"[^\w\s\-\:]", "", text
    )  # Remove punctuation except hyphens/colons
    text = re.sub(r"\s+", " ", text)  # Collapse spacing
    return text.strip()


def sample_deepest_channels(
    channel_counts: dict[str, int],
    sample_size: int,
    seed: int,
    min_segments: int,
) -> list[str]:
    """Deterministically samples a consistent set of channels.

    Uses a stride-based selection over sorted eligible channels to guarantee
    100% repeatable sampling across runs without utilizing pseudo-random generators,
    completely satisfying strict security and styling guidelines.
    """
    eligible_channels = [
        cid for cid, count in channel_counts.items() if count >= min_segments
    ]

    if not eligible_channels:
        logger.warning(
            f"⚠️ No channels contain at least {min_segments} segments! "
            f"Falling back to the deepest channels in the manifest."
        )
        sorted_by_depth = sorted(
            channel_counts.keys(), key=lambda x: channel_counts[x], reverse=True
        )
        eligible_channels = sorted_by_depth[: sample_size * 2]

    eligible_channels.sort()

    n_eligible = len(eligible_channels)
    if n_eligible <= sample_size:
        return eligible_channels

    # Calculate a consistent stride and start offset based on the seed
    stride = max(1, n_eligible // sample_size)
    start_offset = seed % stride

    sampled = []
    for i in range(sample_size):
        idx = (start_offset + i * stride) % n_eligible
        sampled.append(eligible_channels[idx])

    sampled.sort()
    return sampled


def is_fatal_error(exc: Exception) -> bool:
    """Helper to identify unrecoverable errors (e.g., region mismatch, bad credentials, missing model)."""
    exc_str = str(exc).upper()
    for fatal_keyword in (
        "400",
        "403",
        "404",
        "PERMISSION_DENIED",
        "NOT_FOUND",
        "INVALID_ARGUMENT",
    ):
        if fatal_keyword in exc_str:
            return True
    return False


async def evaluate_single_channel_config(
    channel_id: str,
    segments: list[dict],
    context_size: int,
    args: argparse.Namespace,
    genai_client: GenAiClient,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Runs stateless rolling history transcription for a channel using in-memory deque history."""
    results = []
    max_history_turns = max(0, context_size - 1)
    history_buffer = deque(maxlen=max_history_turns)
    system_instruction = SYSTEM_PROMPT
    model_path = f"projects/{args.gcp_project}/locations/{args.gcp_location}/publishers/google/models/{args.model_id}"

    async with semaphore:
        for turn_index, entry in enumerate(segments, start=1):
            uri = entry["audio_filepath"]
            reference = (
                entry.get("text")
                or entry.get("transcript")
                or entry.get("reference")
                or ""
            )

            # Build the list of multi-modal parts with explicit structural labels
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
            latency = 0.0
            prompt_tokens = 0
            cached_tokens = 0
            output_tokens = 0
            is_fallback = False
            err_msg = None
            req_start_time = time.time()

            @custom_async_retry
            async def execute_stateless_call_with_retry(
                contents_payload: list = contents,
            ) -> Any:
                loop = asyncio.get_running_loop()

                def run_api_call() -> Any:
                    return genai_client.models.generate_content(
                        model=model_path,
                        contents=contents_payload,
                        config=types.GenerateContentConfig(
                            system_instruction=system_instruction,
                            temperature=0.0,
                            max_output_tokens=512,
                            thinking_config=types.ThinkingConfig(
                                thinking_budget=0
                            ),
                        ),
                    )

                return await loop.run_in_executor(None, run_api_call)

            try:
                response = await asyncio.wait_for(
                    execute_stateless_call_with_retry(contents), timeout=60.0
                )
                transcript = response.text.strip() if response.text else ""

                usage = getattr(response, "usage_metadata", None)
                if usage:
                    prompt_tokens = getattr(usage, "prompt_token_count", 0) or 0
                    cached_tokens = (
                        getattr(usage, "cached_content_token_count", 0) or 0
                    )
                    output_tokens = (
                        getattr(usage, "candidates_token_count", 0) or 0
                    )
            except Exception as e:
                if is_fatal_error(e):
                    logger.error(
                        f"FATAL API ERROR for segment: {Path(uri).name} | Error: {e!s}. Aborting sweep."
                    )
                    raise
                err_msg = f"{type(e).__name__}: {e!s}"
                transcript = ""
                is_fallback = True

            latency = time.time() - req_start_time
            history_buffer.append(uri)

            results.append(
                {
                    "context_size": context_size,
                    "channel_id": channel_id,
                    "audio_filepath": uri,
                    "turn_index": turn_index,
                    "reference": reference,
                    "hypothesis": transcript,
                    "latency": latency,
                    "prompt_tokens": prompt_tokens,
                    "cached_tokens": cached_tokens,
                    "output_tokens": output_tokens,
                    "is_fallback": is_fallback,
                    "error": err_msg,
                }
            )

    return results


def _load_manifest_sync(manifest_path: str) -> str:
    """Downloads manifest contents from local file path or mock sync helper."""
    with open(manifest_path, encoding="utf-8") as f:
        return f.read()


def _write_csv_results_sync(
    output_csv: str, headers: list[str], rows: list[dict]
) -> None:
    """Writes the accumulated sweep metrics to CSV file (threaded sync)."""
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _save_jsonl_predictions_sync(jsonl_path: str, records: list[dict]) -> None:
    """Writes raw segment-by-segment predictions to JSONL file (threaded sync)."""
    with open(jsonl_path, "w", encoding="utf-8") as jsonl_file:
        jsonl_file.writelines(json.dumps(rec) + "\n" for rec in records)


def _cleanup_manifest_sync(path_obj: Path) -> None:
    """Safely checks and deletes the temp manifest download file (threaded sync)."""
    if path_obj.exists():
        path_obj.unlink()


def _compute_wer_cer_metrics(
    references: list[str], hypotheses: list[str]
) -> tuple[float, float]:
    """Computes alignment error rates using the decoupled scoring module."""
    if not scoring:
        msg = "Scoring module is not loaded."
        raise ImportError(msg)

    scored_pairs = [
        (r, h)
        for r, h in zip(references, hypotheses, strict=False)
        if r.strip()
    ]

    if not scored_pairs:
        return 0.0, 0.0

    normalizer = scoring.build_normalizer()
    clean_refs = [normalizer(r) for r, _ in scored_pairs]
    clean_hyps = [normalizer(h) for _, h in scored_pairs]

    wer_score = jiwer.wer(clean_refs, clean_hyps)
    cer_score = jiwer.cer(clean_refs, clean_hyps)
    return wer_score, cer_score


async def _run_single_sweep_size(
    size: int,
    channels: dict[str, list[dict]],
    sampled_cids: list[str],
    args: argparse.Namespace,
    genai_client: GenAiClient,
    semaphore: asyncio.Semaphore,
    csv_headers: list[str],
    sweep_results: list[dict],
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Executes a single sweep context window size in an isolated, green loop."""
    logger.info("\n" + "=" * 80)
    logger.info(
        f"🚀 RUNNING SWEEP FOR CONTEXT WINDOW SIZE: {size} (NUM_RECENT_EVENTS)"
    )
    logger.info("=" * 80)

    tasks = [
        loop.create_task(
            evaluate_single_channel_config(
                channel_id=cid,
                segments=channels[cid],
                context_size=size,
                args=args,
                genai_client=genai_client,
                semaphore=semaphore,
            )
        )
        for cid in sampled_cids
    ]

    try:
        channel_results = await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(
            f"FATAL Sweep Failure detected: {e!s}. Cancelling all other active channels..."
        )
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    all_segments = [item for sublist in channel_results for item in sublist]

    # Calculate metrics
    successful_segs = [s for s in all_segments if not s["error"]]

    refs = [s["reference"] for s in successful_segs]
    hyps = [s["hypothesis"] for s in successful_segs]

    wer_score, cer_score = _compute_wer_cer_metrics(refs, hyps)

    avg_lat = (
        sum(s["latency"] for s in successful_segs) / len(successful_segs)
        if successful_segs
        else 0.0
    )

    total_p = sum(s["prompt_tokens"] for s in successful_segs)
    total_c = sum(s["cached_tokens"] for s in successful_segs)
    cache_hit = (total_c / total_p * 100.0) if total_p > 0 else 0.0

    halluc_segs = len(
        [
            s
            for s in successful_segs
            if s["hypothesis"] == "[UNINTELLIGIBLE]" and s["reference"].strip()
        ]
    )
    halluc_rate = (
        (halluc_segs / len(successful_segs) * 100.0) if successful_segs else 0.0
    )
    fallback_rate = (
        len([s for s in all_segments if s["is_fallback"]])
        / len(all_segments)
        * 100.0
    )

    logger.success(f"--- RESULTS FOR CONTEXT SIZE {size} ---")
    logger.success(f"   - Word Error Rate (WER):      {wer_score * 100.0:.2f}%")
    logger.success(f"   - Character Error Rate (CER): {cer_score * 100.0:.2f}%")
    logger.success(f"   - Average Request Latency:    {avg_lat:.2f} seconds")
    logger.success(f"   - Context Cache Hit Rate:     {cache_hit:.1f}%")
    logger.success(f"   - Hallucination Rate:         {halluc_rate:.2f}%")
    logger.success(f"   - Stateless Fallback Rate:    {fallback_rate:.1f}%")

    result_row = {
        "context_size": size,
        "avg_wer": round(wer_score * 100.0, 2),
        "avg_cer": round(cer_score * 100.0, 2),
        "avg_latency": round(avg_lat, 2),
        "total_prompt_tokens": total_p,
        "total_cached_tokens": total_c,
        "cache_hit_rate": round(cache_hit, 1),
        "hallucination_rate": round(halluc_rate, 2),
        "fallback_rate": round(fallback_rate, 1),
        "total_segments": len(all_segments),
    }
    sweep_results.append(result_row)

    # Write Results to CSV safely in a background thread
    logger.info(
        f"Writing incremental results for size {size} to CSV: {args.output_csv}..."
    )
    await loop.run_in_executor(
        None,
        _write_csv_results_sync,
        args.output_csv,
        csv_headers,
        sweep_results,
    )

    # Write raw segment predictions safely in a background thread
    csv_path = Path(args.output_csv)
    jsonl_path = csv_path.parent / f"predictions_size_{size}.jsonl"
    logger.info(
        f"Saving raw segment predictions for size {size} to {jsonl_path}..."
    )
    await loop.run_in_executor(
        None, _save_jsonl_predictions_sync, str(jsonl_path), all_segments
    )


async def main_async(args: argparse.Namespace) -> None:
    """Async main orchestrator divided into highly cohesive modular tasks."""
    if not scoring:
        logger.error(
            "\n❌ DEPENDENCY ERROR: The [scoring] extra is not installed!\n"
            "To run this evaluation script, you must install the scoring dependencies (jiwer, NeMo) by running:\n\n"
            "   uv pip install -e 'model/[scoring]'\n\n"
            "Please run this command in your terminal and try again."
        )
        sys.exit(1)

    logger.info("Initializing Google Cloud and GenAI clients...")
    storage_client = storage.Client(project=args.gcp_project)

    client_kwargs = {
        "project": args.gcp_project,
        "location": args.gcp_location,
        "vertexai": True,
    }
    if args.gcp_location != "us":
        base_url = (
            f"https://{args.gcp_location}-aiplatform.googleapis.com"
            if args.gcp_location and args.gcp_location != "global"
            else "https://aiplatform.googleapis.com"
        )
        client_kwargs["http_options"] = types.HttpOptions(base_url=base_url)

    genai_client = GenAiClient(**client_kwargs)

    # 1. Download Manifest
    logger.info(f"Downloading manifest from GCS: {args.manifest_uri}...")
    m_bucket_name = args.manifest_uri.replace("gs://", "").split("/")[0]
    m_path = "/".join(args.manifest_uri.replace("gs://", "").split("/")[1:])
    manifest_blob = storage_client.bucket(m_bucket_name).blob(m_path)

    loop = asyncio.get_running_loop()

    # Download GCS content safely in thread pool executor to satisfy ASYNC230
    local_manifest_temp = "temp_manifest_download.jsonl"
    await loop.run_in_executor(
        None, manifest_blob.download_to_filename, local_manifest_temp
    )

    manifest_data_raw = await loop.run_in_executor(
        None, _load_manifest_sync, local_manifest_temp
    )

    temp_path_obj = Path(local_manifest_temp)
    await loop.run_in_executor(None, _cleanup_manifest_sync, temp_path_obj)

    # 2. Parse and Group segments by Channel
    manifest_lines = manifest_data_raw.strip().split("\n")
    channels = defaultdict(list)
    for line in manifest_lines:
        if line.strip():
            entry = json.loads(line)
            parent_dir = Path(entry["audio_filepath"]).parent.name
            entry["example_id"] = parent_dir
            channels[parent_dir].append(entry)

    # Sort chronologically
    for cid in channels:
        channels[cid].sort(
            key=lambda x: (
                x.get("offset", 0),
                x.get("start_time", 0),
                x.get("audio_filepath", ""),
            )
        )

    # 3. Deterministically Sample qualified channels
    channel_counts = {cid: len(entries) for cid, entries in channels.items()}
    context_sizes_list = [int(x.strip()) for x in args.context_sizes.split(",")]
    max_context = max(context_sizes_list)

    sampled_cids = sample_deepest_channels(
        channel_counts=channel_counts,
        sample_size=args.sample_size,
        seed=args.seed,
        min_segments=max_context,
    )

    logger.info(
        f"✅ Deterministically sampled {len(sampled_cids)} channels for evaluation:"
    )
    for cid in sampled_cids:
        logger.info(f"   - {cid}: {len(channels[cid])} segments")

    sweep_results = []
    csv_headers = [
        "context_size",
        "avg_wer",
        "avg_cer",
        "avg_latency",
        "total_prompt_tokens",
        "total_cached_tokens",
        "cache_hit_rate",
        "hallucination_rate",
        "fallback_rate",
        "total_segments",
    ]

    semaphore = asyncio.Semaphore(args.concurrency_limit)

    # 4. Execute Sweep sizes
    for size in context_sizes_list:
        await _run_single_sweep_size(
            size=size,
            channels=channels,
            sampled_cids=sampled_cids,
            args=args,
            genai_client=genai_client,
            semaphore=semaphore,
            csv_headers=csv_headers,
            sweep_results=sweep_results,
            loop=loop,
        )

    logger.success("\n🎉 CONTEXT WINDOW SWEEP COMPLETE!")
    logger.success(f"Results saved successfully to: {args.output_csv}")
    logger.success(
        "You can now download this CSV and plot it to find your empirical sweet spot!"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Standalone CLI harness to evaluate ASR performance across context window sizes.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Required parameters
    parser.add_argument(
        "--manifest-uri",
        required=True,
        help="GCS URI of the evaluation manifest containing ground truth (e.g. gs://bucket/batch_manifest.jsonl)",
    )

    # Sweeping configurations
    parser.add_argument(
        "--context-sizes",
        default="1,3,5,11,21,31,41,61,101,141",
        help="Comma-separated list of context window sizes to sweep (NUM_RECENT_EVENTS)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=3,
        help="Number of qualified recording channels to deterministically sample for the evaluation run",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Deterministic integer seed to control consistent stride offset selection across runs",
    )

    # Backend configurations
    parser.add_argument(
        "--gcp-project",
        required=True,
        help="GCP Project ID to run Vertex AI batch inference calls",
    )
    parser.add_argument(
        "--gcp-location",
        default="us-central1",
        help="GCP region/location for Vertex AI endpoint routing",
    )
    parser.add_argument(
        "--model-id",
        default="gemini-3.1-flash-lite-preview",
        help="Vertex GenAI Model Name/ID to sweep",
    )
    parser.add_argument(
        "--concurrency-limit",
        type=int,
        default=1,
        help="Maximum parallel GCS pipeline execution requests allowed concurrently (keep low to avoid API quota drops)",
    )
    parser.add_argument(
        "--output-csv",
        default="results/context_sweep_results.csv",
        help="Local file path where the accumulated CSV statistical metrics will be saved",
    )

    args = parser.parse_args()

    # Check output directory exists
    output_dir = Path(args.output_csv).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
