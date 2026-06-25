#!/usr/bin/env python3
"""Run Gemini ASR with transcript-only rolling prior context."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import google.auth
from common import gcs_utils
from google import genai
from google.cloud import storage
from google.genai import types

LOGGER = logging.getLogger(__name__)
DEFAULT_MODEL_ID = "gemini-3.1-flash-lite"
DEFAULT_GCP_LOCATION = "global"
MODEL_KEY = "gemini_transcript_context"
DATASET_MANIFEST_URIS = (
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "bcfy_calls/eval.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "bcfy_feeds/eval.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/echo/eval.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "fire_notifications/eval.jsonl",
)
SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {
        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
        "threshold": "BLOCK_NONE",
    },
    {
        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
        "threshold": "BLOCK_NONE",
    },
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
]
SYSTEM_PROMPT = """
Evaluate all audio specifically as VHF/UHF fire-related dispatch radio
traffic. The audio likely contains mic clicks, RF static, radio hum, and
possibly some unintelligible speech. The speakers use heavy jargon.

EXPECTED TERMINOLOGY:
copy, received, affirmative, affirm, proceed, responding, responding to,
en-route, on-scene, in the area, available, returning, in service, got a
caller, caller advising, in quarters, arrived, go ahead, back at, engine,
tanker, brush, brush truck, tender, battalion, squad, ladder, tower,
tower-ladder, medic, ambulance, k, branch, chopper, copter, AIQ, AOR, DO, IC,
ICP, LAT, RP, SEAT, TAC, VFIRE, VLAT, patrol, rescue, station, personnel, air
attack, air tactics, helispot, lead plane, strike team, control, being toned,
box alarm, cancel the balance, chaparral, exposure protection, fire attack,
fire boss, forward progress stopped, forward rate of spread stopped, heavy
timber, left flank, light flashy fuels, rate of spread, right flank, structure
defense, structure protection, structures threatened, terrain driven, wind
driven, clear, clear and in service, code 1, code 2, code 3, code 4, code 33,
medical call, fire alarm, commercial fire alarm, breathing problem, cardiac,
heart problem, diabetic shock, mvc, trespass, harassment, 10-4, 10-7, 10-8,
10-9, 10-15, 10-20, 10-22, 10-23, 10-91, 10-97.

CRITICAL RULES:
1. Output the transcript exactly as said, with no newlines.
2. When transcribing numbers, write the digits grouped together (e.g., 100,
6333).
3. Format all unit identifiers as the unit type followed by digits (e.g.,
Engine 41, Battalion 2).
4. Do not continue the speech segment beyond what is spoken.
5. QUALITY GATE: Transcribe only what you hear with high acoustic certainty.
If a portion of audio is obscured, noisy, or ambiguous, you MUST replace that
specific portion with [UNINTELLIGIBLE]. Do not attempt to phonetically guess
ambiguous noise.

TASK:
Transcribe the attached audio. Output strictly the transcript.
"""


@dataclass(frozen=True)
class HistoryTurn:
    """One previous transcript retained for transcript-only context.

    Attributes:
        row: Manifest row for the previous audio segment.
        transcript: Previous Gemini prediction from this same arm.
    """

    row: dict[str, Any]
    transcript: str


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("count", "duration"),
        required=True,
        help="Whether to cap prior context by segment count or active seconds.",
    )
    parser.add_argument(
        "--window",
        type=float,
        required=True,
        help="Count or duration value for the selected mode.",
    )
    parser.add_argument("--model-id", default=DEFAULT_MODEL_ID)
    parser.add_argument("--gcp-project", default=None)
    parser.add_argument("--gcp-location", default=DEFAULT_GCP_LOCATION)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-sleep-seconds", type=float, default=2.0)
    parser.add_argument("--max-output-tokens", type=int, default=512)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument(
        "--log-every",
        type=int,
        default=100,
        help=(
            "Log progress every N processed rows. Set to 0 to keep only "
            "start, error, and completion logs."
        ),
    )
    parser.add_argument(
        "--output-jsonl",
        default=None,
        help="Output JSONL path. Defaults under model/data/inference_manifests.",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume completed rows from --output-jsonl.",
    )
    return parser.parse_args()


def resolve_gcp_project(explicit_project: str | None) -> str:
    """Resolve the GCP project used for Vertex AI and GCS clients."""
    if explicit_project:
        return explicit_project
    _, default_project = google.auth.default()
    if default_project:
        return default_project
    try:
        completed = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(
            "Could not infer a GCP project. Pass --gcp-project explicitly."
        ) from exc
    project = completed.stdout.strip()
    if not project or project == "(unset)":
        raise RuntimeError(
            "Could not infer a GCP project. Pass --gcp-project explicitly."
        )
    return project


def default_output_path(args: argparse.Namespace) -> str:
    """Return the default output path for one experiment arm."""
    value = int(args.window) if args.window.is_integer() else args.window
    window = str(value).replace(".", "p")
    filename = f"gemini_transcript_context_{args.mode}_{window}.jsonl"
    return str(Path("model/data/inference_manifests") / filename)


def ensure_parent(path: str) -> None:
    """Create the parent directory for a target file path."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def append_jsonl_row(path: str, row: dict[str, Any]) -> None:
    """Append one JSON row to ``path``."""
    ensure_parent(path)
    with open(path, "a", encoding="utf-8") as out:
        out.write(json.dumps(row) + "\n")


def row_identity(row: dict[str, Any]) -> str:
    """Return the stable row identity used for resume and scoring joins."""
    return json.dumps(
        {
            "audio_filepath": row.get("audio_filepath"),
            "dataset_name": row.get("dataset_name"),
            "original_audio_uri": row.get("original_audio_uri"),
            "original_offset": row.get("original_offset"),
            "row_index": row.get("row_index"),
        },
        sort_keys=True,
    )


def load_resume_rows(path: str) -> dict[str, dict[str, Any]]:
    """Load complete prior output rows keyed by row identity."""
    output_path = Path(path)
    if not output_path.exists():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    with open(output_path, encoding="utf-8") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            row = json.loads(raw_line)
            identity = str(row.get("row_identity") or row_identity(row))
            if row.get(f"pred_text_{MODEL_KEY}") is not None:
                rows[identity] = row
    return rows


def episode_key(row: dict[str, Any]) -> str:
    """Return the safe transcript-context boundary for one row."""
    return str(
        row.get("original_audio_uri")
        or row.get("audio_uri")
        or row.get("source_group")
        or row.get("audio_filepath")
    )


def row_sort_key(row: dict[str, Any]) -> tuple[float, int, str]:
    """Sort rows inside one original recording."""
    offset = row.get("original_offset")
    if offset is None:
        offset = row.get("offset", 0.0)
    row_index = row.get("row_index")
    if row_index is None:
        row_index = 0
    return (float(offset), int(row_index), str(row.get("audio_filepath", "")))


def load_eval_rows(storage_client: storage.Client) -> list[dict[str, Any]]:
    """Load the four eval manifests from GCS."""
    rows: list[dict[str, Any]] = []
    for manifest_uri in DATASET_MANIFEST_URIS:
        manifest_rows = gcs_utils.download_jsonl_manifest(
            storage_client,
            manifest_uri,
        )
        for row in manifest_rows:
            row["manifest_uri"] = manifest_uri
            if row.get("audio_filepath") and row.get("text"):
                rows.append(row)
    return rows


def grouped_eval_rows(
    rows: list[dict[str, Any]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Group rows by original recording, preserving deterministic order."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(episode_key(row), []).append(row)
    return [
        (key, sorted(value, key=row_sort_key))
        for key, value in sorted(grouped.items())
    ]


def select_limited_groups(
    grouped_rows: list[tuple[str, list[dict[str, Any]]]],
    *,
    limit: int,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Apply an optional global row limit without reordering groups."""
    if limit <= 0:
        return grouped_rows
    selected: list[tuple[str, list[dict[str, Any]]]] = []
    remaining = limit
    for episode_id, rows in grouped_rows:
        if remaining <= 0:
            break
        chunk = rows[:remaining]
        if chunk:
            selected.append((episode_id, chunk))
            remaining -= len(chunk)
    return selected


def successful_for_history(transcript: str | None) -> bool:
    """Return whether a prediction should feed later context."""
    if transcript is None:
        return False
    stripped = transcript.strip()
    return bool(stripped) and stripped != "[UNINTELLIGIBLE]"


def select_history(
    history: list[HistoryTurn],
    *,
    mode: str,
    window: float,
) -> list[HistoryTurn]:
    """Select prior transcript history for one current request."""
    if mode == "count":
        count = max(0, int(window))
        return history[-count:] if count else []

    selected_reversed: list[HistoryTurn] = []
    total_duration = 0.0
    for turn in reversed(history):
        duration = float(turn.row.get("duration") or 0.0)
        if total_duration + duration > window:
            continue
        selected_reversed.append(turn)
        total_duration += duration
    return list(reversed(selected_reversed))


def prompt_text(history: list[HistoryTurn]) -> str:
    """Build the transcript-only context prompt."""
    if history:
        lines = [
            "PRIOR TRANSCRIPTS FOR CONTEXT ONLY.",
            "They may contain transcription errors.",
            "Do not re-transcribe, copy, or continue them.",
            "Use them only for recurring units, locations, and jargon.",
            "",
            "Prior transcripts, oldest to newest:",
        ]
        for index, turn in enumerate(history, start=1):
            transcript = " ".join(turn.transcript.split())
            lines.append(f"{index}. {transcript}")
    else:
        lines = [
            "There are no prior transcripts for this original recording.",
        ]
    lines.extend(
        [
            "",
            "CURRENT TASK:",
            "Transcribe only the attached audio clip.",
            "Output strictly the transcript for the current clip.",
            "Apply all critical rules from the system instruction.",
        ]
    )
    return "\n".join(lines)


def request_contents(
    *,
    row: dict[str, Any],
    history: list[HistoryTurn],
) -> list[types.Content]:
    """Build a Gemini request with text-only prior context plus current audio."""
    return [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=prompt_text(history)),
                types.Part.from_uri(
                    file_uri=str(row["audio_filepath"]),
                    mime_type="audio/flac",
                ),
            ],
        )
    ]


def generation_config(args: argparse.Namespace) -> types.GenerateContentConfig:
    """Return the Gemini generation config."""
    return types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT.strip(),
        safety_settings=SAFETY_SETTINGS,
        temperature=args.temperature,
        max_output_tokens=args.max_output_tokens,
    )


async def generate_with_retries(
    *,
    client: genai.Client,
    model_id: str,
    contents: list[types.Content],
    config: types.GenerateContentConfig,
    max_retries: int,
    retry_sleep_seconds: float,
) -> tuple[str, str | None]:
    """Generate one transcript, returning the text and optional error."""
    last_error = "Unknown error"
    for attempt in range(1, max_retries + 1):
        try:
            response = await client.aio.models.generate_content(
                model=model_id,
                contents=contents,
                config=config,
            )
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            LOGGER.warning(
                "Gemini attempt %d/%d failed: %s",
                attempt,
                max_retries,
                last_error,
            )
            if attempt < max_retries:
                await asyncio.sleep(retry_sleep_seconds * attempt)
        else:
            if response.candidates and response.candidates[0].content.parts:
                return response.text.strip(), None
            finish_reason = "UNKNOWN"
            if response.candidates:
                finish_reason = str(
                    getattr(response.candidates[0], "finish_reason", "UNKNOWN")
                )
            return "", f"Response blocked/empty. Reason: {finish_reason}"
    return "", last_error


def output_row(
    *,
    row: dict[str, Any],
    history: list[HistoryTurn],
    transcript: str,
    error: str | None,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Build one output row."""
    history_duration = sum(
        float(turn.row.get("duration") or 0.0) for turn in history
    )
    history_predictions = [turn.transcript for turn in history]
    return {
        **row,
        "row_identity": row_identity(row),
        "context_mode": args.mode,
        "context_window": args.window,
        "context_history_size": len(history),
        "context_history_duration": round(history_duration, 3),
        "context_history_audio_filepaths": [
            turn.row["audio_filepath"] for turn in history
        ],
        f"history_pred_text_{MODEL_KEY}": history_predictions,
        f"pred_text_{MODEL_KEY}": transcript,
        f"error_{MODEL_KEY}": error,
        "history_source": "same_arm_predictions",
        "uses_prior_audio": False,
    }


def should_log_progress(
    *,
    processed: int,
    target_total: int,
    log_every: int,
    has_error: bool,
) -> bool:
    """Return whether to emit a coarse progress log line."""
    return (
        has_error
        or processed == 1
        or processed == target_total
        or (log_every > 0 and processed % log_every == 0)
    )


async def process_episode(
    *,
    episode_id: str,
    rows: list[dict[str, Any]],
    client: genai.Client,
    config: types.GenerateContentConfig,
    args: argparse.Namespace,
    resume_rows: dict[str, dict[str, Any]],
    write_lock: asyncio.Lock,
    progress: dict[str, int],
    target_total: int,
) -> list[dict[str, Any]]:
    """Process one original recording sequentially so history is correct."""
    history: list[HistoryTurn] = []
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        selected_history = select_history(
            history,
            mode=args.mode,
            window=args.window,
        )
        identity = row_identity(row)
        if identity in resume_rows:
            out_row = resume_rows[identity]
        else:
            transcript, error = await generate_with_retries(
                client=client,
                model_id=args.model_id,
                contents=request_contents(row=row, history=selected_history),
                config=config,
                max_retries=args.max_retries,
                retry_sleep_seconds=args.retry_sleep_seconds,
            )
            out_row = output_row(
                row=row,
                history=selected_history,
                transcript=transcript,
                error=error,
                args=args,
            )
            async with write_lock:
                append_jsonl_row(args.output_jsonl, out_row)

        output_rows.append(out_row)
        transcript = str(out_row.get(f"pred_text_{MODEL_KEY}") or "")
        if successful_for_history(transcript):
            history.append(HistoryTurn(row=row, transcript=transcript))

        async with write_lock:
            progress["processed"] += 1
            has_error = bool(out_row.get(f"error_{MODEL_KEY}"))
            if should_log_progress(
                processed=progress["processed"],
                target_total=target_total,
                log_every=args.log_every,
                has_error=has_error,
            ):
                LOGGER.info(
                    "%d/%d mode=%s window=%s episode=%s history=%s error=%s",
                    progress["processed"],
                    target_total,
                    args.mode,
                    args.window,
                    episode_id,
                    out_row.get("context_history_size"),
                    has_error,
                )
    return output_rows


async def run_async(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Run one transcript-context Gemini experiment arm."""
    args.output_jsonl = args.output_jsonl or default_output_path(args)
    if not args.resume and Path(args.output_jsonl).exists():
        Path(args.output_jsonl).unlink()

    project = resolve_gcp_project(args.gcp_project)
    storage_client = storage.Client(project=project)
    grouped_rows = select_limited_groups(
        grouped_eval_rows(load_eval_rows(storage_client)),
        limit=args.limit,
    )
    if not grouped_rows:
        raise RuntimeError("No transcribable rows found.")

    resume_rows = load_resume_rows(args.output_jsonl) if args.resume else {}
    target_total = sum(len(rows) for _, rows in grouped_rows)
    LOGGER.info(
        "Running %s rows across %s episodes; resume rows=%s",
        target_total,
        len(grouped_rows),
        len(resume_rows),
    )

    client = genai.Client(
        vertexai=True,
        project=project,
        location=args.gcp_location,
    )
    config = generation_config(args)
    write_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(args.concurrency)
    progress = {"processed": 0}

    async def guarded_process_episode(
        episode_id: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        async with semaphore:
            return await process_episode(
                episode_id=episode_id,
                rows=rows,
                client=client,
                config=config,
                args=args,
                resume_rows=resume_rows,
                write_lock=write_lock,
                progress=progress,
                target_total=target_total,
            )

    grouped_outputs = await asyncio.gather(
        *[
            guarded_process_episode(episode_id, rows)
            for episode_id, rows in grouped_rows
        ]
    )
    output_rows = [row for rows in grouped_outputs for row in rows]
    output_rows.sort(key=lambda row: row_identity(row))
    ensure_parent(args.output_jsonl)
    with open(args.output_jsonl, "w", encoding="utf-8") as out:
        out.writelines(json.dumps(row) + "\n" for row in output_rows)
    LOGGER.info("Wrote %s rows to %s", len(output_rows), args.output_jsonl)
    print(
        json.dumps(
            {
                "mode": args.mode,
                "window": args.window,
                "rows": len(output_rows),
                "output_jsonl": args.output_jsonl,
                "gcp_project": project,
                "gcp_location": args.gcp_location,
                "model_id": args.model_id,
            },
            indent=2,
        )
    )
    return output_rows


def main() -> int:
    """Entrypoint."""
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    for logger_name in (
        "httpcore",
        "httpx",
        "google.genai",
        "google_genai",
        "google_genai.models",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    asyncio.run(run_async(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
