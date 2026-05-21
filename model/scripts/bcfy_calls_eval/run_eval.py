#!/usr/bin/env python3
"""Framing-context A/B eval on the Broadcastify-Calls eval set.

Four arms = {gemini, chirp} x {baseline, moderate}. The only thing that varies
within a model is whether the per-call framing descriptor (from
``context_records.json``, rendered by ``framing_renderer``) is appended to the
production prompt. Phrase-hint adaptation (Chirp) and every other knob are held
constant, so the baseline-vs-moderate delta isolates framing.

- Gemini arm: Vertex batch (``gemini-3.1-flash-lite-preview``), per-segment
  ``system_instruction`` = production prompt [+ framing].
- Chirp arm: Speech-to-Text v2 synchronous ``recognize`` (``chirp_3``),
  per-segment ``custom_prompt`` = production chirp prompt [+ framing], with the
  production phrase-set adaptation on both arms.

Scoring reuses echo_eval's jiwer + NeMo + dispatch-quirks normalizer and adds
GT-word-length buckets + paired-bootstrap CIs + the spec's decision gates
(``decision_gates.py``).

Subcommands:
    run_eval.py transcribe [--limit N] [--arms gemini,chirp]
    run_eval.py merge
    run_eval.py score
    run_eval.py all [--limit N]
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger

from bcfy_calls_eval import decision_gates, framing_renderer

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GEMINI_MODEL_ID = "gemini-3.1-flash-lite-preview"  # spec: 3.1 Flash Lite, zero-shot
CHIRP_MODEL = "chirp_3"
CHIRP_LOCATION = "us"
CHIRP_RECOGNIZER = "_"
CHIRP_LANGUAGES = ["en-US"]

GCP_PROJECT_ID = "automatic-hawk-481415-m9"
GCP_LOCATION = "global"  # Vertex (Gemini batch)
GCS_BUCKET = "wd-transcription-data"
EXPERIMENT_NAME = "framing_context_05_2026"

GCS_INPUT_DIR = "segmented_audio/broadcastify/calls/eval/audio_raw"
MANIFEST_URI = f"gs://{GCS_BUCKET}/{GCS_INPUT_DIR}/batch_manifest.jsonl"

TRANSCRIPT_ROOT = f"transcripts/broadcastify/calls/eval/{EXPERIMENT_NAME}"
MERGED_GCS_URI = (
    f"gs://{GCS_BUCKET}/inference_manifests/broadcastify/calls/eval/"
    f"{EXPERIMENT_NAME}/merged.jsonl"
)

SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_DIR = SCRIPT_DIR / "results" / EXPERIMENT_NAME
CONTEXT_RECORDS_PATH = RESULTS_DIR / "context_records.json"
FLAC_CACHE = SCRIPT_DIR / ".flac_cache"

# Production prompts (verbatim from the live transcription path).
# V14 dispatcher prompt — the strong baseline (36.68% WER on one_hour_pilot with
# Flash Lite). The earlier NOTEBOOK prompt's "EXPECTED TERMINOLOGY" jargon list
# primed the model to fabricate dispatch text from noise (inflated baseline WER
# + hallucination); V14 + frequency_penalty is the production-faithful config.
GEMINI_BASE_PROMPT = """
ROLE: Expert radio dispatcher and verbatim transcriptionist.
GOAL: Word-for-word transcript of emergency radio traffic.

AUDIO CHARACTERISTICS:
- Variable noise floors and distortion are expected.
- Prioritize phonetic matching over grammar.

GLOSSARY:
- DISPATCHERS: Redcom, Santalina, Felton, Control, Alarm.
- UNITS: Engine, Tanker, Brush, Tender, Battalion, Squad, Medic, Ambulance.
- STATUS: 10-4, 10-8, 10-22, On-scene, En-route, AIQ, AOR.

STRICT RULES:
1. STRICT AUDIO LOCALITY: Transcribe ONLY what is audible.
2. VERBATIM ONLY: Use digits for numbers.
3. METADATA REJECTION: Transcribe spoken words only. If no spoken words are present, you MUST return an EMPTY STRING.
4. NO FORMATTING: Do not use asterisks, bolding, or markdown. Output raw text only.
5. SHORT TRANSMISSIONS: If the transmission is just one or two words (e.g., "copy", "received"), output ONLY those words. Do NOT invent unit numbers or dispatchers.

TASK:
Transcribe the provided audio verbatim. Output only the transcript.
""".strip()

CHIRP_BASE_PROMPT = """
Evaluate all audio specifically as VHF/UHF fire-related dispatch radio traffic. The speakers will use heavy jargon, but you must transcribe EVERY spoken word, including conversational phrasing and incomplete sentences.
This audio likely contains mic clicks, RF static, radio hum, and possibly some unintelligible speech. Only transcribe intelligible speech.

CRITICAL RULES:
* If the audio is completely unintelligible, output the following: [UNINTELLIGIBLE]
* Output the transcript exactly as said, with no newlines.
* Do not continue the speech segment beyond what is spoken.
* When transcribing numbers, write the digits grouped together (e.g., 100 instead of one hundred, 6333 instead of 63 33).
* Format all unit identifiers as the unit type followed by digits (e.g., Engine 41, Battalion 2, Medic 12).
""".strip()

USER_TURN_TEXT = (
    "Transcribe this emergency radio communication segment verbatim per the rules above."
)

SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
]

FRAMING_VARIANTS = ("baseline", "moderate")  # aggressive dropped: ~2% of clips differ


# ---------------------------------------------------------------------------
# Framing lookup (per-call descriptor from context_records.json)
# ---------------------------------------------------------------------------

_CONTEXT_RECORDS: dict[str, dict] | None = None


def _context_records() -> dict[str, dict]:
    global _CONTEXT_RECORDS
    if _CONTEXT_RECORDS is None:
        if not CONTEXT_RECORDS_PATH.exists():
            raise FileNotFoundError(
                f"missing {CONTEXT_RECORDS_PATH}; run build_context_records first"
            )
        _CONTEXT_RECORDS = json.loads(CONTEXT_RECORDS_PATH.read_text())
    return _CONTEXT_RECORDS


def _example_id_from_uri(audio_uri: str) -> str:
    """'.../audio_raw/{eid}/{eid}__seg000.flac' -> '{eid}'."""
    stem = Path(audio_uri).stem  # '{eid}__seg000'
    return stem.split("__seg", 1)[0]


def framing_for(audio_uri: str, variant: str) -> str:
    """Rendered framing descriptor for a segment's parent call ("" for baseline
    or when the call has no usable record)."""
    if variant == "baseline":
        return ""
    rec = _context_records().get(_example_id_from_uri(audio_uri))
    return framing_renderer.render(rec, variant)


# ---------------------------------------------------------------------------
# Arms
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Arm:
    model: str       # "gemini" | "chirp"
    framing: str     # "baseline" | "moderate"

    @property
    def tag(self) -> str:
        return f"{self.model}_{self.framing}"

    @property
    def pred_field(self) -> str:
        return f"pred_text_{self.tag}"

    @property
    def output_base(self) -> str:
        return f"{TRANSCRIPT_ROOT}/{self.tag}"

    @property
    def batch_input_uri(self) -> str:
        return f"gs://{GCS_BUCKET}/{self.output_base}/vertex_batch_input.jsonl"

    @property
    def batch_output_root(self) -> str:
        return f"gs://{GCS_BUCKET}/{self.output_base}/batch_results/"

    @property
    def predictions_uri(self) -> str:
        return f"gs://{GCS_BUCKET}/{self.output_base}/predictions.jsonl"


def all_arms() -> list[Arm]:
    return [Arm(m, f) for m in ("gemini", "chirp") for f in FRAMING_VARIANTS]


# ---------------------------------------------------------------------------
# GCS helpers
# ---------------------------------------------------------------------------


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    without = uri.removeprefix("gs://")
    bucket, _, path = without.partition("/")
    return bucket, path


def _storage_client():
    from google.cloud import storage

    return storage.Client(project=GCP_PROJECT_ID)


def _read_blob_text(uri: str) -> str | None:
    client = _storage_client()
    bucket, path = _parse_gcs_uri(uri)
    blob = client.bucket(bucket).blob(path)
    return blob.download_as_text() if blob.exists() else None


def _write_blob_text(uri: str, text: str) -> None:
    client = _storage_client()
    bucket, path = _parse_gcs_uri(uri)
    client.bucket(bucket).blob(path).upload_from_string(text)


def _load_jsonl(uri_or_path: str | Path) -> list[dict[str, Any]]:
    if isinstance(uri_or_path, str) and uri_or_path.startswith("gs://"):
        text = _read_blob_text(uri_or_path)
        if text is None:
            raise FileNotFoundError(uri_or_path)
    else:
        text = Path(uri_or_path).read_text(encoding="utf-8")
    return [json.loads(ln) for ln in text.splitlines() if ln.strip()]


def _ensure_iam_binding() -> None:
    """Idempotently grant the Vertex service agent read on the GCS bucket."""
    try:
        proj_num = subprocess.check_output(
            ["gcloud", "projects", "describe", GCP_PROJECT_ID,
             "--format=value(projectNumber)"], text=True,
        ).strip()
    except subprocess.CalledProcessError as e:
        logger.warning(f"project-number lookup failed; skipping IAM grant. {e}")
        return
    member = f"serviceAccount:service-{proj_num}@gcp-sa-aiplatform.iam.gserviceaccount.com"
    try:
        subprocess.run(
            ["gcloud", "storage", "buckets", "add-iam-policy-binding",
             f"gs://{GCS_BUCKET}", f"--member={member}",
             "--role=roles/storage.objectViewer", "--quiet"],
            check=True, capture_output=True, text=True,
        )
        logger.info(f"IAM grant ensured for {member}")
    except subprocess.CalledProcessError as e:
        out = (e.stderr or "") + (e.stdout or "")
        if "already" in out.lower() or "no changes" in out.lower():
            logger.info("IAM binding already exists.")
        else:
            logger.warning(f"IAM grant returned: {e.stderr or e.stdout}")


# ---------------------------------------------------------------------------
# Stage 1a — Gemini (Vertex batch)
# ---------------------------------------------------------------------------


def _gemini_request(arm: Arm, audio_uri: str) -> dict[str, Any]:
    system_prompt = framing_renderer.for_gemini(
        GEMINI_BASE_PROMPT, framing_for(audio_uri, arm.framing)
    )
    return {
        "request": {
            "contents": [{
                "role": "user",
                "parts": [
                    {"file_data": {"file_uri": audio_uri, "mime_type": "audio/flac"}},
                    {"text": USER_TURN_TEXT},
                ],
            }],
            "system_instruction": {"role": "system", "parts": [{"text": system_prompt}]},
            "generation_config": {
                # Production-faithful config (one_hour_pilot winner): frequency_penalty
                # combats the "engine N+1, engine N+2..." enumeration/hallucination loop.
                "temperature": 0.0,
                "max_output_tokens": 512,
                "candidate_count": 1,
                "frequency_penalty": 0.3,
                "presence_penalty": 0.0,
                "top_k": 1,
                "top_p": 0.1,
                "thinking_config": {"include_thoughts": False, "thinking_level": "low"},
            },
            "safety_settings": SAFETY_SETTINGS,
        }
    }


def _uri_from_prediction_line(line: str) -> str | None:
    data = json.loads(line)
    if data.get("status"):
        return None
    parts = data.get("request", {}).get("contents", [{}])[0].get("parts", [])
    for p in parts:
        fd = p.get("file_data") or p.get("fileData")
        if fd:
            return fd.get("file_uri") or fd.get("fileUri")
    return None


def _existing_predicted_uris(arm: Arm) -> set[str]:
    text = _read_blob_text(arm.predictions_uri)
    if text is None:
        return set()
    out: set[str] = set()
    for line in text.splitlines():
        if line.strip():
            uri = _uri_from_prediction_line(line)
            if uri:
                out.add(uri)
    return out


def _gemini_consolidate(arm: Arm) -> int:
    client = _storage_client()
    bucket = client.bucket(GCS_BUCKET)
    prefix = f"{arm.output_base}/batch_results/"
    success: dict[str, str] = {}
    for blob in bucket.list_blobs(prefix=prefix):
        if not blob.name.endswith("predictions.jsonl"):
            continue
        for line in blob.download_as_text().splitlines():
            if not line.strip():
                continue
            uri = _uri_from_prediction_line(line)
            if uri and uri not in success:
                success[uri] = line
    _write_blob_text(arm.predictions_uri, "\n".join(success.values()))
    logger.info(f"[{arm.tag}] consolidated {len(success)} predictions")
    return len(success)


def _gemini_submit(arm: Arm, input_uri: str) -> str:
    from google import genai

    client = genai.Client(vertexai=True, project=GCP_PROJECT_ID, location=GCP_LOCATION)
    job = client.batches.create(
        model=GEMINI_MODEL_ID, src=input_uri, config={"dest": arm.batch_output_root}
    )
    logger.info(f"[{arm.tag}] submitted {job.name}")
    while True:
        job = client.batches.get(name=job.name)
        state = getattr(job.state, "name", str(job.state))
        if "SUCCEEDED" in state or "FAILED" in state or "CANCELLED" in state:
            break
        logger.info(f"[{arm.tag}] state={state}; sleeping 60s")
        time.sleep(60)
    logger.info(f"[{arm.tag}] terminal state: {state}")
    return state


def transcribe_gemini(arm: Arm, rows: list[dict[str, Any]], limit: int | None) -> None:
    skip = _existing_predicted_uris(arm)
    entries, seen = [], 0
    for row in rows:
        uri = row["audio_filepath"]
        if uri in skip:
            continue
        entries.append(json.dumps(_gemini_request(arm, uri)))
        seen += 1
        if limit and seen >= limit:
            break
    if not entries:
        logger.info(f"[{arm.tag}] nothing to submit")
        return
    _write_blob_text(arm.batch_input_uri, "\n".join(entries))
    logger.info(f"[{arm.tag}] wrote {len(entries)} requests")
    state = _gemini_submit(arm, arm.batch_input_uri)
    if "SUCCEEDED" in state:
        _gemini_consolidate(arm)
    else:
        logger.error(f"[{arm.tag}] job ended {state}; no consolidation")


# ---------------------------------------------------------------------------
# Stage 1b — Chirp (Speech-to-Text v2 synchronous recognize)
# ---------------------------------------------------------------------------

_PHRASE_HINTS: list[str] | None = None


def _phrase_hints() -> list[str]:
    """Production phrase-set hints, held constant across arms (not the lever)."""
    global _PHRASE_HINTS
    if _PHRASE_HINTS is None:
        hints_path = (
            SCRIPT_DIR.parents[2]
            / "backend/pipeline/transcription/chirp_phrase_hints.txt"
        )
        lines = hints_path.read_text().splitlines() if hints_path.exists() else []
        _PHRASE_HINTS = [
            ln.strip() for ln in lines if ln.strip() and not ln.startswith("#")
        ]
    return _PHRASE_HINTS


def _chirp_download(audio_uri: str, cache: Path) -> Path | None:
    eid_seg = Path(audio_uri).stem
    dst = cache / f"{eid_seg}.flac"
    if dst.exists() and dst.stat().st_size > 0:
        return dst
    try:
        subprocess.run(["gsutil", "-q", "cp", audio_uri, str(dst)],
                       check=True, capture_output=True)
        return dst
    except subprocess.CalledProcessError as e:
        logger.warning(f"flac download failed {audio_uri}: {e.stderr.decode()[:100]}")
        return None


def _chirp_recognize_one(
    arm: Arm, audio_uri: str, cache: Path
) -> tuple[str, str, bool]:
    """Return (audio_uri, transcript, ok). ok=False marks a HARD failure
    (download/API error) so the caller can avoid persisting it as a spurious
    empty — distinct from a genuine empty transcript (ok=True, text="")."""
    from google.cloud import speech_v2 as cloud_speech
    from google.cloud.speech_v2 import SpeechClient

    path = _chirp_download(audio_uri, cache)
    if path is None:
        return audio_uri, "", False
    audio_data = path.read_bytes()

    custom_prompt = framing_renderer.for_chirp(
        CHIRP_BASE_PROMPT, framing_for(audio_uri, arm.framing)
    )
    client = SpeechClient(
        client_options={"api_endpoint": f"{CHIRP_LOCATION}-speech.googleapis.com"}
    )
    adaptation = cloud_speech.SpeechAdaptation(
        phrase_sets=[cloud_speech.SpeechAdaptation.AdaptationPhraseSet(
            inline_phrase_set=cloud_speech.PhraseSet(
                phrases=[cloud_speech.PhraseSet.Phrase(value=v) for v in _phrase_hints()]
            )
        )]
    ) if _phrase_hints() else None
    request = cloud_speech.RecognizeRequest(
        recognizer=SpeechClient.recognizer_path(
            GCP_PROJECT_ID, CHIRP_LOCATION, CHIRP_RECOGNIZER
        ),
        config=cloud_speech.RecognitionConfig(
            auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
            model=CHIRP_MODEL,
            language_codes=CHIRP_LANGUAGES,
            adaptation=adaptation,
            features=cloud_speech.RecognitionFeatures(
                enable_automatic_punctuation=True,
                custom_prompt_config=cloud_speech.CustomPromptConfig(
                    custom_prompt=custom_prompt
                ) if custom_prompt else None,
            ),
        ),
        content=audio_data,
    )
    # STT v2 enforces a per-minute Recognize quota; under concurrency we hit
    # 429s. Retry with exponential backoff so throttling never silently becomes
    # an empty transcript (which would look like a catastrophic framing effect).
    for attempt in range(6):
        try:
            resp = client.recognize(request=request)
            text = " ".join(
                r.alternatives[0].transcript for r in resp.results if r.alternatives
            ).strip()
            return audio_uri, text, True
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg or "RESOURCE_EXHAUSTED" in msg:
                time.sleep(min(2 ** attempt + 1, 45))
                continue
            logger.warning(f"[{arm.tag}] recognize failed {audio_uri}: {e}")
            return audio_uri, "", False
    logger.warning(f"[{arm.tag}] recognize gave up (429) {audio_uri}")
    return audio_uri, "", False


def transcribe_chirp(arm: Arm, rows: list[dict[str, Any]], limit: int | None) -> None:
    cache = FLAC_CACHE
    cache.mkdir(parents=True, exist_ok=True)
    skip = _existing_predicted_uris(arm)
    todo = [r["audio_filepath"] for r in rows if r["audio_filepath"] not in skip]
    if limit:
        todo = todo[:limit]
    if not todo:
        logger.info(f"[{arm.tag}] nothing to transcribe")
        return
    logger.info(f"[{arm.tag}] recognizing {len(todo)} segments")
    results: dict[str, str] = {}
    failed = 0
    # 5 workers + in-call 429 backoff keeps us under the STT per-minute quota.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(_chirp_recognize_one, arm, uri, cache): uri for uri in todo}
        for i, fut in enumerate(concurrent.futures.as_completed(futs), 1):
            uri, text, ok = fut.result()
            if ok:
                results[uri] = text  # persist only successes (incl. genuine empties)
            else:
                failed += 1
            if i % 50 == 0:
                logger.info(f"[{arm.tag}] {i}/{len(todo)} ({failed} hard-failed)")
    if failed:
        logger.warning(f"[{arm.tag}] {failed} segments hard-failed; left unpersisted "
                       f"(re-run to retry them)")
    # Persist in the same shape Gemini uses (one JSON object per line keyed by uri).
    existing = _read_blob_text(arm.predictions_uri) or ""
    lines = [ln for ln in existing.splitlines() if ln.strip()]
    for uri, text in results.items():
        lines.append(json.dumps({
            "request": {"contents": [{"parts": [{"file_data": {"file_uri": uri}}]}]},
            "response": {"candidates": [{"content": {"parts": [{"text": text}]}}]},
        }))
    _write_blob_text(arm.predictions_uri, "\n".join(lines))
    logger.info(f"[{arm.tag}] wrote {len(results)} predictions")


def stage_transcribe(limit: int | None, arms: list[Arm]) -> None:
    rows = _load_jsonl(MANIFEST_URI)
    logger.info(f"manifest rows: {len(rows)} (limit={limit})")
    gemini_arms = [a for a in arms if a.model == "gemini"]
    chirp_arms = [a for a in arms if a.model == "chirp"]
    if gemini_arms:
        _ensure_iam_binding()
    # Gemini arms run in parallel (batch jobs); Chirp arms run sequentially
    # (each already fans out internally over a thread pool).
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(gemini_arms))) as ex:
        futs = {ex.submit(transcribe_gemini, a, rows, limit): a for a in gemini_arms}
        for fut in concurrent.futures.as_completed(futs):
            arm = futs[fut]
            try:
                fut.result()
            except Exception:
                logger.exception(f"[{arm.tag}] gemini transcribe failed")
    for arm in chirp_arms:
        try:
            transcribe_chirp(arm, rows, limit)
        except Exception:
            logger.exception(f"[{arm.tag}] chirp transcribe failed")


# ---------------------------------------------------------------------------
# Stage 2 — Merge
# ---------------------------------------------------------------------------


def _pred_map(arm: Arm) -> dict[tuple[str, str], str]:
    text = _read_blob_text(arm.predictions_uri)
    if text is None:
        logger.warning(f"[{arm.tag}] no predictions at {arm.predictions_uri}")
        return {}
    out: dict[tuple[str, str], str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        if data.get("status"):
            continue
        uri = _uri_from_prediction_line(line)
        if not uri or "__seg" not in Path(uri).stem:
            continue
        eid, seg = Path(uri).stem.split("__seg", 1)
        pred = (
            data.get("response", {}).get("candidates", [{}])[0]
            .get("content", {}).get("parts", [{}])[0].get("text", "") or ""
        ).strip()
        out[(eid, seg)] = pred
    logger.info(f"[{arm.tag}] indexed {len(out)} predictions")
    return out


def stage_merge(arms: list[Arm]) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    rows = _load_jsonl(MANIFEST_URI)
    pmaps = {a.tag: _pred_map(a) for a in arms}
    attempted: set[tuple[str, str]] = set()
    for pm in pmaps.values():
        attempted.update(pm.keys())

    merged = []
    for row in rows:
        key = (row["example_id"], str(row["segment_id"]))
        if key not in attempted:
            continue
        out = dict(row)
        for a in arms:
            out[a.pred_field] = pmaps[a.tag].get(key, "")
        merged.append(out)
    (RESULTS_DIR / "merged.jsonl").write_text(
        "\n".join(json.dumps(r) for r in merged) + "\n", encoding="utf-8"
    )
    logger.info(f"wrote {RESULTS_DIR / 'merged.jsonl'} ({len(merged)} rows)")
    _write_blob_text(MERGED_GCS_URI, "\n".join(json.dumps(r) for r in merged))


# ---------------------------------------------------------------------------
# Stage 3 — Score (normalizer reused from echo_eval; gates in decision_gates)
# ---------------------------------------------------------------------------

_NEMO = None


def _nemo():
    global _NEMO
    if _NEMO is None:
        from nemo_text_processing.text_normalization.normalize import Normalizer
        _NEMO = Normalizer(input_case="cased", lang="en")
    return _NEMO


def _build_normalizer():
    import jiwer

    class NemoNormalization(jiwer.AbstractTransform):
        def process_string(self, s: str) -> str:
            return _nemo().normalize(s, verbose=False)

    class NormalizeDispatchQuirks(jiwer.AbstractTransform):
        def process_string(self, s: str) -> str:
            s = re.sub(r"\d{10,}", " [hallucination] ", s)
            s = re.sub(r"\b(uh|um|ah|er)\b", "", s, flags=re.IGNORECASE)
            return s.replace("-", " ")

    return jiwer.Compose([
        NormalizeDispatchQuirks(), NemoNormalization(),
        jiwer.SubstituteRegexes({r"[\n\r\t]+": " "}),
        jiwer.ToLowerCase(), jiwer.RemovePunctuation(),
        jiwer.RemoveWhiteSpace(replace_by_space=True),
        jiwer.RemoveMultipleSpaces(), jiwer.Strip(),
    ])


def stage_score(arms: list[Arm]) -> None:
    import jiwer

    merged_path = RESULTS_DIR / "merged.jsonl"
    if not merged_path.exists():
        raise FileNotFoundError(f"missing {merged_path}; run `merge` first")
    rows = _load_jsonl(merged_path)
    normalizer = _build_normalizer()
    records = _context_records()

    # Per-segment normalized GT + per-arm WER; only score rows with GT text.
    scored: list[dict[str, Any]] = []
    for row in rows:
        gt = normalizer(row.get("text", "") or "")
        if not gt:
            continue
        eid = row["example_id"]
        rec = records.get(eid)
        framed = bool(framing_renderer.render(rec, "moderate"))
        entry = {
            "example_id": eid,
            "gt": gt,
            "gt_words": len(gt.split()),
            "framed": framed,
            "system": (rec or {}).get("sName") or "unknown",
        }
        gt_set = set(gt.split())
        for a in arms:
            raw = row.get(a.pred_field, "") or ""
            pred = normalizer(raw) if raw else ""
            wer = 100 * jiwer.process_words(gt, pred or "").wer
            entry[f"pred_{a.tag}"] = pred
            entry[f"wer_{a.tag}"] = round(wer, 2)
            entry[f"wer_capped_{a.tag}"] = round(min(100.0, wer), 2)
            entry[f"pred_words_{a.tag}"] = len(pred.split())
            # over-insertion: predicted tokens absent from GT
            entry[f"ins_{a.tag}"] = sum(1 for w in pred.split() if w not in gt_set)
        scored.append(entry)

    report = decision_gates.analyze(scored, arms=[a.tag for a in arms],
                                    framing_variants=list(FRAMING_VARIANTS))
    (RESULTS_DIR / "eval_per_sample.jsonl").write_text(
        "\n".join(json.dumps(r, sort_keys=True) for r in scored) + "\n"
    )
    (RESULTS_DIR / "decision_report.json").write_text(json.dumps(report, indent=2))
    md = decision_gates.render_markdown(report)
    (RESULTS_DIR / "decision_report.md").write_text(md)
    logger.info(f"wrote scoring artifacts to {RESULTS_DIR}")
    print("\n" + md)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _setup_logging() -> None:
    logger.remove()
    logger.add(sys.stderr, format="<level>{level}</level>: {message}")


def _parse_arms(spec: str | None) -> list[Arm]:
    if not spec:
        return all_arms()
    models = {m.strip() for m in spec.split(",") if m.strip()}
    return [a for a in all_arms() if a.model in models]


def main() -> None:
    _setup_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("transcribe", "all"):
        sp = sub.add_parser(name)
        sp.add_argument("--limit", type=int, default=None)
        sp.add_argument("--arms", default=None, help="comma list: gemini,chirp")
    for name in ("merge", "score"):
        sp = sub.add_parser(name)
        sp.add_argument("--arms", default=None)
    args = parser.parse_args()
    arms = _parse_arms(getattr(args, "arms", None))

    if args.cmd == "transcribe":
        stage_transcribe(args.limit, arms)
    elif args.cmd == "merge":
        stage_merge(arms)
    elif args.cmd == "score":
        stage_score(arms)
    elif args.cmd == "all":
        stage_transcribe(args.limit, arms)
        stage_merge(arms)
        stage_score(arms)


if __name__ == "__main__":
    main()
