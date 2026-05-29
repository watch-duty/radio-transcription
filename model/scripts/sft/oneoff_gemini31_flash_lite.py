"""Prepare one-off Gemini 3.1 Flash-Lite SFT experiment artifacts.

This script stages data and writes configs for the four planned tuning runs. It
does not submit Vertex tuning jobs.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import random
import re
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Final

REPO_ROOT: Final = Path(__file__).resolve().parents[3]
SFT_DIR: Final = Path(__file__).resolve().parent
RESULTS_DIR: Final = SFT_DIR / "results"
MODEL_COLABS_DIR: Final = REPO_ROOT / "model" / "colabs"

sys.path.insert(0, str(MODEL_COLABS_DIR))
sys.path.insert(0, str(SFT_DIR))

from common.gcs_utils import parse_gcs_uri  # noqa: E402
from common.sft import build_example, validate_example  # noqa: E402
from pipeline import (  # noqa: E402
    DEFAULT_BASE_MODEL,
    DEFAULT_GCP_PROJECT,
    SFT_MODEL_DISPLAY_NAMES,
    SFT_TRAINING_COST_PER_MILLION,
)
from prompts import PIPELINE_SYSTEM_PROMPT, PIPELINE_USER_PROMPT  # noqa: E402

logger = logging.getLogger(__name__)

SEED: Final = 20260529
HF_DATASET_ID: Final = "jacktol/ATC-ASR-Dataset"
GCP_PROJECT: Final = DEFAULT_GCP_PROJECT
GCS_BUCKET: Final = "wd-transcription-data"
GCS_PREFIX: Final = (
    "gs://wd-transcription-data/sft/oneoff_runs/"
    "2026-05-29-gemini31-flash-lite-public-atc"
)
WORKSPACE_DIR: Final = RESULTS_DIR / "2026-05-29-gemini31-flash-lite-oneoff"
SELECTION_DIR: Final = WORKSPACE_DIR / "selection"
STAGING_DIR: Final = WORKSPACE_DIR / "staging"
CONFIG_DIR: Final = WORKSPACE_DIR / "configs"

WD_VERSION: Final = "radio-transcription-sft-v20260528"
WD_GEMINI_TRAIN_URI: Final = (
    "gs://wd-transcription-data/sft/dataset_versions/"
    f"{WD_VERSION}/model_inputs/gemini/train.jsonl"
)
WD_GEMINI_EVAL_URI: Final = (
    "gs://wd-transcription-data/sft/dataset_versions/"
    f"{WD_VERSION}/model_inputs/gemini/eval.jsonl"
)
WD_CANONICAL_TRAIN_URI: Final = (
    "gs://wd-transcription-data/sft/dataset_versions/"
    f"{WD_VERSION}/manifests/canonical/train.jsonl"
)
WD_CANONICAL_EVAL_URI: Final = (
    "gs://wd-transcription-data/sft/dataset_versions/"
    f"{WD_VERSION}/manifests/canonical/eval.jsonl"
)

PUBLIC_SELECTED_JSONL: Final = SELECTION_DIR / "public_atc_selected.jsonl"
PUBLIC_REPORT_JSON: Final = SELECTION_DIR / "public_atc_selection_report.json"
PUBLIC_GEMINI_JSONL: Final = STAGING_DIR / "public_atc_gemini_train.jsonl"
PUBLIC_MANIFEST_JSONL: Final = STAGING_DIR / "public_atc_manifest.jsonl"
BLENDED_GEMINI_JSONL: Final = STAGING_DIR / "wd_plus_public_atc_train.jsonl"
READINESS_REPORT_JSON: Final = WORKSPACE_DIR / "readiness_report.json"
SELECTION_AUDIT_REPORT_JSON: Final = (
    WORKSPACE_DIR / "selection_audit_report.json"
)
PREFLIGHT_SUMMARY_JSON: Final = WORKSPACE_DIR / "preflight_summary.json"
COMMANDS_MD: Final = WORKSPACE_DIR / "commands.md"

PUBLIC_GCS_MANIFEST_URI: Final = (
    f"{GCS_PREFIX}/public_atc/selected_manifest.jsonl"
)
PUBLIC_GCS_GEMINI_URI: Final = f"{GCS_PREFIX}/public_atc/gemini_train.jsonl"
BLENDED_GCS_TRAIN_URI: Final = f"{GCS_PREFIX}/blended/gemini_train.jsonl"

TARGET_SELECTED: Final = 2000
TARGETS: Final = {
    "word_0_3": 350,
    "word_4_10": 1150,
    "word_11_14": 400,
    "word_15_25": 100,
    "numeric_any": 1600,
    "numeric_phrase_context": 1540,
    "number_only": 60,
    "phonetic_any": 500,
    "numeric_and_phonetic": 400,
}

HARD_ATC_TERMS: Final = (
    "runway",
    "taxi",
    "takeoff",
    "landing",
    "flight level",
    "squawk",
    "qnh",
    "ils",
    "atis",
    "glideslope",
    "localizer",
)
SOFT_ATC_TERMS: Final = (
    "heading",
    "climb",
    "descend",
    "approach",
    "departure",
    "tower",
    "ground",
    "cleared",
    "knots",
    "altitude",
    "maintain",
    "direct",
    "contact",
    "radar",
    "turn",
    "proceed",
    "hold short",
    "line up",
    "wind",
    "degrees",
    "decimal",
    "lufthansa",
    "speedbird",
    "aeroflot",
    "turkish",
    "austrian",
    "shamrock",
    "ascot",
    "sunturk",
    "ethiopian",
    "csa",
)
PHONETIC_WORDS: Final = frozenset(
    {
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliett",
        "juliet",
        "kilo",
        "lima",
        "mike",
        "november",
        "oscar",
        "papa",
        "quebec",
        "romeo",
        "sierra",
        "tango",
        "uniform",
        "victor",
        "whiskey",
        "xray",
        "x-ray",
        "yankee",
        "zulu",
    }
)
DIGIT_WORDS: Final = {
    "zero": "0",
    "oh": "0",
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8",
    "nine": "9",
    "niner": "9",
}


@dataclass(frozen=True)
class PublicCandidate:
    row_id: str
    split: str
    source_file: str
    row_index: int
    text: str
    normalized_text: str
    word_count: int
    word_bucket: str
    numeric_any: bool
    numeric_phrase_context: bool
    number_only: bool
    phonetic_any: bool
    numeric_and_phonetic: bool
    hard_atc: bool
    soft_atc_count: int
    similarity_score: int = 0


@dataclass(frozen=True)
class RunSpec:
    round_id: str
    variant: str
    train_uri: str
    adapter_size: str
    epochs: int = 5
    lr_multiplier: float = 1.0


RUN_SPECS: Final = (
    RunSpec(
        round_id="2026-05-29-wd-internal-a4-gemini31-flash-lite",
        variant="wd_internal",
        train_uri=WD_GEMINI_TRAIN_URI,
        adapter_size="FOUR",
    ),
    RunSpec(
        round_id="2026-05-29-wd-internal-a8-gemini31-flash-lite",
        variant="wd_internal",
        train_uri=WD_GEMINI_TRAIN_URI,
        adapter_size="EIGHT",
    ),
    RunSpec(
        round_id="2026-05-29-wd-atc-a4-gemini31-flash-lite",
        variant="wd_internal_plus_atc",
        train_uri=BLENDED_GCS_TRAIN_URI,
        adapter_size="FOUR",
    ),
    RunSpec(
        round_id="2026-05-29-wd-atc-a8-gemini31-flash-lite",
        variant="wd_internal_plus_atc",
        train_uri=BLENDED_GCS_TRAIN_URI,
        adapter_size="EIGHT",
    ),
)


def _ensure_dirs() -> None:
    for path in (SELECTION_DIR, STAGING_DIR, CONFIG_DIR):
        path.mkdir(parents=True, exist_ok=True)


def _tokens(text: str) -> list[str]:
    return re.findall(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)?", text.lower())


def normalize_public_label(text: str) -> str:
    tokens = _tokens(text)
    out: list[str] = []
    digit_run = ""
    for token in tokens:
        digit = DIGIT_WORDS.get(token)
        if digit is not None:
            digit_run += digit
            continue
        if digit_run:
            out.append(digit_run)
            digit_run = ""
        out.append(token)
    if digit_run:
        out.append(digit_run)
    return " ".join(out)


def _contains_term(text: str, term: str) -> bool:
    return bool(re.search(rf"\b{re.escape(term)}\b", text, flags=re.IGNORECASE))


def _word_bucket(word_count: int) -> str:
    if word_count <= 3:
        return "word_0_3"
    if word_count <= 10:
        return "word_4_10"
    if word_count <= 14:
        return "word_11_14"
    if word_count <= 25:
        return "word_15_25"
    return "word_26_plus"


def build_candidate(
    *,
    row_id: str,
    split: str,
    source_file: str,
    row_index: int,
    text: str,
) -> PublicCandidate:
    clean_text = " ".join(str(text).split())
    tokens = _tokens(clean_text)
    normalized = normalize_public_label(clean_text)
    lower_text = clean_text.lower()
    numeric_any = any(
        t in DIGIT_WORDS or any(ch.isdigit() for ch in t) for t in tokens
    )
    phonetic_any = any(t in PHONETIC_WORDS for t in tokens)
    number_only = bool(tokens) and all(
        t in DIGIT_WORDS or t.isdigit() for t in tokens
    )
    hard_atc = any(_contains_term(lower_text, term) for term in HARD_ATC_TERMS)
    soft_atc_count = sum(
        1 for term in SOFT_ATC_TERMS if _contains_term(lower_text, term)
    )
    return PublicCandidate(
        row_id=str(row_id),
        split=split,
        source_file=source_file,
        row_index=row_index,
        text=clean_text,
        normalized_text=normalized,
        word_count=len(tokens),
        word_bucket=_word_bucket(len(tokens)),
        numeric_any=numeric_any,
        numeric_phrase_context=numeric_any and not number_only,
        number_only=number_only,
        phonetic_any=phonetic_any,
        numeric_and_phonetic=numeric_any and phonetic_any,
        hard_atc=hard_atc,
        soft_atc_count=soft_atc_count,
    )


def _candidate_for_objective(candidate: PublicCandidate) -> int:
    tie = int(hashlib.sha1(candidate.row_id.encode()).hexdigest()[:6], 16) % 50
    return candidate.soft_atc_count * 10_000 - candidate.similarity_score + tie


def _public_split_from_path(path: str) -> str:
    name = Path(path).name
    return name.split("-", 1)[0]


def _hf_parquet_files() -> list[str]:
    from huggingface_hub import HfApi

    api = HfApi()
    return sorted(
        path
        for path in api.list_repo_files(HF_DATASET_ID, repo_type="dataset")
        if path.endswith(".parquet")
    )


def _load_public_candidates() -> list[PublicCandidate]:
    import pyarrow.parquet as pq
    from huggingface_hub import hf_hub_download

    candidates: list[PublicCandidate] = []
    for repo_file in _hf_parquet_files():
        local_file = hf_hub_download(
            HF_DATASET_ID, repo_file, repo_type="dataset"
        )
        table = pq.read_table(local_file, columns=["id", "text"])
        split = _public_split_from_path(repo_file)
        for row_index, row in enumerate(table.to_pylist()):
            candidates.append(
                build_candidate(
                    row_id=str(row["id"]),
                    split=split,
                    source_file=repo_file,
                    row_index=row_index,
                    text=str(row["text"]),
                )
            )
    return sorted(
        candidates, key=lambda c: (c.source_file, c.row_index, c.row_id)
    )


def _storage_client() -> Any:
    from google.cloud import storage

    return storage.Client(project=GCP_PROJECT)


def _download_gcs_text(storage_client: Any, gcs_uri: str) -> str:
    from google.cloud.storage.retry import DEFAULT_RETRY

    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    return (
        storage_client.bucket(bucket_name)
        .blob(blob_path)
        .download_as_text(retry=DEFAULT_RETRY)
    )


def _upload_file(storage_client: Any, local_path: Path, gcs_uri: str) -> None:
    from google.cloud.storage.retry import DEFAULT_RETRY

    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    storage_client.bucket(bucket_name).blob(blob_path).upload_from_filename(
        str(local_path), retry=DEFAULT_RETRY
    )
    logger.debug("Uploaded %s -> %s", local_path, gcs_uri)


def _upload_text(storage_client: Any, text: str, gcs_uri: str) -> None:
    from google.cloud.storage.retry import DEFAULT_RETRY

    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    storage_client.bucket(bucket_name).blob(blob_path).upload_from_string(
        text, content_type="application/json", retry=DEFAULT_RETRY
    )
    logger.info("Uploaded text -> %s", gcs_uri)


def _blob_exists(storage_client: Any, gcs_uri: str) -> bool:
    from google.cloud.storage.retry import DEFAULT_RETRY

    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    return (
        storage_client.bucket(bucket_name)
        .blob(blob_path)
        .exists(retry=DEFAULT_RETRY, timeout=30)
    )


def _model_text_from_sft(example: dict[str, Any]) -> str:
    contents = example.get("contents", [])
    if not isinstance(contents, list) or len(contents) < 2:
        return ""
    model_turn = contents[1]
    if not isinstance(model_turn, dict):
        return ""
    parts = model_turn.get("parts", [])
    if not isinstance(parts, list) or not parts:
        return ""
    first = parts[0]
    if not isinstance(first, dict):
        return ""
    text = first.get("text", "")
    return text if isinstance(text, str) else ""


def _audio_uri_from_sft(example: dict[str, Any]) -> str:
    contents = example.get("contents", [])
    if not isinstance(contents, list) or not contents:
        return ""
    user_turn = contents[0]
    if not isinstance(user_turn, dict):
        return ""
    parts = user_turn.get("parts", [])
    if not isinstance(parts, list):
        return ""
    for part in parts:
        if not isinstance(part, dict):
            continue
        file_data = part.get("fileData") or part.get("file_data")
        if isinstance(file_data, dict):
            uri = file_data.get("fileUri") or file_data.get("file_uri")
            if isinstance(uri, str):
                return uri
    return ""


def _iter_jsonl_text(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _sum_manifest_duration(storage_client: Any, gcs_uri: str) -> float:
    rows = _iter_jsonl_text(_download_gcs_text(storage_client, gcs_uri))
    return float(sum(float(row.get("duration") or 0.0) for row in rows))


def _compute_similarity_scores(
    candidates: list[PublicCandidate],
) -> dict[str, int]:
    try:
        import numpy as np
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import linear_kernel
    except ImportError:
        logger.warning(
            "scikit-learn missing; selector similarity scores set to zero"
        )
        return {}

    storage_client = _storage_client()
    wd_texts: list[str] = []
    for uri in (WD_GEMINI_TRAIN_URI, WD_GEMINI_EVAL_URI):
        for row in _iter_jsonl_text(_download_gcs_text(storage_client, uri)):
            label = _model_text_from_sft(row)
            if re.search(r"\d", label) or any(
                t in DIGIT_WORDS for t in _tokens(label)
            ):
                wd_texts.append(label.lower())
    if not wd_texts:
        logger.warning(
            "No numeric WD texts found; selector similarity scores set to zero"
        )
        return {}

    public_texts = [candidate.normalized_text for candidate in candidates]
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=2)
    matrix = vectorizer.fit_transform(wd_texts + public_texts)
    wd_centroid = np.asarray(matrix[: len(wd_texts)].mean(axis=0))
    similarities = linear_kernel(matrix[len(wd_texts) :], wd_centroid).ravel()
    return {
        candidate.row_id: round(float(score) * 1000)
        for candidate, score in zip(candidates, similarities, strict=True)
    }


def _feature_value(candidate: PublicCandidate, feature: str) -> bool:
    if feature.startswith("word_"):
        return candidate.word_bucket == feature
    return bool(getattr(candidate, feature))


def _select_with_solver(
    candidates: list[PublicCandidate],
) -> tuple[list[PublicCandidate], str, float]:
    from ortools.sat.python import cp_model

    feasible = [
        c
        for c in candidates
        if not c.hard_atc and c.word_bucket != "word_26_plus"
    ]
    model = cp_model.CpModel()
    variables = [model.NewBoolVar(f"x_{i}") for i, _ in enumerate(feasible)]
    model.Add(sum(variables) == TARGET_SELECTED)
    for feature, target in TARGETS.items():
        model.Add(
            sum(
                var
                for var, candidate in zip(variables, feasible, strict=True)
                if _feature_value(candidate, feature)
            )
            == target
        )
    model.Minimize(
        sum(
            var * _candidate_for_objective(candidate)
            for var, candidate in zip(variables, feasible, strict=True)
        )
    )

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 300.0
    solver.parameters.num_search_workers = 8
    status = solver.Solve(model)
    status_name = solver.StatusName(status)
    if status not in {cp_model.OPTIMAL, cp_model.FEASIBLE}:
        raise RuntimeError(
            f"Public ATC selector failed with status {status_name}"
        )

    selected = [
        candidate
        for var, candidate in zip(variables, feasible, strict=True)
        if solver.BooleanValue(var)
    ]
    logger.info("Selector status: %s", status_name)
    return selected, status_name, float(solver.ObjectiveValue())


def _distribution(candidates: list[PublicCandidate]) -> dict[str, Any]:
    dist: dict[str, Any] = {
        "rows": len(candidates),
        "splits": {},
        "soft_atc_rows": 0,
        "soft_atc_term_hits": 0,
    }
    for feature in [*TARGETS, "word_26_plus"]:
        dist[feature] = 0
    for candidate in candidates:
        dist["splits"][candidate.split] = (
            dist["splits"].get(candidate.split, 0) + 1
        )
        for feature in [*TARGETS, "word_26_plus"]:
            if _feature_value(candidate, feature):
                dist[feature] += 1
        if candidate.soft_atc_count:
            dist["soft_atc_rows"] += 1
            dist["soft_atc_term_hits"] += candidate.soft_atc_count
    return dist


def select_public(args: argparse.Namespace) -> int:
    _ensure_dirs()
    candidates = _load_public_candidates()
    filtered_count = sum(
        1
        for c in candidates
        if not c.hard_atc and c.word_bucket != "word_26_plus"
    )
    if not args.skip_similarity:
        scores = _compute_similarity_scores(candidates)
        candidates = [
            PublicCandidate(
                **{**asdict(c), "similarity_score": scores.get(c.row_id, 0)}
            )
            for c in candidates
        ]

    selected, status_name, objective_value = _select_with_solver(candidates)
    rows = [asdict(candidate) for candidate in selected]
    _write_jsonl(PUBLIC_SELECTED_JSONL, rows)

    report = {
        "dataset": HF_DATASET_ID,
        "license_metadata": None,
        "seed": SEED,
        "candidate_rows": len(candidates),
        "candidate_rows_after_hard_filters": filtered_count,
        "selected_rows": len(selected),
        "solver_status": status_name,
        "objective_value": objective_value,
        "targets": TARGETS,
        "selected_distribution": _distribution(selected),
        "all_candidate_distribution": _distribution(candidates),
        "hard_atc_terms": HARD_ATC_TERMS,
        "soft_atc_terms": SOFT_ATC_TERMS,
        "selected_manifest": str(PUBLIC_SELECTED_JSONL.relative_to(REPO_ROOT)),
    }
    PUBLIC_REPORT_JSON.write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    logger.info("Wrote %s and %s", PUBLIC_SELECTED_JSONL, PUBLIC_REPORT_JSON)
    return 0


def _selection_objective(candidates: list[PublicCandidate]) -> int:
    return sum(_candidate_for_objective(candidate) for candidate in candidates)


def _target_deltas(distribution: dict[str, Any]) -> dict[str, int]:
    return {
        feature: int(distribution.get(feature, 0)) - target
        for feature, target in TARGETS.items()
    }


def _duration_bucket(duration: float) -> str:
    if duration < 3:
        return "duration_0_3"
    if duration < 7:
        return "duration_3_7"
    if duration < 15:
        return "duration_7_15"
    if duration < 30:
        return "duration_15_30"
    return "duration_30_plus"


def _text_distribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [
        build_candidate(
            row_id=str(i),
            split=str(row.get("split", "unknown")),
            source_file=str(row.get("source_file", "unknown")),
            row_index=i,
            text=str(row.get("text", "")),
        )
        for i, row in enumerate(rows)
    ]
    distribution = _distribution(candidates)
    duration_buckets = {
        "duration_0_3": 0,
        "duration_3_7": 0,
        "duration_7_15": 0,
        "duration_15_30": 0,
        "duration_30_plus": 0,
    }
    durations: list[float] = []
    for row in rows:
        duration = float(row.get("duration") or 0.0)
        durations.append(duration)
        duration_buckets[_duration_bucket(duration)] += 1
    distribution["duration_buckets"] = duration_buckets
    distribution["total_duration_seconds"] = round(sum(durations), 3)
    distribution["mean_duration_seconds"] = (
        round(sum(durations) / len(durations), 3) if durations else 0.0
    )
    return distribution


def _manifest_distribution(storage_client: Any, gcs_uri: str) -> dict[str, Any]:
    rows = _iter_jsonl_text(_download_gcs_text(storage_client, gcs_uri))
    return _text_distribution(rows)


def audit_selection(args: argparse.Namespace) -> int:
    del args
    _ensure_dirs()
    if not PUBLIC_SELECTED_JSONL.exists():
        raise FileNotFoundError(f"Run select first: {PUBLIC_SELECTED_JSONL}")
    if not PUBLIC_MANIFEST_JSONL.exists():
        raise FileNotFoundError(
            f"Run stage-public first: {PUBLIC_MANIFEST_JSONL}"
        )

    candidates = _load_public_candidates()
    scores = _compute_similarity_scores(candidates)
    candidates = [
        PublicCandidate(
            **{
                **asdict(candidate),
                "similarity_score": scores.get(candidate.row_id, 0),
            }
        )
        for candidate in candidates
    ]
    selected_ids = {
        str(row["row_id"]) for row in _read_jsonl(PUBLIC_SELECTED_JSONL)
    }
    selected = [
        candidate
        for candidate in candidates
        if candidate.row_id in selected_ids
    ]
    optimal_selected, status_name, optimal_objective = _select_with_solver(
        candidates
    )
    selected_objective = float(_selection_objective(selected))
    optimal_ids = {candidate.row_id for candidate in optimal_selected}
    selected_distribution = _distribution(selected)

    storage_client = _storage_client()
    public_manifest = _read_jsonl(PUBLIC_MANIFEST_JSONL)
    report = {
        "best_under_current_objective": status_name == "OPTIMAL"
        and selected_distribution.get("rows") == TARGET_SELECTED
        and all(
            delta == 0
            for delta in _target_deltas(selected_distribution).values()
        )
        and abs(selected_objective - optimal_objective) < 0.001,
        "solver_status": status_name,
        "selected_rows": len(selected),
        "optimal_rows": len(optimal_selected),
        "current_selected_objective": selected_objective,
        "optimal_objective": optimal_objective,
        "objective_delta": selected_objective - optimal_objective,
        "selected_ids_matching_resolved_optimum": len(
            selected_ids & optimal_ids
        ),
        "candidate_rows": len(candidates),
        "feasible_rows_after_hard_filters": sum(
            1
            for candidate in candidates
            if not candidate.hard_atc
            and candidate.word_bucket != "word_26_plus"
        ),
        "selected_distribution": selected_distribution,
        "target_deltas": _target_deltas(selected_distribution),
        "selected_similarity_score_mean": round(
            sum(candidate.similarity_score for candidate in selected)
            / len(selected),
            3,
        ),
        "candidate_similarity_score_mean": round(
            sum(candidate.similarity_score for candidate in candidates)
            / len(candidates),
            3,
        ),
        "wd_train_distribution": _manifest_distribution(
            storage_client, WD_CANONICAL_TRAIN_URI
        ),
        "wd_eval_distribution": _manifest_distribution(
            storage_client, WD_CANONICAL_EVAL_URI
        ),
        "public_selected_manifest_distribution": _text_distribution(
            public_manifest
        ),
        "interpretation": [
            "The selected rows are optimal for the locked constrained objective.",
            "This does not prove the objective is globally best for Watch Duty WER; the tuning runs test that transfer hypothesis.",
            "The selected slice intentionally over-represents numeric and phonetic structures relative to WD to test targeted transfer.",
        ],
    }
    SELECTION_AUDIT_REPORT_JSON.write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    logger.info(
        "Selection best under current objective: %s",
        report["best_under_current_objective"],
    )
    logger.info("Wrote %s", SELECTION_AUDIT_REPORT_JSON)
    return 0 if report["best_under_current_objective"] else 1


def _selected_by_file() -> dict[str, dict[str, dict[str, Any]]]:
    selected = _read_jsonl(PUBLIC_SELECTED_JSONL)
    by_file: dict[str, dict[str, dict[str, Any]]] = {}
    for row in selected:
        by_file.setdefault(row["source_file"], {})[row["row_id"]] = row
    return by_file


def _duration_seconds(audio_bytes: bytes) -> tuple[float, int, Any]:
    import soundfile as sf

    audio, sample_rate = sf.read(io.BytesIO(audio_bytes), dtype="float32")
    frames = len(audio)
    return frames / float(sample_rate), int(sample_rate), audio


def stage_public(args: argparse.Namespace) -> int:
    del args
    _ensure_dirs()
    if not PUBLIC_SELECTED_JSONL.exists():
        raise FileNotFoundError(f"Run select first: {PUBLIC_SELECTED_JSONL}")

    import pyarrow.parquet as pq
    import soundfile as sf
    from huggingface_hub import hf_hub_download

    storage_client = _storage_client()
    by_file = _selected_by_file()
    manifest_rows: list[dict[str, Any]] = []
    sft_rows: list[dict[str, Any]] = []
    audio_dir = STAGING_DIR / "public_atc_audio"
    audio_dir.mkdir(parents=True, exist_ok=True)

    for repo_file, selected_by_id in by_file.items():
        local_file = hf_hub_download(
            HF_DATASET_ID, repo_file, repo_type="dataset"
        )
        table = pq.read_table(local_file, columns=["id", "audio", "text"])
        for row in table.to_pylist():
            row_id = str(row["id"])
            selected = selected_by_id.get(row_id)
            if selected is None:
                continue
            audio_bytes = row["audio"]["bytes"]
            duration, sample_rate, audio = _duration_seconds(audio_bytes)
            if sample_rate != 16000:
                raise ValueError(
                    f"{row_id} has unsupported sample rate {sample_rate}"
                )
            audio_path = audio_dir / f"{row_id}.flac"
            sf.write(audio_path, audio, sample_rate, format="FLAC")
            audio_gcs_uri = f"{GCS_PREFIX}/public_atc/audio/{row_id}.flac"
            _upload_file(storage_client, audio_path, audio_gcs_uri)

            normalized_text = str(selected["normalized_text"])
            example = build_example(
                audio_uri=audio_gcs_uri,
                gt_text=normalized_text,
                system_prompt=PIPELINE_SYSTEM_PROMPT,
                user_prompt=PIPELINE_USER_PROMPT,
            )
            if not validate_example(example):
                raise ValueError(f"Invalid SFT example for public row {row_id}")
            sft_rows.append(example)
            manifest_rows.append(
                {
                    "audio_filepath": audio_gcs_uri,
                    "duration": duration,
                    "offset": 0.0,
                    "text": normalized_text,
                    "example_id": row_id,
                    "segment_id": "public_atc",
                    "source_dataset": HF_DATASET_ID,
                    "source_split": selected["split"],
                    "source_file": repo_file,
                }
            )

    if len(sft_rows) != TARGET_SELECTED:
        raise RuntimeError(
            f"Expected {TARGET_SELECTED} public rows, got {len(sft_rows)}"
        )

    _write_jsonl(PUBLIC_GEMINI_JSONL, sft_rows)
    _write_jsonl(PUBLIC_MANIFEST_JSONL, manifest_rows)
    _upload_file(storage_client, PUBLIC_GEMINI_JSONL, PUBLIC_GCS_GEMINI_URI)
    _upload_file(storage_client, PUBLIC_MANIFEST_JSONL, PUBLIC_GCS_MANIFEST_URI)

    report = {
        "public_rows": len(sft_rows),
        "total_public_duration_seconds": sum(
            float(row["duration"]) for row in manifest_rows
        ),
        "public_gemini_uri": PUBLIC_GCS_GEMINI_URI,
        "public_manifest_uri": PUBLIC_GCS_MANIFEST_URI,
    }
    _upload_text(
        storage_client,
        json.dumps(report, indent=2),
        f"{GCS_PREFIX}/public_atc/staging_report.json",
    )
    logger.info("Staged %d public ATC rows", len(sft_rows))
    return 0


def build_blended(args: argparse.Namespace) -> int:
    del args
    _ensure_dirs()
    if not PUBLIC_GEMINI_JSONL.exists():
        raise FileNotFoundError(
            f"Run stage-public first: {PUBLIC_GEMINI_JSONL}"
        )

    storage_client = _storage_client()
    wd_rows = _iter_jsonl_text(
        _download_gcs_text(storage_client, WD_GEMINI_TRAIN_URI)
    )
    public_rows = _read_jsonl(PUBLIC_GEMINI_JSONL)
    combined = [*wd_rows, *public_rows]
    random.Random(SEED).shuffle(combined)  # noqa: S311
    _write_jsonl(BLENDED_GEMINI_JSONL, combined)
    _upload_file(storage_client, BLENDED_GEMINI_JSONL, BLENDED_GCS_TRAIN_URI)

    public_manifest = _read_jsonl(PUBLIC_MANIFEST_JSONL)
    wd_duration = _sum_manifest_duration(storage_client, WD_CANONICAL_TRAIN_URI)
    public_duration = sum(float(row["duration"]) for row in public_manifest)
    report = {
        "wd_train_rows": len(wd_rows),
        "public_rows": len(public_rows),
        "blended_rows": len(combined),
        "wd_train_duration_seconds": wd_duration,
        "public_duration_seconds": public_duration,
        "blended_duration_seconds": wd_duration + public_duration,
        "blended_train_uri": BLENDED_GCS_TRAIN_URI,
        "shuffle_seed": SEED,
    }
    (WORKSPACE_DIR / "blended_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    _upload_text(
        storage_client,
        json.dumps(report, indent=2),
        f"{GCS_PREFIX}/blended/blended_report.json",
    )
    logger.info("Built blended train JSONL with %d rows", len(combined))
    return 0


def _run_duration_seconds(storage_client: Any, spec: RunSpec) -> float:
    wd_duration = _sum_manifest_duration(storage_client, WD_CANONICAL_TRAIN_URI)
    if spec.variant == "wd_internal":
        return wd_duration
    public_manifest = _read_jsonl(PUBLIC_MANIFEST_JSONL)
    return wd_duration + sum(float(row["duration"]) for row in public_manifest)


def _estimate_cost(total_seconds: float, epochs: int) -> dict[str, Any]:
    rate = SFT_TRAINING_COST_PER_MILLION[DEFAULT_BASE_MODEL]
    estimated_tokens = total_seconds * 32 * epochs
    return {
        "audio_tokens_per_second": 32,
        "estimated_training_tokens": round(estimated_tokens),
        "training_rate_per_million_tokens_usd": rate,
        "estimated_cost_usd": round((estimated_tokens / 1_000_000) * rate, 2),
        "model_display_name": SFT_MODEL_DISPLAY_NAMES[DEFAULT_BASE_MODEL],
    }


def make_configs(args: argparse.Namespace) -> int:
    del args
    _ensure_dirs()
    storage_client = _storage_client()
    commands: list[str] = [
        "# Prepared tuning commands",
        "",
        "Run these only when ready to submit paid Vertex tuning jobs.",
        "",
        "```bash",
        "cd model/scripts/sft",
    ]
    summary: list[dict[str, Any]] = []
    for spec in RUN_SPECS:
        total_duration = _run_duration_seconds(storage_client, spec)
        config = {
            "round_id": spec.round_id,
            "experiment_variant": spec.variant,
            "datasets": ["wd_sft_v20260528"],
            "gcp_project": GCP_PROJECT,
            "gcs_bucket": GCS_BUCKET,
            "gcs_sft_prefix": f"gs://{GCS_BUCKET}/sft",
            "system_prompt": PIPELINE_SYSTEM_PROMPT,
            "user_prompt": PIPELINE_USER_PROMPT,
            "combined_train_uri": spec.train_uri,
            "combined_val_uri": WD_GEMINI_EVAL_URI,
            "eval_manifest_uri": WD_CANONICAL_EVAL_URI,
            "base_model": DEFAULT_BASE_MODEL,
            "epochs": spec.epochs,
            "adapter_size": spec.adapter_size,
            "lr_multiplier": spec.lr_multiplier,
            "export_last_checkpoint_only": False,
            "total_train_duration_seconds": total_duration,
            "cost_estimate": _estimate_cost(total_duration, spec.epochs),
            "prepared_at": "2026-05-29",
        }
        round_dir = RESULTS_DIR / spec.round_id
        round_dir.mkdir(parents=True, exist_ok=True)
        (round_dir / "config.json").write_text(
            json.dumps(config, indent=2), encoding="utf-8"
        )
        summary.append(config)
        commands.append(
            "PYTHONPATH=../../colabs:. python pipeline.py tune "
            f"--round-id {spec.round_id} "
            f"--base-model {DEFAULT_BASE_MODEL} "
            f"--epochs {spec.epochs} "
            f"--adapter-size {spec.adapter_size} "
            f"--lr-multiplier {spec.lr_multiplier} "
            "--confirm"
        )
    commands.extend(["```", ""])
    COMMANDS_MD.write_text("\n".join(commands), encoding="utf-8")
    (CONFIG_DIR / "config_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    logger.info("Wrote %d run configs and %s", len(RUN_SPECS), COMMANDS_MD)
    return 0


def _line_count_gcs(storage_client: Any, gcs_uri: str) -> int:
    return len(_iter_jsonl_text(_download_gcs_text(storage_client, gcs_uri)))


def _validate_sft_gcs(storage_client: Any, gcs_uri: str) -> dict[str, Any]:
    rows = _iter_jsonl_text(_download_gcs_text(storage_client, gcs_uri))
    invalid = sum(1 for row in rows if not validate_example(row))
    audio_uris = {_audio_uri_from_sft(row) for row in rows}
    audio_uris.discard("")
    return {
        "uri": gcs_uri,
        "rows": len(rows),
        "invalid_rows": invalid,
        "unique_audio_uris": len(audio_uris),
        "audio_uris": audio_uris,
    }


def validate_ready(args: argparse.Namespace) -> int:
    del args
    _ensure_dirs()
    storage_client = _storage_client()
    required_uris = [
        WD_GEMINI_TRAIN_URI,
        WD_GEMINI_EVAL_URI,
        WD_CANONICAL_TRAIN_URI,
        WD_CANONICAL_EVAL_URI,
        PUBLIC_GCS_GEMINI_URI,
        PUBLIC_GCS_MANIFEST_URI,
        BLENDED_GCS_TRAIN_URI,
    ]
    existence = {
        uri: _blob_exists(storage_client, uri) for uri in required_uris
    }
    missing = [uri for uri, exists in existence.items() if not exists]

    wd_train = _validate_sft_gcs(storage_client, WD_GEMINI_TRAIN_URI)
    wd_eval = _validate_sft_gcs(storage_client, WD_GEMINI_EVAL_URI)
    blended = _validate_sft_gcs(storage_client, BLENDED_GCS_TRAIN_URI)
    train_eval_overlap = sorted(blended["audio_uris"] & wd_eval["audio_uris"])

    configs: list[dict[str, Any]] = []
    for spec in RUN_SPECS:
        cfg_path = RESULTS_DIR / spec.round_id / "config.json"
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        configs.append(
            {
                "round_id": spec.round_id,
                "exists": cfg_path.exists(),
                "base_model": cfg.get("base_model"),
                "adapter_size": cfg.get("adapter_size"),
                "epochs": cfg.get("epochs"),
                "train_uri": cfg.get("combined_train_uri"),
                "val_uri": cfg.get("combined_val_uri"),
                "cost_estimate": cfg.get("cost_estimate"),
            }
        )

    report = {
        "passed": not missing
        and wd_train["invalid_rows"] == 0
        and wd_eval["invalid_rows"] == 0
        and blended["invalid_rows"] == 0
        and not train_eval_overlap,
        "required_uri_existence": existence,
        "missing_uris": missing,
        "wd_train": {k: v for k, v in wd_train.items() if k != "audio_uris"},
        "wd_eval": {k: v for k, v in wd_eval.items() if k != "audio_uris"},
        "blended_train": {
            k: v for k, v in blended.items() if k != "audio_uris"
        },
        "train_eval_audio_overlap_count": len(train_eval_overlap),
        "train_eval_audio_overlap_examples": train_eval_overlap[:20],
        "public_manifest_rows": _line_count_gcs(
            storage_client, PUBLIC_GCS_MANIFEST_URI
        ),
        "configs": configs,
        "fine_tuning_jobs_submitted": False,
    }
    READINESS_REPORT_JSON.write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    _upload_text(
        storage_client,
        json.dumps(report, indent=2),
        f"{GCS_PREFIX}/readiness_report.json",
    )
    logger.info("Readiness passed: %s", report["passed"])
    return 0 if report["passed"] else 1


def _download_gcs_file(
    storage_client: Any, gcs_uri: str, local_path: Path
) -> None:
    from google.cloud.storage.retry import DEFAULT_RETRY

    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    storage_client.bucket(bucket_name).blob(blob_path).download_to_filename(
        str(local_path), retry=DEFAULT_RETRY
    )


def preflight_configs(args: argparse.Namespace) -> int:
    del args
    from preflight import run_preflight

    _ensure_dirs()
    storage_client = _storage_client()
    checks = [
        {
            "name": "wd_internal",
            "train_uri": WD_GEMINI_TRAIN_URI,
            "val_uri": WD_GEMINI_EVAL_URI,
            "report_path": WORKSPACE_DIR / "preflight_wd_internal_report.json",
        },
        {
            "name": "wd_internal_plus_atc",
            "train_uri": BLENDED_GCS_TRAIN_URI,
            "val_uri": WD_GEMINI_EVAL_URI,
            "report_path": WORKSPACE_DIR / "preflight_wd_atc_report.json",
        },
    ]
    summary: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        for check in checks:
            train_path = tmp_dir / f"{check['name']}_train.jsonl"
            val_path = tmp_dir / f"{check['name']}_val.jsonl"
            _download_gcs_file(storage_client, check["train_uri"], train_path)
            _download_gcs_file(storage_client, check["val_uri"], val_path)
            report = run_preflight(
                train_jsonl_path=train_path,
                val_jsonl_path=val_path,
                storage_client=storage_client,
                report_path=check["report_path"],
                system_prompt=PIPELINE_SYSTEM_PROMPT,
                user_prompt=PIPELINE_USER_PROMPT,
                gcs_max_workers=8,
                gcs_batch_pause_seconds=0.1,
            )
            summary.append(
                {
                    "name": check["name"],
                    "train_uri": check["train_uri"],
                    "val_uri": check["val_uri"],
                    "report_path": str(
                        check["report_path"].relative_to(REPO_ROOT)
                    ),
                    "passed": report.passed,
                    "failure_count": len(report.failures),
                }
            )
    PREFLIGHT_SUMMARY_JSON.write_text(
        json.dumps(
            {
                "passed": all(row["passed"] for row in summary),
                "checks": summary,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    logger.info("Preflight summary: %s", PREFLIGHT_SUMMARY_JSON)
    return 0 if all(row["passed"] for row in summary) else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare one-off Gemini 3.1 Flash-Lite SFT run artifacts"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    select_parser = sub.add_parser("select")
    select_parser.add_argument("--skip-similarity", action="store_true")
    sub.add_parser("stage-public")
    sub.add_parser("build-blended")
    sub.add_parser("make-configs")
    sub.add_parser("validate-ready")
    sub.add_parser("audit-selection")
    sub.add_parser("preflight-configs")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    return {
        "select": select_public,
        "stage-public": stage_public,
        "build-blended": build_blended,
        "make-configs": make_configs,
        "validate-ready": validate_ready,
        "audit-selection": audit_selection,
        "preflight-configs": preflight_configs,
    }[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
