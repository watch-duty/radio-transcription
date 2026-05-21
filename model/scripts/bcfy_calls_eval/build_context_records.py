"""Build per-call framing-context records from the eval clips' WAV metadata.

Background: the bcfy_calls eval clips were captured by scanner software
(ProScan / SDRTrunk) that embedded each call's talkgroup identity in the
audio's ID3-style tags. The Broadcastify-Calls *API* cannot recover that
context for these clips — Jake's collection dropped the system ``sid`` (keeping
only ``{call_ts}-{tgDec}``) and ``tgDec`` collides across systems, so there is
no reliable sid->groupId mapping (a round-trip mis-resolved Palmetto 800 to
"Baltimore County"). The embedded tags are the faithful per-call source. See
``FINDINGS.md`` for the full provenance investigation.

Pipeline:
1. Load the eval manifest, collect unique ``example_id`` (= "{call_ts}-{tgDec}").
2. Download each unique call's full WAV (cached locally).
3. ``ffprobe`` the tags; ``wav_context.build_record`` -> framing record.
4. Write ``context_records.json`` (keyed by example_id) + ``context_report.json``
   (framing-coverage stats: how many clips yield framing vs. fall back to
   baseline, and the per-variant rendered-descriptor counts).

Outputs (under results/<EXPERIMENT_NAME>/):
- context_records.json  -- per-call framing records consumed by run_eval.py
- context_report.json   -- coverage summary
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from bcfy_calls_eval import framing_renderer, wav_context

log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MANIFEST = (
    "gs://wd-transcription-data/segmented_audio/broadcastify/calls/eval/"
    "audio_raw/batch_manifest.jsonl"
)
AUDIO_PREFIX = "gs://wd-transcription-data/audio/broadcastify/calls/eval"
EXPERIMENT_NAME = "framing_context_05_2026"
RESULTS_DIR = SCRIPT_DIR / "results" / EXPERIMENT_NAME
WAV_CACHE = SCRIPT_DIR / ".wav_cache"


# ---------------------------------------------------------------------------
# Manifest + audio I/O
# ---------------------------------------------------------------------------


def load_example_ids(uri: str) -> list[str]:
    if uri.startswith("gs://"):
        raw = subprocess.check_output(["gsutil", "cat", uri], text=True)
    else:
        raw = Path(uri).read_text()
    eids = {json.loads(line)["example_id"] for line in raw.splitlines() if line.strip()}
    return sorted(eids)


def _download_one(eid: str, cache: Path) -> Path | None:
    dst = cache / f"{eid}.wav"
    if dst.exists() and dst.stat().st_size > 0:
        return dst
    try:
        subprocess.run(
            ["gsutil", "-q", "cp", f"{AUDIO_PREFIX}/{eid}.wav", str(dst)],
            check=True, capture_output=True,
        )
        return dst
    except subprocess.CalledProcessError as exc:  # noqa: BLE001
        log.warning("download failed for %s: %s", eid, exc.stderr.decode()[:120])
        return None


def download_all(eids: list[str], cache: Path, workers: int = 16) -> dict[str, Path]:
    cache.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for eid, path in zip(eids, ex.map(lambda e: _download_one(e, cache), eids)):
            if path is not None:
                out[eid] = path
    return out


_META_KEYS = ("title", "artist", "comment", "album", "genre", "date")


def ffprobe_tags(path: Path) -> dict[str, str]:
    """Extract ID3-style tags via ffprobe (format-tags as JSON)."""
    out = subprocess.run(
        ["ffprobe", "-hide_banner", "-v", "quiet", "-print_format", "json",
         "-show_format", str(path)],
        capture_output=True, text=True,
    ).stdout
    try:
        tags = json.loads(out).get("format", {}).get("tags", {})
    except json.JSONDecodeError:
        return {}
    # ffprobe lower-cases tag keys; keep only the ones we use.
    return {k.lower(): v.strip() for k, v in tags.items()
            if k.lower() in _META_KEYS and v and v.strip()}


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def main() -> int:
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO
    )
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", default=DEFAULT_MANIFEST)
    p.add_argument("--cache", default=str(WAV_CACHE), help="local WAV cache dir")
    p.add_argument("--limit", type=int, default=0, help="cap unique calls (0=all)")
    args = p.parse_args()

    eids = load_example_ids(args.manifest)
    if args.limit:
        eids = eids[: args.limit]
    log.info("manifest: %d unique calls", len(eids))

    cache = Path(args.cache)
    log.info("downloading WAVs into %s ...", cache)
    paths = download_all(eids, cache)
    log.info("have %d/%d WAVs", len(paths), len(eids))

    records: dict[str, dict] = {}
    no_meta = 0
    for eid in eids:
        path = paths.get(eid)
        meta = ffprobe_tags(path) if path else {}
        if not meta:
            no_meta += 1
        records[eid] = wav_context.build_record(meta, eid)

    # Coverage: a record "frames" if moderate renders a non-empty descriptor.
    framed = sum(
        1 for r in records.values() if framing_renderer.render(r, "moderate")
    )
    with_state = sum(1 for r in records.values() if r["state"])
    with_system = sum(1 for r in records.values() if r["sName"])
    aggressive_extra = sum(
        1 for r in records.values()
        if framing_renderer.render(r, "aggressive")
        != framing_renderer.render(r, "moderate")
    )

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "context_records.json").write_text(
        json.dumps(records, indent=2, sort_keys=True)
    )

    report = {
        "manifest": args.manifest,
        "unique_calls": len(eids),
        "wavs_downloaded": len(paths),
        "no_metadata": no_meta,
        "framed_moderate": framed,
        "framed_pct": round(100.0 * framed / max(1, len(eids)), 1),
        "with_system_name": with_system,
        "with_state": with_state,
        "aggressive_adds_detail": aggressive_extra,
        "source": "wav_metadata",
    }
    (RESULTS_DIR / "context_report.json").write_text(json.dumps(report, indent=2))
    log.info("wrote context_records.json (%d records)", len(records))
    log.info(
        "framing coverage: %d/%d (%.1f%%) clips frame; %d have state, %d have system",
        framed, len(eids), report["framed_pct"], with_state, with_system,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
