#!/usr/bin/env python3
"""Inspect ground-truth manifests to settle the masking design questions.

Answers three things about the annotation data:

  1. Which categories carry ground-truth text, and which do not. This is
     what decides whether disposition can be driven by text presence
     rather than by a hand-maintained category list.
  2. What the text actually looks like for non-transcription categories --
     specifically whether [UNINTELLIGIBLE]-style markers appear, since a
     marker is trainable ground truth while an empty string is not.
  3. Whether annotations ever overlap in time, and which category pairs
     do it. Overlap is what makes "mask this span" and "keep that span"
     conflict.

Usage:

    python inspect_gt_manifest.py MANIFEST [MANIFEST ...]

Accepts local paths or gs:// URIs (needs google-cloud-storage for the
latter), and either JSON arrays or JSONL. Read-only; writes nothing.
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib
import sys

# Prefer the real taxonomy over the copy below, so this cannot drift from
# what the notebook actually masks with.
_SRC_DIR = pathlib.Path(__file__).resolve().parents[1] / "src"
if _SRC_DIR.is_dir() and str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

# The fallback literals keep the script runnable when it is copied out of
# the repo, e.g. pasted into a Colab cell.
try:
    from common import audio_masking

    SPEECH_PREFIXES = audio_masking.SPEECH_PREFIXES
    MASKED_PREFIXES = audio_masking.MASKED_PREFIXES
    PRESERVED_PREFIXES = audio_masking.PRESERVED_PREFIXES
except ImportError:
    SPEECH_PREFIXES = ("TRANSCRIPTION",)
    MASKED_PREFIXES = (
        "PII",
        "UNINTELLIGIBLE",
        "FOREIGN_SPEECH",
        "UNKNOWN",
        "LAUGHTER",
    )
    PRESERVED_PREFIXES = ("DTMF", "RINGING", "STATIC")

KNOWN_PREFIXES = SPEECH_PREFIXES + MASKED_PREFIXES + PRESERVED_PREFIXES


def load_manifest(path: str) -> list[dict]:
    """Read a manifest from a local path or gs:// URI, JSON or JSONL."""
    if path.startswith("gs://"):
        from google.cloud import storage  # noqa: PLC0415

        bucket_name, _, blob_name = path[len("gs://") :].partition("/")
        client = storage.Client()
        content = client.bucket(bucket_name).blob(blob_name).download_as_text()
    else:
        with open(path) as handle:
            content = handle.read()

    content = content.strip()
    if not content:
        return []
    if content[0] == "[":
        return json.loads(content)
    return [json.loads(line) for line in content.splitlines() if line.strip()]


def normalize(category: str | None) -> str:
    return (category or "").strip().upper() or "<missing>"


def has_text(entry: dict) -> bool:
    return bool((entry.get("text") or "").strip())


def report_text_by_category(entries: list[dict]) -> None:
    """Which categories carry ground truth text."""
    counts: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0])
    for entry in entries:
        counts[normalize(entry.get("category"))][
            0 if has_text(entry) else 1
        ] += 1

    print("\ncategory x ground-truth text")
    print(f"  {'category':<28} {'total':>8} {'with text':>10} {'empty':>8}")
    for category in sorted(counts):
        with_text, empty = counts[category]
        print(
            f"  {category:<28} {with_text + empty:>8,} "
            f"{with_text:>10,} {empty:>8,}"
        )

    # Composite labels (PII/ADDRESS, PII/ID/PHONE) fragment the picture, so
    # roll them up to the base prefix as well -- a family that splits across
    # text/no-text is the signal, even when no single label does.
    rolled: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0])
    for category, (with_text, empty) in counts.items():
        base = next(
            (p for p in KNOWN_PREFIXES if category.startswith(p)),
            "<unclassified>",
        )
        rolled[base][0] += with_text
        rolled[base][1] += empty

    print("\n  rolled up to base prefix")
    print(f"  {'prefix':<28} {'total':>8} {'with text':>10} {'empty':>8}")
    for base in sorted(rolled):
        with_text, empty = rolled[base]
        print(
            f"  {base:<28} {with_text + empty:>8,} {with_text:>10,} {empty:>8,}"
        )

    split = sorted(c for c, (w, e) in counts.items() if w and e)
    split_families = sorted(b for b, (w, e) in rolled.items() if w and e)
    print("\n  splits across text / no-text:")
    if not split and not split_families:
        print("    (none -- category and text presence agree perfectly)")
        print(
            "    -> a text-driven rule reproduces the current category "
            "lists exactly."
        )
        return
    for category in split:
        print(f"    label  {category}")
    for base in split_families:
        print(f"    family {base}*")
    print(
        "    -> text presence carries information the label does not;\n"
        "       driving disposition off text changes what gets masked."
    )


def report_text_samples(entries: list[dict], limit: int = 5) -> None:
    """What the text looks like for non-transcription categories."""
    samples: dict[str, collections.Counter] = collections.defaultdict(
        collections.Counter
    )
    for entry in entries:
        category = normalize(entry.get("category"))
        if category.startswith(SPEECH_PREFIXES):
            continue
        text = (entry.get("text") or "").strip()
        if text:
            samples[category][text] += 1

    print("\nmost common text on non-transcription categories")
    if not samples:
        print("  (none carry text at all)")
        return
    for category in sorted(samples):
        print(f"  {category}:")
        for text, count in samples[category].most_common(limit):
            shown = text if len(text) <= 60 else text[:57] + "..."
            print(f"    {count:>6,}x  {shown!r}")


def report_unknown_categories(entries: list[dict]) -> None:
    """Categories outside the taxonomy currently in common.audio_masking."""
    unknown = collections.Counter()
    for entry in entries:
        category = normalize(entry.get("category"))
        if not category.startswith(KNOWN_PREFIXES):
            unknown[category] += 1

    print("\ncategories not classified by the current taxonomy")
    if not unknown:
        print("  (none)")
        return
    for category, count in unknown.most_common():
        print(f"  {count:>8,}x  {category}")


def report_overlaps(entries: list[dict]) -> None:
    """Annotations that overlap in time within the same audio file."""
    by_file = collections.defaultdict(list)
    for entry in entries:
        offset, duration = entry.get("offset"), entry.get("duration")
        if offset is None or duration is None:
            continue
        by_file[entry.get("audio_filepath")].append(
            (
                float(offset),
                float(offset) + float(duration),
                normalize(entry.get("category")),
                has_text(entry),
            )
        )

    pairs = collections.Counter()
    worst = []
    total = 0
    for segments in by_file.values():
        segments.sort()
        for i, (start_a, end_a, cat_a, text_a) in enumerate(segments):
            for start_b, end_b, cat_b, text_b in segments[i + 1 :]:
                if start_b >= end_a:
                    break  # sorted by start; nothing later can overlap
                overlap = min(end_a, end_b) - start_b
                if overlap <= 0:
                    continue
                total += 1
                pairs[tuple(sorted((cat_a, cat_b)))] += 1
                worst.append((overlap, cat_a, text_a, cat_b, text_b))

    print("\ntime overlaps within the same audio file")
    if not total:
        print("  (none -- annotations are strictly non-overlapping)")
        return
    print(f"  {total:,} overlapping pairs\n")
    for (cat_a, cat_b), count in pairs.most_common(15):
        print(f"    {count:>8,}  {cat_a}  x  {cat_b}")

    worst.sort(reverse=True)
    print("\n  largest overlaps:")
    for overlap, cat_a, text_a, cat_b, text_b in worst[:5]:
        mark_a = "text" if text_a else "no text"
        mark_b = "text" if text_b else "no text"
        print(f"    {overlap:6.2f}s  {cat_a} ({mark_a})  x  {cat_b} ({mark_b})")
    print(
        "\n  -> pairs where one side has text and the other does not are the\n"
        "     ones that force a mask/keep conflict."
    )


# ---------------------------------------------------------------------------
# Damage accounting: what the segmentation notebook on main does to the audio,
# replayed from timings alone. No audio is downloaded -- offsets and durations
# are all the masking decisions depend on.
# ---------------------------------------------------------------------------

# main's sets, matched by equality exactly as main does (no strip/upper, no
# prefix matching) so the replay reproduces its real behavior.
MAIN_HUMAN = {"TRANSCRIPTION", "PII", "UNINTELLIGIBLE"}
MAIN_MASKED = {"PII", "UNINTELLIGIBLE", "DTMF", "RINGING"}

TRANSIENT_PAD = 0.05
CROSSFADE = 0.01


def _merge(spans: list[tuple[float, float]]) -> list[tuple[float, float]]:
    merged: list[list[float]] = []
    for start, end in sorted(spans):
        if start >= end:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


def _intersect_duration(
    a: list[tuple[float, float]], b: list[tuple[float, float]]
) -> float:
    total = 0.0
    for a_start, a_end in _merge(a):
        for b_start, b_end in _merge(b):
            overlap = min(a_end, b_end) - max(a_start, b_start)
            if overlap > 0:
                total += overlap
    return total


def _subtract(
    target: tuple[float, float], protected: list[tuple[float, float]]
) -> list[tuple[float, float]]:
    start, end = target
    if start >= end:
        return []
    out, cursor = [], start
    for p_start, p_end in _merge(protected):
        if p_end <= cursor:
            continue
        if p_start >= end:
            break
        if p_start > cursor:
            out.append((cursor, p_start))
        cursor = max(cursor, p_end)
    if cursor < end:
        out.append((cursor, end))
    return out


def _span(entry: dict) -> tuple[float, float]:
    offset = float(entry["offset"])
    return (offset, offset + float(entry["duration"]))


def _cluster(segments: list[dict], threshold: float) -> list[list[dict]]:
    if not segments:
        return []
    blocks = [[segments[0]]]
    max_end = _span(segments[0])[1]
    for segment in segments[1:]:
        start, end = _span(segment)
        if start - max_end <= threshold:
            blocks[-1].append(segment)
            max_end = max(max_end, end)
        else:
            blocks.append([segment])
            max_end = end
    return blocks


def _old_clamped_mask(
    entry: dict, transcriptions: list[dict]
) -> tuple[float, float]:
    """Reproduce the pre-fix clamp loop exactly."""
    seg_start, seg_end = _span(entry)
    mask_start = seg_start - TRANSIENT_PAD
    mask_end = seg_end + TRANSIENT_PAD
    for transcription in transcriptions:
        t_start, t_end = _span(transcription)
        if mask_start < t_end and seg_start >= t_end:
            mask_start = t_end
        if mask_end > t_start and seg_end <= t_start:
            mask_end = t_start
    return (mask_start, mask_end)


def _exported_windows(
    entries: list[dict], threshold: float, pad: float
) -> list[tuple[float, float, list[dict], list[dict]]]:
    """Replay main's segmentation. Yields (start, end, block, all entries)."""
    entries = sorted(entries, key=lambda e: float(e["offset"]))
    anchors = [e for e in entries if e.get("category") in MAIN_HUMAN]
    transcriptions = [
        e for e in entries if e.get("category") == "TRANSCRIPTION"
    ]
    # No audio on hand, so cap at the last annotation rather than file length.
    audio_end = max((_span(e)[1] for e in entries), default=0.0)

    windows = []
    for block in _cluster(anchors, threshold):
        block_transcriptions = [
            e for e in block if e.get("category") == "TRANSCRIPTION"
        ]
        if not block_transcriptions:
            continue
        core_start = min(_span(e)[0] for e in block_transcriptions)
        core_end = max(_span(e)[1] for e in block_transcriptions)

        preceding = [e for e in transcriptions if _span(e)[1] <= core_start]
        front = core_start - max(
            (_span(e)[1] for e in preceding), default=core_start - pad
        )
        following = [e for e in transcriptions if _span(e)[0] >= core_end]
        back = (
            min((_span(e)[0] for e in following), default=core_end + pad)
            - core_end
        )

        start = max(0.0, core_start - max(0.0, min(pad, front)))
        end = min(audio_end, core_end + max(0.0, min(pad, back)))
        windows.append((start, end, block_transcriptions, entries))
    return windows


def _clip(
    spans: list[tuple[float, float]], start: float, end: float
) -> list[tuple[float, float]]:
    return [
        (max(start, s), min(end, e))
        for s, e in spans
        if min(end, e) > max(start, s)
    ]


def _measure_segment(
    start: float,
    end: float,
    block_transcriptions: list[dict],
    all_entries: list[dict],
) -> dict[str, float]:
    """Measure one exported segment: old masking vs new, in seconds.

    Args:
        start: Segment start on the source file's timeline.
        end: Segment end.
        block_transcriptions: Transcribed annotations in this block, which
            the old code clamped against and the new code protects.
        all_entries: Every annotation on the source file.

    Returns:
        Seconds for each damage class, keyed ``destroyed``, ``unlabeled``,
        ``bleed``, ``signaling``, plus ``violation`` set when the new
        masking touches transcribed speech, which it never should.
    """
    protected = _clip([_span(t) for t in block_transcriptions], start, end)

    old_masks, old_fades, new_masks, old_signaling = [], [], [], []
    for entry in all_entries:
        category = entry.get("category") or ""
        if category in MAIN_MASKED:
            mask = _old_clamped_mask(entry, block_transcriptions)
            old_masks.append(mask)
            old_fades.append((mask[0] - CROSSFADE, mask[0]))
            old_fades.append((mask[1], mask[1] + CROSSFADE))
            if category in {"DTMF", "RINGING"}:
                old_signaling.append(mask)
        if category.strip().upper().startswith(MASKED_PREFIXES):
            span_start, span_end = _span(entry)
            new_masks.extend(
                _subtract(
                    (span_start - TRANSIENT_PAD, span_end + TRANSIENT_PAD),
                    protected,
                )
            )

    old_masks = _clip(old_masks, start, end)
    new_masks = _clip(new_masks, start, end)

    # B. speech with no ground truth left audible: what the new masking
    #    covers and the old did not.
    unlabeled = 0.0
    for mask in new_masks:
        for remainder in _subtract(mask, old_masks):
            unlabeled += remainder[1] - remainder[0]

    return {
        # A. transcribed audio the old clamp masked over
        "destroyed": _intersect_duration(old_masks, protected),
        "unlabeled": unlabeled,
        # C. crossfade ramps attenuating speech
        "bleed": _intersect_duration(_clip(old_fades, start, end), protected),
        # D. signaling the old profile masked and the new one preserves
        "signaling": _intersect_duration(old_signaling, [(start, end)]),
        # The property the fix exists to guarantee.
        "violation": _intersect_duration(new_masks, protected),
    }


def report_damage(entries: list[dict], threshold: float, pad: float) -> None:
    """Quantify what the pre-fix masking gets wrong, per exported segment."""
    by_file = collections.defaultdict(list)
    for entry in entries:
        if entry.get("offset") is None or entry.get("duration") is None:
            continue
        by_file[entry.get("audio_filepath")].append(entry)

    measured = collections.defaultdict(list)
    exported = 0
    violations = 0

    for file_entries in by_file.values():
        for window in _exported_windows(file_entries, threshold, pad):
            exported += 1
            result = _measure_segment(*window)
            for key in ("destroyed", "unlabeled", "bleed", "signaling"):
                if result[key] > 0:
                    measured[key].append(result[key])
            # Counted rather than asserted so a violation is reported in
            # the summary instead of aborting the run with a traceback.
            if result["violation"] > 1e-9:
                violations += 1

    destroyed = measured["destroyed"]
    unlabeled = measured["unlabeled"]
    bleed = measured["bleed"]
    signaling = measured["signaling"]

    def summarize(name: str, values: list[float], note: str) -> None:
        print(f"\n{name}")
        if not values:
            print("  none")
            return
        print(
            f"  {len(values):,} of {exported:,} exported segments affected, "
            f"{sum(values):,.1f}s total, {max(values):.2f}s worst"
        )
        buckets = [
            ("       < 50ms", 0.0, 0.05),
            ("  50ms - 200ms", 0.05, 0.2),
            ("   200ms - 1s", 0.2, 1.0),
            ("       1s - 3s", 1.0, 3.0),
            ("         > 3s", 3.0, float("inf")),
        ]
        for label, low, high in buckets:
            count = sum(1 for v in values if low <= v < high)
            if count:
                share = 100.0 * count / len(values)
                print(f"    {label}: {count:>6,} segments ({share:4.1f}%)")
        print(f"  -> {note}")

    print(f"\n{'-' * 70}")
    print(f"damage replay ({exported:,} exported segments)")
    print("-" * 70)
    summarize(
        "A. transcribed audio masked over (ground truth with no audio)",
        destroyed,
        "fixed by subtract_intervals; existing datasets carry this.",
    )
    summarize(
        "B. unlabeled speech left audible (composite PII never matched)",
        unlabeled,
        "fixed by prefix matching; this is the punish-the-model case.",
    )
    summarize(
        "C. speech attenuated by crossfade ramps",
        bleed,
        "fixed by fade_bounds; small per instance, check the total.",
    )
    summarize(
        "D. DTMF/RINGING masked before, preserved now",
        signaling,
        "deliberate change, not a defect -- shown so the delta is visible.",
    )

    print("\ncheck: segments where the NEW masking touches transcribed speech")
    if violations:
        print(f"  {violations:,} of {exported:,} -- EXPECTED 0, investigate")
    else:
        print(f"  0 of {exported:,} -- the fix holds over this manifest")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifests", nargs="+", help="local paths or gs:// URIs"
    )
    parser.add_argument(
        "--combined",
        action="store_true",
        help="report over all manifests pooled instead of one by one",
    )
    parser.add_argument(
        "--damage",
        action="store_true",
        help="replay main's masking and quantify what it gets wrong",
    )
    parser.add_argument(
        "--gap",
        type=float,
        default=0.5,
        help="TRANSMISSION_GAP_THRESHOLD used for the replay (default 0.5)",
    )
    parser.add_argument(
        "--pad",
        type=float,
        default=0.5,
        help="DESIRED_PAD used for the replay (default 0.5)",
    )
    args = parser.parse_args()

    pooled = []
    for path in args.manifests:
        entries = load_manifest(path)
        pooled.extend(entries)
        if args.combined:
            print(f"loaded {len(entries):,} entries from {path}")
            continue
        files = {e.get("audio_filepath") for e in entries}
        print(f"\n{'=' * 70}")
        print(f"{path}  ({len(entries):,} entries, {len(files):,} audio files)")
        print("=" * 70)
        report_text_by_category(entries)
        report_text_samples(entries)
        report_unknown_categories(entries)
        report_overlaps(entries)
        if args.damage:
            report_damage(entries, args.gap, args.pad)

    if args.combined:
        files = {e.get("audio_filepath") for e in pooled}
        print(f"\n{'=' * 70}")
        print(
            f"COMBINED  ({len(pooled):,} entries, {len(files):,} audio files)"
        )
        print("=" * 70)
        report_text_by_category(pooled)
        report_text_samples(pooled)
        report_unknown_categories(pooled)
        report_overlaps(pooled)
        if args.damage:
            report_damage(pooled, args.gap, args.pad)

    return 0


if __name__ == "__main__":
    sys.exit(main())
