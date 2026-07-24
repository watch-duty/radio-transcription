"""Extract an aggregate production request-duration target.

This module intentionally reads only the structural telemetry needed to identify
one logical model request: the transcriber's duration record, its correlation
fields, the canonical feed in the audio URI, and the presence of a correlated
``generateContent`` call.  It never reads response text, finish reasons, HTTP
status, transcripts, or error classifications.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime
import decimal
import fractions
import hashlib
import json
import math
import os
import re
import tempfile
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


TRANSCRIBING_RE = re.compile(
    r"^Transcribing (?P<seconds>[0-9]+(?:\.[0-9]+)?)s of audio from GCS URI: "
    r"(?P<uri>gs://[^ ]+)$"
)
BOUND_URI_RE = re.compile(
    r"^gs://ingestion-canonical-bucket/"
    r"(?:lossless|ephemeral/transcription)/"
    r"(?P<feed>[^/]+)/[0-9]{4}/[0-9]{2}/[0-9]{2}/"
    r"(?P<segment>[^/]+)\.flac$"
)
TUNED_URL_RE = re.compile(r"/locations/[^/]+/endpoints/[^/:]+:generateContent$")
BASE_URL_RE = re.compile(
    r"/locations/[^/]+/publishers/google/models/[^/:]+:generateContent$"
)
HTTP_MESSAGE_RE = re.compile(
    r'^HTTP Request: POST (?P<url>https://[^ ]+:generateContent) "HTTP/1\.1 '
    r'[0-9]{3} [^"]*"$'
)

_QUANTILES = (0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99)
_PRIVACY_CONTRACT = (
    "aggregate-only; contains no transcript, response, outcome, error, HTTP "
    "status, segment/feed/audio identifier, URI, trace/span, endpoint, raw log "
    "message, or local input path"
)


@dataclasses.dataclass(frozen=True, slots=True)
class DistributionCost:
    """Exact one-dimensional distance between two empirical histograms."""

    wasserstein_1_ms: fractions.Fraction
    maximum_cdf_gap: fractions.Fraction


@dataclasses.dataclass(slots=True)
class _RequestGroup:
    """Internal correlated telemetry state; never serialized."""

    segment: uuid.UUID | None = None
    feed: uuid.UUID | None = None
    duration_ms: int | None = None
    transcribing_events: int = 0
    transcribing_events_in_window: int = 0
    model_call_events_in_window: int = 0


def parse_timestamp(value: object) -> datetime.datetime:
    """Parse an ISO-8601 timestamp and normalize it to UTC."""

    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"invalid timestamp: {value!r}") from error
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(datetime.timezone.utc)


def duration_histogram(durations_ms: Sequence[int]) -> dict[int, int]:
    """Build a validated duration histogram."""

    histogram = Counter(durations_ms)
    return _validate_histogram(histogram)


def exact_ecdf(
    histogram: Mapping[int, int],
) -> tuple[tuple[int, int, int], ...]:
    """Return ``(duration_ms, cumulative_count, total_count)`` ECDF points."""

    checked = _validate_histogram(histogram)
    total = sum(checked.values())
    if total == 0:
        raise ValueError("ECDF requires a nonempty histogram")
    cumulative = 0
    result: list[tuple[int, int, int]] = []
    for duration_ms, count in checked.items():
        cumulative += count
        result.append((duration_ms, cumulative, total))
    return tuple(result)


def ecdf_at(
    histogram: Mapping[int, int],
    duration_ms: int,
) -> fractions.Fraction:
    """Evaluate an empirical CDF exactly at ``duration_ms``."""

    checked = _validate_histogram(histogram)
    total = sum(checked.values())
    if total == 0:
        raise ValueError("ECDF requires a nonempty histogram")
    cumulative = sum(
        count for value, count in checked.items() if value <= duration_ms
    )
    return fractions.Fraction(cumulative, total)


def exact_distribution_cost(
    left: Mapping[int, int],
    right: Mapping[int, int],
) -> DistributionCost:
    """Compute exact Wasserstein-1 and Kolmogorov-Smirnov distances.

    Wasserstein distance is the exact area between the two empirical CDFs on
    the integer-millisecond axis.  The result remains a ``Fraction`` so a
    reconstruction solver can compare candidates without floating-point ties.
    """

    checked_left = _validate_histogram(left)
    checked_right = _validate_histogram(right)
    left_total = sum(checked_left.values())
    right_total = sum(checked_right.values())
    if left_total == 0 or right_total == 0:
        raise ValueError(
            "distribution distance requires two nonempty histograms"
        )

    support = sorted(set(checked_left) | set(checked_right))
    left_cumulative = 0
    right_cumulative = 0
    wasserstein = fractions.Fraction()
    maximum_gap = fractions.Fraction()
    prior_duration: int | None = None

    for duration_ms in support:
        gap = abs(
            fractions.Fraction(left_cumulative, left_total)
            - fractions.Fraction(right_cumulative, right_total)
        )
        if prior_duration is not None:
            wasserstein += (duration_ms - prior_duration) * gap
        left_cumulative += checked_left.get(duration_ms, 0)
        right_cumulative += checked_right.get(duration_ms, 0)
        maximum_gap = max(
            maximum_gap,
            abs(
                fractions.Fraction(left_cumulative, left_total)
                - fractions.Fraction(right_cumulative, right_total)
            ),
        )
        prior_duration = duration_ms

    return DistributionCost(
        wasserstein_1_ms=wasserstein,
        maximum_cdf_gap=maximum_gap,
    )


def type7_quantile_ms(
    histogram: Mapping[int, int],
    probability: float | fractions.Fraction,
) -> fractions.Fraction:
    """Return the exact Hyndman-Fan type-7 empirical quantile."""

    checked = _validate_histogram(histogram)
    total = sum(checked.values())
    if total == 0:
        raise ValueError("quantile requires a nonempty histogram")
    probability_fraction = (
        probability
        if isinstance(probability, fractions.Fraction)
        else fractions.Fraction(str(probability))
    )
    if not 0 <= probability_fraction <= 1:
        raise ValueError("probability must be between zero and one")

    position = (total - 1) * probability_fraction
    lower_rank = position.numerator // position.denominator
    upper_rank = math.ceil(position)
    lower = _rank_value(checked, lower_rank)
    upper = _rank_value(checked, upper_rank)
    return lower + (position - lower_rank) * (upper - lower)


def histogram_statistics(histogram: Mapping[int, int]) -> dict[str, Any]:
    """Return aggregate, human-readable statistics for a duration histogram."""

    checked = _validate_histogram(histogram)
    count = sum(checked.values())
    if count == 0:
        raise ValueError("statistics require a nonempty histogram")
    total_duration = sum(value * weight for value, weight in checked.items())
    mean = fractions.Fraction(total_duration, count)
    return {
        "request_count": count,
        "unique_duration_count": len(checked),
        "total_duration_ms": total_duration,
        "minimum_duration_ms": min(checked),
        "maximum_duration_ms": max(checked),
        "mean_duration_ms": float(mean),
        "mean_duration_ms_exact": _fraction_text(mean),
        "quantile_method": "Hyndman-Fan type 7 (linear)",
        "quantiles_ms": {
            f"p{round(probability * 100):02d}": _json_number(
                type7_quantile_ms(checked, probability)
            )
            for probability in _QUANTILES
        },
    }


def extract_bcfy_feeds_geometry(
    *,
    raw_dir: Path,
    feed_sources_path: Path,
    start: datetime.datetime | str,
    end: datetime.datetime | str,
) -> dict[str, Any]:
    """Extract the privacy-safe BCFY-feeds production duration histogram."""

    start_utc = (
        parse_timestamp(start) if isinstance(start, str) else _as_utc(start)
    )
    end_utc = parse_timestamp(end) if isinstance(end, str) else _as_utc(end)
    if end_utc <= start_utc:
        raise ValueError("end must be after start")

    feed_sources, feed_provenance = _read_feed_sources(feed_sources_path)
    raw_files = sorted(raw_dir.glob("*.jsonl"))
    if not raw_files:
        raise ValueError(f"no JSONL logs found in {raw_dir}")

    groups: dict[tuple[str, str, str], _RequestGroup] = {}
    raw_set_digest = hashlib.sha256()
    raw_bytes = 0
    raw_rows = 0
    rows_in_window = 0
    duplicate_log_identities = 0
    transcribing_events = 0
    transcribing_events_in_window = 0
    model_call_events_in_window = 0
    other_generate_content_events_in_window = 0

    for raw_path in raw_files:
        file_digest = hashlib.sha256()
        identities_seen: set[tuple[str, str, str]] = set()
        with raw_path.open("rb") as stream:
            for line_number, raw_line in enumerate(stream, start=1):
                file_digest.update(raw_line)
                raw_bytes += len(raw_line)
                if not raw_line.strip():
                    continue
                raw_rows += 1
                try:
                    row = json.loads(raw_line)
                except (json.JSONDecodeError, UnicodeDecodeError) as error:
                    raise ValueError(
                        f"invalid JSON in {raw_path.name}:{line_number}"
                    ) from error
                if not isinstance(row, dict):
                    continue

                event_kind, request_url = _event_kind(row)
                identity = row.get("identity")
                timestamp_value = (
                    identity.get("timestamp")
                    if isinstance(identity, dict)
                    else None
                )
                try:
                    timestamp = parse_timestamp(timestamp_value)
                except ValueError:
                    if event_kind is not None:
                        raise ValueError(
                            "matched telemetry event has an invalid identity "
                            f"timestamp in {raw_path.name}:{line_number}"
                        )
                    continue

                identity_key = _identity_key(identity)
                if identity_key is None:
                    if event_kind is not None:
                        raise ValueError(
                            "matched telemetry event lacks a complete log identity "
                            f"in {raw_path.name}:{line_number}"
                        )
                    continue
                if identity_key in identities_seen:
                    duplicate_log_identities += 1
                    continue
                identities_seen.add(identity_key)

                in_window = start_utc <= timestamp < end_utc
                if in_window:
                    rows_in_window += 1
                if event_kind is None:
                    continue

                group_key = _group_key(row)
                if group_key is None:
                    raise ValueError(
                        "matched telemetry event lacks revision/trace/span "
                        f"in {raw_path.name}:{line_number}"
                    )
                group = groups.setdefault(group_key, _RequestGroup())

                if event_kind == "transcribing":
                    message = row["message"]
                    match = TRANSCRIBING_RE.fullmatch(message)
                    if match is None:
                        raise ValueError(
                            "malformed Transcribing event "
                            f"in {raw_path.name}:{line_number}"
                        )
                    uri_match = BOUND_URI_RE.fullmatch(match.group("uri"))
                    if uri_match is None:
                        raise ValueError(
                            "Transcribing event has an unbound canonical audio URI "
                            f"in {raw_path.name}:{line_number}"
                        )
                    try:
                        feed = uuid.UUID(uri_match.group("feed"))
                        segment = uuid.UUID(uri_match.group("segment"))
                    except ValueError as error:
                        raise ValueError(
                            "Transcribing event has an invalid feed or segment UUID "
                            f"in {raw_path.name}:{line_number}"
                        ) from error
                    duration_ms = _duration_ms(match.group("seconds"))
                    _bind_transcribing_event(
                        group,
                        segment=segment,
                        feed=feed,
                        duration_ms=duration_ms,
                    )
                    group.transcribing_events += 1
                    transcribing_events += 1
                    if in_window:
                        group.transcribing_events_in_window += 1
                        transcribing_events_in_window += 1
                    continue

                assert request_url is not None
                if not in_window:
                    continue
                if TUNED_URL_RE.search(request_url) or BASE_URL_RE.search(
                    request_url
                ):
                    group.model_call_events_in_window += 1
                    model_call_events_in_window += 1
                else:
                    other_generate_content_events_in_window += 1

        digest = file_digest.digest()
        raw_set_digest.update(len(raw_path.name).to_bytes(4, "big"))
        raw_set_digest.update(raw_path.name.encode())
        raw_set_digest.update(len(digest).to_bytes(4, "big"))
        raw_set_digest.update(digest)

    histogram: Counter[int] = Counter()
    groups_with_model_call = 0
    groups_without_transcribing_event = 0
    groups_with_multiple_transcribing_events = 0
    groups_with_unmapped_feed = 0
    groups_with_non_bcfy_source = 0
    for group in groups.values():
        if not group.model_call_events_in_window:
            continue
        groups_with_model_call += 1
        if (
            group.segment is None
            or group.feed is None
            or group.duration_ms is None
            or not group.transcribing_events
        ):
            groups_without_transcribing_event += 1
            continue
        if group.transcribing_events > 1:
            groups_with_multiple_transcribing_events += 1
        source = feed_sources.get(group.feed)
        if source is None:
            groups_with_unmapped_feed += 1
            continue
        if source != "bcfy_feeds":
            groups_with_non_bcfy_source += 1
            continue
        histogram[group.duration_ms] += 1

    checked_histogram = _validate_histogram(histogram)
    if not checked_histogram:
        raise ValueError("no BCFY-feeds logical model requests were found")
    return {
        "schema_version": 1,
        "kind": "bcfy_feeds_production_logical_request_duration_histogram",
        "privacy_contract": _PRIVACY_CONTRACT,
        "definition": (
            "one correlated revision/trace/span group with a canonical "
            "Transcribing duration event and at least one tuned-endpoint or "
            "foundation-model generateContent call inside the frozen window; "
            "multiple provider attempts in a group count as one logical request"
        ),
        "window": {
            "start_inclusive": _isoformat_utc(start_utc),
            "end_exclusive": _isoformat_utc(end_utc),
            "seconds": (end_utc - start_utc).total_seconds(),
        },
        "duration_histogram_ms": {
            str(duration_ms): count
            for duration_ms, count in checked_histogram.items()
        },
        "statistics": histogram_statistics(checked_histogram),
        "provenance": {
            "semantics": "July 22 production-distribution analyzer grouping",
            "raw_logs": {
                "jsonl_file_count": len(raw_files),
                "byte_count": raw_bytes,
                "content_set_sha256": raw_set_digest.hexdigest(),
            },
            "feed_source_map": feed_provenance,
            "reconciliation": {
                "raw_rows_scanned": raw_rows,
                "rows_inside_window": rows_in_window,
                "duplicate_log_identities_discarded_within_file": (
                    duplicate_log_identities
                ),
                "transcribing_events_in_buffer": transcribing_events,
                "transcribing_events_inside_window": (
                    transcribing_events_in_window
                ),
                "recognized_model_call_events_inside_window": (
                    model_call_events_in_window
                ),
                "other_generate_content_events_inside_window": (
                    other_generate_content_events_in_window
                ),
                "logical_request_groups_all_sources": groups_with_model_call,
                "groups_without_transcribing_event": (
                    groups_without_transcribing_event
                ),
                "groups_with_multiple_transcribing_events": (
                    groups_with_multiple_transcribing_events
                ),
                "groups_with_unmapped_feed": groups_with_unmapped_feed,
                "groups_with_non_bcfy_source": groups_with_non_bcfy_source,
                "bcfy_feeds_logical_requests": sum(checked_histogram.values()),
            },
        },
    }


def extract_and_write(
    *,
    raw_dir: Path,
    feed_sources_path: Path,
    start: datetime.datetime | str,
    end: datetime.datetime | str,
    output_path: Path,
) -> dict[str, Any]:
    """Extract the aggregate and atomically write canonical JSON."""

    payload = extract_bcfy_feeds_geometry(
        raw_dir=raw_dir,
        feed_sources_path=feed_sources_path,
        start=start,
        end=end,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=output_path.parent,
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as stream:
        temporary_path = Path(stream.name)
        json.dump(payload, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary_path, output_path)
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for the one-time aggregate extraction."""

    parser = argparse.ArgumentParser(
        description="Extract privacy-safe BCFY-feeds production geometry.",
    )
    parser.add_argument("--raw-dir", required=True, type=Path)
    parser.add_argument("--feed-sources", required=True, type=Path)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args(argv)
    extract_and_write(
        raw_dir=arguments.raw_dir,
        feed_sources_path=arguments.feed_sources,
        start=arguments.start,
        end=arguments.end,
        output_path=arguments.output,
    )
    return 0


def _read_feed_sources(
    path: Path,
) -> tuple[dict[uuid.UUID, str], dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("invalid feed-source map JSON") from error
    if not isinstance(payload, dict):
        raise ValueError("feed-source map must be an object")
    if payload.get("read_only_repeatable_read") is not True:
        raise ValueError(
            "feed-source map lacks a read-only transaction receipt"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError("feed-source map rows must be a list")
    if payload.get("row_count") != len(rows):
        raise ValueError(
            "feed-source map declared row count does not match rows"
        )

    sources: dict[uuid.UUID, str] = {}
    source_counts: Counter[str] = Counter()
    for row_number, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"feed-source row {row_number} must be an object")
        try:
            feed = uuid.UUID(row["feed_id"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                f"feed-source row {row_number} has an invalid feed ID"
            ) from error
        source = row.get("source_type")
        if not isinstance(source, str) or not source:
            raise ValueError(f"feed-source row {row_number} has no source type")
        if feed in sources:
            raise ValueError(f"duplicate feed in feed-source row {row_number}")
        sources[feed] = source
        source_counts[source] += 1
    if not source_counts["bcfy_feeds"]:
        raise ValueError("feed-source map contains no bcfy_feeds rows")

    return sources, {
        "sha256": _sha256_file(path),
        "captured_at_utc": payload.get("captured_at_utc"),
        "query_sha256": payload.get("query_sha256"),
        "declared_row_count": payload.get("row_count"),
        "observed_unique_rows": len(sources),
        "source_type_counts": dict(sorted(source_counts.items())),
        "scope": (
            "current feed metadata captured after the frozen telemetry window; "
            "used only to select current bcfy_feeds membership"
        ),
    }


def _event_kind(row: Mapping[str, Any]) -> tuple[str | None, str | None]:
    message = row.get("message")
    if isinstance(message, str) and message.startswith("Transcribing "):
        return "transcribing", None

    request_url = row.get("request_url")
    if isinstance(request_url, str) and ":generateContent" in request_url:
        return "model_call", request_url
    if (
        isinstance(message, str)
        and message.startswith("HTTP Request: POST ")
        and ":generateContent" in message
    ):
        match = HTTP_MESSAGE_RE.fullmatch(message)
        if match is None:
            raise ValueError("malformed generateContent HTTP event")
        return "model_call", match.group("url")
    return None, None


def _identity_key(identity: object) -> tuple[str, str, str] | None:
    if not isinstance(identity, dict):
        return None
    values = (
        identity.get("log_name"),
        identity.get("insert_id"),
        identity.get("timestamp"),
    )
    if not all(isinstance(value, str) and value for value in values):
        return None
    return values  # type: ignore[return-value]


def _group_key(row: Mapping[str, Any]) -> tuple[str, str, str] | None:
    values = (row.get("revision"), row.get("trace"), row.get("span"))
    if not all(isinstance(value, str) and value for value in values):
        return None
    return values  # type: ignore[return-value]


def _duration_ms(value: str) -> int:
    seconds = decimal.Decimal(value)
    milliseconds = seconds * 1000
    if milliseconds != milliseconds.to_integral_value():
        raise ValueError(
            "Transcribing duration is not exactly representable in milliseconds"
        )
    result = int(milliseconds)
    if result < 0:
        raise ValueError("Transcribing duration must be non-negative")
    return result


def _bind_transcribing_event(
    group: _RequestGroup,
    *,
    segment: uuid.UUID,
    feed: uuid.UUID,
    duration_ms: int,
) -> None:
    observed = (segment, feed, duration_ms)
    existing = (group.segment, group.feed, group.duration_ms)
    if all(value is None for value in existing):
        group.segment, group.feed, group.duration_ms = observed
        return
    if existing != observed:
        raise ValueError(
            "one revision/trace/span group binds conflicting Transcribing events"
        )


def _validate_histogram(histogram: Mapping[int, int]) -> dict[int, int]:
    checked: dict[int, int] = {}
    for duration_ms, count in histogram.items():
        if (
            isinstance(duration_ms, bool)
            or not isinstance(duration_ms, int)
            or duration_ms < 0
        ):
            raise ValueError(
                "histogram durations must be non-negative integers"
            )
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise ValueError("histogram counts must be positive integers")
        checked[duration_ms] = count
    return dict(sorted(checked.items()))


def _rank_value(histogram: Mapping[int, int], rank: int) -> int:
    cumulative = 0
    for duration_ms, count in histogram.items():
        cumulative += count
        if cumulative > rank:
            return duration_ms
    raise AssertionError("rank not found")


def _json_number(value: fractions.Fraction) -> int | float:
    return value.numerator if value.denominator == 1 else float(value)


def _fraction_text(value: fractions.Fraction) -> str:
    return (
        str(value.numerator)
        if value.denominator == 1
        else f"{value.numerator}/{value.denominator}"
    )


def _as_utc(value: datetime.datetime) -> datetime.datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(datetime.timezone.utc)


def _isoformat_utc(value: datetime.datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
