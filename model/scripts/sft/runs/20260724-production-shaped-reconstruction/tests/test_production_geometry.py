"""Tests for privacy-safe production request geometry extraction."""

from __future__ import annotations

import fractions
import json
import pathlib
import sys
import uuid

import pytest


RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(RUN_ROOT) not in sys.path:
    sys.path.insert(0, str(RUN_ROOT))

from dataset_run import production_geometry  # noqa: E402


START = "2026-07-17T20:45:29.481576Z"
END = "2026-07-21T18:14:46.293733Z"
BCFY_FEED = uuid.UUID("00de748d-54fc-42b6-8d35-97cfc6803997")
CALLS_FEED = uuid.UUID("10de748d-54fc-42b6-8d35-97cfc6803997")


def _identity(timestamp: str, insert_id: str) -> dict[str, str]:
    return {
        "timestamp": timestamp,
        "insert_id": insert_id,
        "log_name": "projects/test/logs/stdout",
    }


def _row(
    *,
    timestamp: str,
    insert_id: str,
    trace: str,
    span: str,
    message: str = "unrelated",
    request_url: str | None = None,
) -> dict[str, object]:
    return {
        "identity": _identity(timestamp, insert_id),
        "revision": "revision-1",
        "trace": trace,
        "span": span,
        "message": message,
        "request_url": request_url,
    }


def _transcribing(
    *,
    timestamp: str,
    insert_id: str,
    trace: str,
    span: str,
    feed: uuid.UUID,
    segment: uuid.UUID,
    duration: str,
) -> dict[str, object]:
    uri = (
        "gs://ingestion-canonical-bucket/lossless/"
        f"{feed}/2026/07/20/{segment}.flac"
    )
    return _row(
        timestamp=timestamp,
        insert_id=insert_id,
        trace=trace,
        span=span,
        message=f"Transcribing {duration}s of audio from GCS URI: {uri}",
    )


def _model_call(
    *,
    timestamp: str,
    insert_id: str,
    trace: str,
    span: str,
) -> dict[str, object]:
    return _row(
        timestamp=timestamp,
        insert_id=insert_id,
        trace=trace,
        span=span,
        request_url=(
            "https://example.test/v1/projects/p/locations/us-central1/"
            "endpoints/123:generateContent"
        ),
    )


def _write_inputs(
    tmp_path: pathlib.Path,
    rows: list[dict[str, object]],
) -> tuple[pathlib.Path, pathlib.Path]:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    raw_path = raw_dir / "part.jsonl"
    raw_path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows),
        encoding="utf-8",
    )
    feed_sources = tmp_path / "feed-sources.json"
    feed_sources.write_text(
        json.dumps(
            {
                "read_only_repeatable_read": True,
                "captured_at_utc": "2026-07-23T01:56:20Z",
                "query_sha256": "query-digest",
                "row_count": 2,
                "rows": [
                    {
                        "feed_id": str(BCFY_FEED),
                        "source_type": "bcfy_feeds",
                    },
                    {
                        "feed_id": str(CALLS_FEED),
                        "source_type": "bcfy_calls",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return raw_dir, feed_sources


def test_extracts_one_request_per_group_and_only_bcfy_feeds(
    tmp_path: pathlib.Path,
) -> None:
    segment_a = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    segment_b = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    segment_c = uuid.UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
    rows = [
        # A buffered Transcribing event is bound to a provider call in-window.
        _transcribing(
            timestamp="2026-07-17T20:45:29.000000Z",
            insert_id="a-transcribe",
            trace="trace-a",
            span="span-a",
            feed=BCFY_FEED,
            segment=segment_a,
            duration="1.234",
        ),
        _model_call(
            timestamp="2026-07-17T20:45:30.000000Z",
            insert_id="a-call-1",
            trace="trace-a",
            span="span-a",
        ),
        # A retry is another provider event, not another logical request.
        _model_call(
            timestamp="2026-07-17T20:45:31.000000Z",
            insert_id="a-call-2",
            trace="trace-a",
            span="span-a",
        ),
        _transcribing(
            timestamp="2026-07-18T00:00:00Z",
            insert_id="b-transcribe",
            trace="trace-b",
            span="span-b",
            feed=BCFY_FEED,
            segment=segment_b,
            duration="5.000",
        ),
        _model_call(
            timestamp="2026-07-18T00:00:01Z",
            insert_id="b-call",
            trace="trace-b",
            span="span-b",
        ),
        # A valid non-BCFY request is excluded after the source join.
        _transcribing(
            timestamp="2026-07-18T00:00:02Z",
            insert_id="c-transcribe",
            trace="trace-c",
            span="span-c",
            feed=CALLS_FEED,
            segment=segment_c,
            duration="7.000",
        ),
        _model_call(
            timestamp="2026-07-18T00:00:03Z",
            insert_id="c-call",
            trace="trace-c",
            span="span-c",
        ),
        # Structurally unrelated malformed content is ignored.
        {"identity": {"timestamp": "not-a-time"}, "message": "unrelated"},
    ]
    raw_dir, feed_sources = _write_inputs(tmp_path, rows)

    payload = production_geometry.extract_bcfy_feeds_geometry(
        raw_dir=raw_dir,
        feed_sources_path=feed_sources,
        start=START,
        end=END,
    )

    assert payload["duration_histogram_ms"] == {"1234": 1, "5000": 1}
    assert payload["statistics"]["request_count"] == 2
    reconciliation = payload["provenance"]["reconciliation"]
    assert reconciliation["recognized_model_call_events_inside_window"] == 4
    assert reconciliation["logical_request_groups_all_sources"] == 3
    assert reconciliation["groups_with_non_bcfy_source"] == 1

    # The aggregate must not serialize any row-level correlation or media data.
    serialized = json.dumps(payload)
    for forbidden in (
        str(BCFY_FEED),
        str(segment_a),
        "trace-a",
        "span-a",
        "gs://",
        str(raw_dir),
    ):
        assert forbidden not in serialized


def test_provider_before_transcribing_is_order_independent(
    tmp_path: pathlib.Path,
) -> None:
    segment = uuid.UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
    rows = [
        _model_call(
            timestamp="2026-07-18T00:00:00Z",
            insert_id="call",
            trace="trace",
            span="span",
        ),
        _transcribing(
            timestamp="2026-07-18T00:00:01Z",
            insert_id="transcribe",
            trace="trace",
            span="span",
            feed=BCFY_FEED,
            segment=segment,
            duration="2.500",
        ),
    ]
    raw_dir, feed_sources = _write_inputs(tmp_path, rows)

    payload = production_geometry.extract_bcfy_feeds_geometry(
        raw_dir=raw_dir,
        feed_sources_path=feed_sources,
        start=START,
        end=END,
    )

    assert payload["duration_histogram_ms"] == {"2500": 1}


@pytest.mark.parametrize(
    "message",
    [
        "Transcribing not-a-duration",
        (
            "Transcribing 1.000s of audio from GCS URI: "
            "gs://other-bucket/audio.flac"
        ),
    ],
)
def test_malformed_matched_transcribing_event_fails_loudly(
    tmp_path: pathlib.Path,
    message: str,
) -> None:
    rows = [
        _row(
            timestamp="2026-07-18T00:00:00Z",
            insert_id="bad",
            trace="trace",
            span="span",
            message=message,
        )
    ]
    raw_dir, feed_sources = _write_inputs(tmp_path, rows)

    with pytest.raises(ValueError, match="malformed|unbound"):
        production_geometry.extract_bcfy_feeds_geometry(
            raw_dir=raw_dir,
            feed_sources_path=feed_sources,
            start=START,
            end=END,
        )


def test_exact_ecdf_and_cost_helpers() -> None:
    left = {0: 1, 10: 1}
    right = {5: 2}

    assert production_geometry.exact_ecdf(left) == (
        (0, 1, 2),
        (10, 2, 2),
    )
    assert production_geometry.ecdf_at(left, 5) == fractions.Fraction(1, 2)
    assert (
        production_geometry.type7_quantile_ms(
            {1: 1, 5: 1},
            fractions.Fraction(1, 4),
        )
        == 2
    )
    assert production_geometry.exact_distribution_cost(
        left,
        right,
    ) == production_geometry.DistributionCost(
        wasserstein_1_ms=fractions.Fraction(5),
        maximum_cdf_gap=fractions.Fraction(1, 2),
    )


def test_cli_writes_canonical_json(tmp_path: pathlib.Path) -> None:
    segment = uuid.UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
    rows = [
        _transcribing(
            timestamp="2026-07-18T00:00:00Z",
            insert_id="transcribe",
            trace="trace",
            span="span",
            feed=BCFY_FEED,
            segment=segment,
            duration="3.000",
        ),
        _model_call(
            timestamp="2026-07-18T00:00:01Z",
            insert_id="call",
            trace="trace",
            span="span",
        ),
    ]
    raw_dir, feed_sources = _write_inputs(tmp_path, rows)
    output = tmp_path / "geometry.json"

    assert (
        production_geometry.main(
            [
                "--raw-dir",
                str(raw_dir),
                "--feed-sources",
                str(feed_sources),
                "--start",
                START,
                "--end",
                END,
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert json.loads(output.read_text(encoding="utf-8"))[
        "duration_histogram_ms"
    ] == {"3000": 1}
