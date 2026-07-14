"""Contract tests for the exact checked-in SID evidence reducers."""

from __future__ import annotations

import copy
import datetime
import json
import pathlib
import shutil
import subprocess
import unittest

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_REDUCER_DIR = _ROOT / "scripts" / "operations" / "bcfy_calls_sid"
_FIXTURE_DIR = (
    _ROOT / "scripts" / "tests" / "fixtures" / "bcfy_calls_sid_evidence"
)
_MANIFEST_PATH = _FIXTURE_DIR / "manifest.json"


def _load_json(path: pathlib.Path) -> dict[str, object]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        message = f"{path} must contain one JSON object"
        raise TypeError(message)
    return value


def _parse_timestamp(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)


def _format_timestamp(value: datetime.datetime) -> str:
    if value.microsecond:
        rendered = value.isoformat(timespec="microseconds")
    else:
        rendered = value.isoformat(timespec="seconds")
    return rendered.replace("+00:00", "Z")


def _offset_timestamp(start: str, microseconds: int) -> str:
    value = _parse_timestamp(start) + datetime.timedelta(
        microseconds=microseconds
    )
    return _format_timestamp(value)


def _poll_payload(
    *,
    sid: str,
    occurrence: int,
    last_pos: int,
) -> dict[str, object]:
    return {
        "event_type": "bcfy_calls_sid_poll",
        "schema_version": 1,
        "sid": sid,
        "source_type": "bcfy_calls",
        "owner_worker_id": "fixture-worker-a",
        "fencing_token": 10,
        "outcome": "success",
        "provider_observed": True,
        "http_attempt_count": 1,
        "response_row_count": 1,
        "response_distinct_audio_url_count": 1,
        "request_pos": last_pos if occurrence == 0 else last_pos - 1,
        "response_last_pos": last_pos,
        "response_last_pos_state": "valid",
        "lease_cursor_lag_seconds": 5,
        "maximum_feed_cursor_lag_seconds": 4,
        "null_feed_cursor_count": 0,
        "pressure_encountered": False,
        "pressure_wait_count": 0,
        "oldest_queue_age_seconds": 0,
        "maximum_queue_wait_seconds": 0,
        "maximum_queue_depth": 0,
        "maximum_worker_utilization_numerator": 1,
        "worker_utilization_denominator": 4,
        "fence_rejection_count": 0,
        "membership_rejection_count": 0,
    }


def _entry(
    *,
    timestamp: str,
    insert_id: str,
    instance_id: str,
    payload: dict[str, object],
) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "insertId": insert_id,
        "resource": {
            "type": "gce_instance",
            "labels": {"instance_id": instance_id},
        },
        "jsonPayload": payload,
    }


def _pages(entry_count: int) -> list[dict[str, object]]:
    first_count = entry_count // 2
    second_count = entry_count - first_count
    return [
        {
            "page_number": 1,
            "request_page_token": None,
            "next_page_token": "fixture-page-2",
            "page_complete": False,
            "entry_count": first_count,
        },
        {
            "page_number": 2,
            "request_page_token": "fixture-page-2",
            "next_page_token": None,
            "page_complete": True,
            "entry_count": second_count,
        },
    ]


def _cyclic_pages(entry_count: int) -> list[dict[str, object]]:
    base_count = entry_count // 4
    counts = [base_count, base_count, base_count]
    counts.append(entry_count - sum(counts))
    return [
        {
            "page_number": 1,
            "request_page_token": None,
            "next_page_token": "fixture-cycle-a",
            "page_complete": False,
            "entry_count": counts[0],
        },
        {
            "page_number": 2,
            "request_page_token": "fixture-cycle-a",
            "next_page_token": "fixture-cycle-b",
            "page_complete": False,
            "entry_count": counts[1],
        },
        {
            "page_number": 3,
            "request_page_token": "fixture-cycle-b",
            "next_page_token": "fixture-cycle-a",
            "page_complete": False,
            "entry_count": counts[2],
        },
        {
            "page_number": 4,
            "request_page_token": "fixture-cycle-a",
            "next_page_token": None,
            "page_complete": True,
            "entry_count": counts[3],
        },
    ]


def _collection_document(
    *,
    entries: list[dict[str, object]],
    window_start: str,
    window_end: str,
    event_types: list[str],
    instance_ids: list[str],
) -> dict[str, object]:
    collection = {
        "schema_version": 1,
        "collection_complete": True,
        "limit_reached": False,
        "result_limit": len(entries) + 100,
        "returned_count": len(entries),
        "query_event_types": event_types,
        "frozen_instance_ids": instance_ids,
        "filter_instance_ids": instance_ids,
        "window_start": window_start,
        "window_end": window_end,
        "page_count": 2,
        "pages": _pages(len(entries)),
    }
    return {"collection": collection, "entries": entries}


def _refresh_collection(document: dict[str, object]) -> None:
    entries = document["entries"]
    collection = document["collection"]
    if not isinstance(entries, list) or not isinstance(collection, dict):
        message = "fixture document shape drifted"
        raise TypeError(message)
    collection["returned_count"] = len(entries)
    collection["result_limit"] = len(entries) + 100
    collection["page_count"] = 2
    collection["pages"] = _pages(len(entries))


def _bootstrap_document(
    manifest: dict[str, object],
) -> dict[str, object]:
    defaults = manifest["defaults"]
    expected_sids = manifest["expected_sids"]
    if not isinstance(defaults, dict) or not isinstance(expected_sids, list):
        message = "fixture manifest shape drifted"
        raise TypeError(message)
    instance_ids = defaults["frozen_instance_ids"]
    if not isinstance(instance_ids, list):
        message = "fixture instance list drifted"
        raise TypeError(message)
    entries: list[dict[str, object]] = []
    for index, sid_value in enumerate(expected_sids):
        sid = str(sid_value)
        second = 10 + index
        timestamp = _offset_timestamp(
            str(defaults["bootstrap_window_start"]),
            second * 1_000_000,
        )
        last_pos = 2_000_000_000 + index
        payload = _poll_payload(
            sid=sid,
            occurrence=0,
            last_pos=last_pos,
        )
        insert_id = f"qualifying-{sid}-a"
        entries.append(
            _entry(
                timestamp=timestamp,
                insert_id=insert_id,
                instance_id=str(instance_ids[index % len(instance_ids)]),
                payload=payload,
            )
        )
        if index == 0:
            tied_payload = copy.deepcopy(payload)
            entries.append(
                _entry(
                    timestamp=timestamp,
                    insert_id=f"qualifying-{sid}-b",
                    instance_id=str(instance_ids[1]),
                    payload=tied_payload,
                )
            )
            fractional_payload = copy.deepcopy(payload)
            entries.append(
                _entry(
                    timestamp=_offset_timestamp(
                        str(defaults["bootstrap_window_start"]),
                        10_100_000,
                    ),
                    insert_id=f"qualifying-{sid}-fractional",
                    instance_id=str(instance_ids[0]),
                    payload=fractional_payload,
                )
            )
            failed_payload = copy.deepcopy(payload)
            failed_payload["outcome"] = "provider_failed"
            failed_payload["provider_observed"] = False
            failed_payload["response_row_count"] = None
            failed_payload["response_distinct_audio_url_count"] = None
            failed_payload["response_last_pos"] = None
            failed_payload["response_last_pos_state"] = "not_observed"
            entries.append(
                _entry(
                    timestamp=_offset_timestamp(
                        str(defaults["bootstrap_window_start"]),
                        5_000_000,
                    ),
                    insert_id="nonqualifying-81001",
                    instance_id=str(instance_ids[0]),
                    payload=failed_payload,
                )
            )
    entries.reverse()
    return _collection_document(
        entries=entries,
        window_start=str(defaults["bootstrap_window_start"]),
        window_end=str(defaults["bootstrap_window_end"]),
        event_types=["bcfy_calls_sid_poll"],
        instance_ids=[str(value) for value in instance_ids],
    )


def _soak_document(
    manifest: dict[str, object],
    poll_count: int,
) -> dict[str, object]:
    defaults = manifest["defaults"]
    expected_sids = manifest["expected_sids"]
    if not isinstance(defaults, dict) or not isinstance(expected_sids, list):
        message = "fixture manifest shape drifted"
        raise TypeError(message)
    instance_ids = defaults["frozen_instance_ids"]
    if not isinstance(instance_ids, list):
        message = "fixture instance list drifted"
        raise TypeError(message)
    occurrences = {str(sid): 0 for sid in expected_sids}
    entries: list[dict[str, object]] = []
    for index in range(poll_count):
        sid = str(expected_sids[index % len(expected_sids)])
        occurrence = occurrences[sid]
        occurrences[sid] += 1
        offset = (index * 1_800_000_000) // poll_count
        last_pos = 2_100_000_000 + occurrence
        payload = _poll_payload(
            sid=sid,
            occurrence=occurrence,
            last_pos=last_pos,
        )
        entries.append(
            _entry(
                timestamp=_offset_timestamp(
                    str(defaults["soak_start"]),
                    offset,
                ),
                insert_id=f"poll-{index:04d}",
                instance_id=str(instance_ids[index % len(instance_ids)]),
                payload=payload,
            )
        )
    entries.reverse()
    return _collection_document(
        entries=entries,
        window_start=str(defaults["soak_start"]),
        window_end=str(defaults["soak_end"]),
        event_types=[
            "bcfy_calls_replay_window_truncated",
            "bcfy_calls_sid_poll",
        ],
        instance_ids=[str(value) for value in instance_ids],
    )


def _payload(entry: dict[str, object]) -> dict[str, object]:
    payload = entry["jsonPayload"]
    if not isinstance(payload, dict):
        message = "fixture payload shape drifted"
        raise TypeError(message)
    return payload


def _entries(document: dict[str, object]) -> list[dict[str, object]]:
    entries = document["entries"]
    if not isinstance(entries, list):
        message = "fixture entry shape drifted"
        raise TypeError(message)
    return entries


def _entries_for_sid(
    document: dict[str, object],
    sid: str,
) -> list[dict[str, object]]:
    matches = [
        entry for entry in _entries(document) if _payload(entry)["sid"] == sid
    ]
    return sorted(
        matches,
        key=lambda entry: _parse_timestamp(str(entry["timestamp"])),
    )


def _apply_common_scenario(  # noqa: PLR0911, PLR0912
    document: dict[str, object],
    scenario: str,
    expected_sids: list[str],
) -> bool:
    entries = _entries(document)
    if scenario == "missing_sid":
        document["entries"] = [
            entry
            for entry in entries
            if _payload(entry)["sid"] != expected_sids[-1]
        ]
    elif scenario == "unexpected_sid":
        _payload(entries[0])["sid"] = "89999"
    elif scenario == "duplicate_identity":
        entries.append(copy.deepcopy(entries[0]))
    elif scenario == "malformed_denominator":
        _payload(entries[0])["response_distinct_audio_url_count"] = "bad"
    elif scenario == "incomplete_collection":
        collection = document["collection"]
        if isinstance(collection, dict):
            collection["collection_complete"] = False
        return True
    elif scenario == "missing_page_token":
        collection = document["collection"]
        if isinstance(collection, dict):
            pages = collection["pages"]
            if isinstance(pages, list) and isinstance(pages[1], dict):
                pages[1]["request_page_token"] = None
        return True
    elif scenario == "missing_final_page_completion":
        collection = document["collection"]
        if isinstance(collection, dict):
            pages = collection["pages"]
            if isinstance(pages, list) and isinstance(pages[-1], dict):
                del pages[-1]["page_complete"]
        return True
    elif scenario == "repeated_page_token":
        collection = document["collection"]
        if isinstance(collection, dict):
            collection["page_count"] = 4
            collection["pages"] = _cyclic_pages(len(entries))
        return True
    elif scenario == "result_at_limit":
        collection = document["collection"]
        if isinstance(collection, dict):
            collection["result_limit"] = len(entries)
        return True
    elif scenario == "missing_instance_scope":
        resource = entries[0]["resource"]
        if isinstance(resource, dict):
            labels = resource["labels"]
            if isinstance(labels, dict):
                del labels["instance_id"]
        return True
    elif scenario == "filter_scope_mismatch":
        collection = document["collection"]
        if isinstance(collection, dict):
            collection["filter_instance_ids"] = ["fixture-instance-001"]
        return True
    elif scenario == "out_of_window":
        collection = document["collection"]
        if isinstance(collection, dict):
            entries[0]["timestamp"] = collection["window_end"]
        return True
    else:
        return False
    _refresh_collection(document)
    return True


def _apply_bootstrap_scenario(
    document: dict[str, object],
    scenario: str,
    expected_sids: list[str],
) -> None:
    if scenario == "baseline":
        return
    if not _apply_common_scenario(document, scenario, expected_sids):
        message = f"unknown bootstrap scenario: {scenario}"
        raise ValueError(message)


def _set_first_sid_entries(
    document: dict[str, object],
    expected_sids: list[str],
    count: int,
    update: dict[str, object],
) -> None:
    selected = _entries_for_sid(document, expected_sids[0])[:count]
    for entry in selected:
        _payload(entry).update(update)


def _apply_soak_scenario(  # noqa: PLR0912, PLR0915
    document: dict[str, object],
    scenario: str,
    expected_sids: list[str],
) -> None:
    entries = _entries(document)
    if scenario == "baseline":
        return
    if _apply_common_scenario(document, scenario, expected_sids):
        return
    if scenario == "all_ratio_boundaries":
        for entry in entries[:675]:
            payload = _payload(entry)
            payload["http_attempt_count"] = 2
            payload["response_row_count"] = 2
        ordered = _entries_for_sid(document, expected_sids[0])
        first_pos = _payload(ordered[0])["response_last_pos"]
        _payload(ordered[1])["response_last_pos"] = first_pos
        _payload(ordered[1])["request_pos"] = first_pos
    elif scenario == "attempt_ratio_above":
        for entry in entries[:676]:
            _payload(entry)["http_attempt_count"] = 2
    elif scenario == "row_ratio_above":
        for entry in entries[:676]:
            _payload(entry)["response_row_count"] = 2
    elif scenario == "zero_over_zero":
        for entry in entries:
            payload = _payload(entry)
            payload["response_row_count"] = 0
            payload["response_distinct_audio_url_count"] = 0
    elif scenario == "positive_over_zero":
        for entry in entries:
            payload = _payload(entry)
            payload["response_row_count"] = 0
            payload["response_distinct_audio_url_count"] = 0
        _payload(entries[0])["response_row_count"] = 1
    elif scenario == "last_pos_regression":
        ordered = _entries_for_sid(document, expected_sids[0])
        first_pos = int(_payload(ordered[0])["response_last_pos"])
        _payload(ordered[1])["response_last_pos"] = first_pos - 1
        _payload(ordered[1])["request_pos"] = first_pos - 1
    elif scenario == "isolated_hazards":
        _payload(entries[0]).update(
            {"pressure_encountered": True, "pressure_wait_count": 1}
        )
        _payload(entries[1])["fence_rejection_count"] = 1
        _payload(entries[2])["membership_rejection_count"] = 1
        _payload(entries[3])["lease_cursor_lag_seconds"] = 31
        invalid_payload = _payload(entries[5])
        invalid_payload["response_last_pos"] = None
        invalid_payload["response_last_pos_state"] = "invalid"
        _payload(entries[4])["lease_cursor_lag_seconds"] = None
        ordered = _entries_for_sid(document, expected_sids[4])
        for entry in ordered[1:]:
            _payload(entry).update(
                {
                    "fencing_token": 11,
                    "owner_worker_id": "fixture-worker-b",
                }
            )
    elif scenario == "sustained_pressure":
        _set_first_sid_entries(
            document,
            expected_sids,
            6,
            {"pressure_encountered": True, "pressure_wait_count": 1},
        )
    elif scenario == "sustained_cursor_lag":
        _set_first_sid_entries(
            document,
            expected_sids,
            6,
            {"lease_cursor_lag_seconds": 31},
        )
    elif scenario == "sustained_membership_rejection":
        _set_first_sid_entries(
            document,
            expected_sids,
            6,
            {"membership_rejection_count": 1},
        )
    elif scenario == "repeated_reacquisition":
        ordered = _entries_for_sid(document, expected_sids[0])
        for index, entry in enumerate(ordered[:7]):
            _payload(entry).update(
                {
                    "fencing_token": 10 + index,
                    "owner_worker_id": f"fixture-worker-{index % 2}",
                }
            )
        for entry in ordered[7:]:
            _payload(entry).update(
                {
                    "fencing_token": 16,
                    "owner_worker_id": "fixture-worker-0",
                }
            )
    elif scenario == "replay_truncation":
        defaults_timestamp = "2026-07-14T00:20:00Z"
        replay_payload = {
            "event_type": "bcfy_calls_replay_window_truncated",
            "schema_version": 1,
            "sid": expected_sids[0],
            "source_type": "bcfy_calls",
            "owner_worker_id": "fixture-worker-a",
            "fencing_token": 10,
            "cause": "fixture_replay_floor",
        }
        entries.append(
            _entry(
                timestamp=defaults_timestamp,
                insert_id="replay-truncation-001",
                instance_id="fixture-instance-001",
                payload=replay_payload,
            )
        )
        _refresh_collection(document)
    elif scenario == "missing_success":
        for entry in _entries_for_sid(document, expected_sids[0]):
            _payload(entry)["outcome"] = "provider_failed"
    elif scenario == "valid_request_regression":
        payload = _payload(entries[0])
        payload["request_pos"] = int(payload["response_last_pos"]) + 1
    elif scenario == "missing_lag_key":
        del _payload(entries[0])["lease_cursor_lag_seconds"]
    else:
        message = f"unknown soak scenario: {scenario}"
        raise ValueError(message)


def _run_jq(
    *,
    reducer: pathlib.Path,
    document: dict[str, object],
    arguments: list[tuple[str, object, bool]],
) -> subprocess.CompletedProcess[str]:
    jq = shutil.which("jq")
    if jq is None:
        message = "jq is required for the evidence contract tests"
        raise RuntimeError(message)
    command = [jq]
    for name, value, is_json in arguments:
        command.extend(["--argjson" if is_json else "--arg", name])
        command.append(json.dumps(value) if is_json else str(value))
    command.extend(["-f", str(reducer)])
    return subprocess.run(
        command,
        input=json.dumps(document, separators=(",", ":")),
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )


class BcfyCallsSidEvidenceTest(unittest.TestCase):
    """Execute every manifest-owned case through the checked-in jq."""

    def setUp(self) -> None:
        self.manifest = _load_json(_MANIFEST_PATH)
        expected_sids = self.manifest["expected_sids"]
        defaults = self.manifest["defaults"]
        if not isinstance(expected_sids, list) or not isinstance(
            defaults, dict
        ):
            self.fail("manifest structure is invalid")
        self.expected_sids = [str(value) for value in expected_sids]
        self.defaults = defaults

    def test_manifest_owns_exact_sanitized_fixture_corpus(self) -> None:
        owned = self.manifest["owned_files"]
        self.assertIsInstance(owned, list)
        actual = sorted(
            path.name
            for path in _FIXTURE_DIR.iterdir()
            if path.name != "manifest.json"
        )
        self.assertEqual(actual, owned)
        self.assertTrue(self.manifest["sanitized"])
        self.assertEqual(len(self.expected_sids), 19)
        self.assertEqual(self.expected_sids, sorted(set(self.expected_sids)))
        for filename in owned:
            fixture_text = (_FIXTURE_DIR / str(filename)).read_text(
                encoding="utf-8"
            )
            self.assertNotIn("https://", fixture_text)
            self.assertNotIn("http://", fixture_text)
            self.assertNotIn("X-Goog-", fixture_text)
            fixture = _load_json(_FIXTURE_DIR / str(filename))
            cases = fixture["cases"]
            self.assertIsInstance(cases, list)
            self.assertTrue(cases)

    def test_bootstrap_fixture_cases_use_exact_reducer(self) -> None:
        fixture = _load_json(_FIXTURE_DIR / "bootstrap_cases.json")
        cases = fixture["cases"]
        self.assertIsInstance(cases, list)
        for value in cases:
            self.assertIsInstance(value, dict)
            case = value
            with self.subTest(case=case["name"]):
                document = _bootstrap_document(self.manifest)
                _apply_bootstrap_scenario(
                    document,
                    str(case["scenario"]),
                    self.expected_sids,
                )
                result = _run_jq(
                    reducer=_REDUCER_DIR / "bootstrap.jq",
                    document=document,
                    arguments=[
                        ("EXPECTED_SIDS", self.expected_sids, True),
                    ],
                )
                if case["expect"] == "reject":
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn(str(case["error"]), result.stderr)
                    continue
                self.assertEqual(result.returncode, 0, result.stderr)
                output = json.loads(result.stdout)
                self.assertEqual(output["status"], "pass")
                self.assertEqual(output["expected_sid_count"], 19)
                self.assertEqual(output["observed_sid_count"], 19)
                self.assertEqual(
                    output["BOOTSTRAP_END"],
                    case["expected_bootstrap_end"],
                )
                retained = output["retained_first_events"]
                self.assertEqual(
                    [event["sid"] for event in retained],
                    self.expected_sids,
                )
                self.assertEqual(
                    retained[0]["insert_id"],
                    case["expected_first_insert_id"],
                )
                authority = output["authority_evidence"]
                self.assertFalse(authority["configured_authority_evaluated"])
                self.assertFalse(
                    authority["direct_wire_selector_absence_evaluated"]
                )

    def test_soak_fixture_cases_use_exact_reducer(self) -> None:
        fixture = _load_json(_FIXTURE_DIR / "soak_cases.json")
        cases = fixture["cases"]
        self.assertIsInstance(cases, list)
        for value in cases:
            self.assertIsInstance(value, dict)
            case = value
            with self.subTest(case=case["name"]):
                document = _soak_document(
                    self.manifest,
                    int(case["poll_count"]),
                )
                _apply_soak_scenario(
                    document,
                    str(case["scenario"]),
                    self.expected_sids,
                )
                bootstrap_end = self.defaults["bootstrap_end"]
                if case.get("bootstrap_end_equals_soak_start"):
                    bootstrap_end = self.defaults["soak_start"]
                soak_end = self.defaults["soak_end"]
                if case.get("short_window"):
                    soak_end = "2026-07-14T00:39:59Z"
                arguments = [
                    ("EXPECTED_SIDS", self.expected_sids, True),
                    ("BOOTSTRAP_END", bootstrap_end, False),
                    ("SOAK_START", self.defaults["soak_start"], False),
                    ("SOAK_END", soak_end, False),
                    (
                        "MISSING_SUCCESS_THRESHOLD_SECONDS",
                        case.get(
                            "missing_success_threshold_seconds",
                            self.defaults["missing_success_threshold_seconds"],
                        ),
                        True,
                    ),
                    (
                        "CURSOR_LAG_THRESHOLD_SECONDS",
                        case.get(
                            "cursor_lag_threshold_seconds",
                            self.defaults["cursor_lag_threshold_seconds"],
                        ),
                        True,
                    ),
                    (
                        "SUSTAINED_CONSECUTIVE_POLLS",
                        case.get(
                            "sustained_consecutive_polls",
                            self.defaults["sustained_consecutive_polls"],
                        ),
                        True,
                    ),
                    (
                        "SUSTAINED_SECONDS",
                        case.get(
                            "sustained_seconds",
                            self.defaults["sustained_seconds"],
                        ),
                        True,
                    ),
                ]
                result = _run_jq(
                    reducer=_REDUCER_DIR / "soak.jq",
                    document=document,
                    arguments=arguments,
                )
                if case["expect"] == "reject":
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn(str(case["error"]), result.stderr)
                    if case.get("expect_drill_down"):
                        report = json.loads(result.stderr)
                        self.assertEqual(report["status"], "fail")
                        self.assertTrue(report["aggregate_context"])
                        self.assertTrue(report["drill_down"])
                    continue
                self.assertEqual(result.returncode, 0, result.stderr)
                output = json.loads(result.stdout)
                self.assertEqual(output["status"], "pass")
                self.assertEqual(output["gates"]["expected_sid_count"], 19)
                if "expected_polls_per_minute" in case:
                    self.assertEqual(
                        output["gates"]["logical_polls_per_minute"],
                        case["expected_polls_per_minute"],
                    )
                if "expected_attempt_ratio" in case:
                    self.assertEqual(
                        output["gates"]["attempts_per_logical_poll"],
                        case["expected_attempt_ratio"],
                    )
                if "expected_row_ratio" in case:
                    self.assertEqual(
                        output["gates"][
                            "response_rows_per_summed_per_response_distinct_url"
                        ],
                        case["expected_row_ratio"],
                    )
                if "expected_zero_over_zero" in case:
                    self.assertEqual(
                        output["gates"][
                            "zero_over_zero_row_amplification_passed"
                        ],
                        case["expected_zero_over_zero"],
                    )
                if "expected_hazards" in case:
                    observed_hazards = sorted(
                        {
                            hazard
                            for event in output["drill_down"]["retained_events"]
                            for hazard in event["hazards"]
                        }
                    )
                    self.assertEqual(
                        observed_hazards,
                        case["expected_hazards"],
                    )
                self.assertEqual(
                    output["denominator_interpretation"],
                    "sum_of_exact_per_response_distinct_audio_url_counts",
                )
                self.assertFalse(
                    output["window_wide_cross_poll_uniqueness_evaluated"]
                )
                authority = output["authority_evidence"]
                self.assertFalse(authority["configured_authority_evaluated"])
                self.assertFalse(
                    authority["direct_wire_selector_absence_evaluated"]
                )


if __name__ == "__main__":
    unittest.main()
