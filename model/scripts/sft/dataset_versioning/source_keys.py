from __future__ import annotations

import csv
import re
import urllib.parse
from collections.abc import Mapping
from pathlib import Path

from dataset_versioning.types import SourceIdentityError

DEFAULT_ECHO_REGISTRY_PATH = (
    Path(__file__).resolve().parents[3]
    / "data_sources"
    / "echo"
    / "all_echo_mono_streams.csv"
)

_BCFY_CALLS_BASENAME_RE = re.compile(r"(?P<group_id>\d+)_\d+")
_BCFY_FEEDS_ARCHIVE_RE = re.compile(
    r"archives\.broadcastify\.com/(?P<path_feed>\d+)/\d{8}/"
    r"[^/]*-(?P<file_feed>\d+)\.[A-Za-z0-9]+"
)
_ECHO_FILE_RE = re.compile(r"(?P<echo_name>.+?)_\d{8}_\d{2}\.mp3$")


def load_echo_registry(
    path: Path = DEFAULT_ECHO_REGISTRY_PATH,
) -> dict[str, set[str]]:
    registry: dict[str, set[str]] = {}
    with path.open(newline="") as csv_file:
        for row in csv.reader(csv_file):
            if len(row) < 2:
                continue
            area_code = row[0].strip().strip('"')
            echo_name = row[1].strip().strip('"')
            if area_code and echo_name:
                registry.setdefault(echo_name, set()).add(area_code)
    return registry


def resolve_source_group(
    row: Mapping[str, object],
    *,
    dataset_family: str,
    source_strategy: str,
    echo_registry: Mapping[str, set[str]] | None = None,
    source_map: Mapping[str, Mapping[str, str]] | None = None,
) -> str:
    if source_strategy == "bcfy_calls":
        return _resolve_bcfy_calls(row)
    if source_strategy == "bcfy_feeds":
        return _resolve_bcfy_feeds(row)
    if source_strategy == "echo":
        return _resolve_echo(
            row, echo_registry=echo_registry, source_map=source_map
        )
    if source_strategy == "fire_notifications":
        return _resolve_fire_notifications(row)
    raise SourceIdentityError(
        f"unsupported source_strategy={source_strategy} "
        f"for dataset_family={dataset_family}"
    )


def _resolve_bcfy_calls(row: Mapping[str, object]) -> str:
    group_id = _first_text(
        row,
        (
            "groupId",
            "group_id",
            "group_id_pool",
            "source_group_id",
            "source_feed_id",
        ),
    )
    if group_id:
        return f"bcfy_calls:{group_id}"

    for uri in _candidate_uris(row):
        match = _BCFY_CALLS_BASENAME_RE.search(_basename(uri))
        if match:
            return f"bcfy_calls:{match.group('group_id')}"
    raise SourceIdentityError("bcfy_calls source group could not be resolved")


def _resolve_bcfy_feeds(row: Mapping[str, object]) -> str:
    feed_id = _first_text(row, ("feedId", "feed_id", "feedID"))
    if feed_id:
        return f"bcfy_feeds:{feed_id}"

    for uri in _candidate_uris(row):
        match = _BCFY_FEEDS_ARCHIVE_RE.search(uri)
        if not match:
            continue
        path_feed = match.group("path_feed")
        file_feed = match.group("file_feed")
        if path_feed != file_feed:
            raise SourceIdentityError(
                f"bcfy_feeds archive feed IDs disagree: "
                f"path={path_feed} filename={file_feed}"
            )
        return f"bcfy_feeds:{path_feed}"
    raise SourceIdentityError("bcfy_feeds source group could not be resolved")


def _resolve_echo(
    row: Mapping[str, object],
    *,
    echo_registry: Mapping[str, set[str]] | None,
    source_map: Mapping[str, Mapping[str, str]] | None,
) -> str:
    area_code = _first_text(row, ("area_code",))
    echo_name = _first_text(row, ("echo_name",))
    if area_code and echo_name:
        return f"echo:{area_code}/{echo_name}"

    device_name = _first_text(row, ("device_name",))
    filename_prefix = _first_text(row, ("filename_prefix",))
    if device_name and filename_prefix:
        return f"echo:{device_name}/{filename_prefix}"

    source_map_match = _lookup_echo_source_map(row, source_map)
    if source_map_match:
        return source_map_match

    for uri in _candidate_uris(row):
        parsed = _parse_echo_uri(uri)
        if parsed:
            parsed_area, parsed_name = parsed
            return f"echo:{parsed_area}/{parsed_name}"

    if echo_name:
        registry = echo_registry if echo_registry is not None else load_echo_registry()
        areas = set(registry.get(echo_name, set()))
        if len(areas) == 1:
            return f"echo:{next(iter(areas))}/{echo_name}"
        if len(areas) > 1:
            raise SourceIdentityError(f"ambiguous echo_name: {echo_name}")

    raise SourceIdentityError("echo source group could not be resolved")


def _lookup_echo_source_map(
    row: Mapping[str, object],
    source_map: Mapping[str, Mapping[str, str]] | None,
) -> str | None:
    if source_map is None:
        return None
    for uri in _candidate_uris(row):
        mapped = source_map.get(uri)
        if not mapped:
            continue
        area_code = mapped.get("area_code")
        echo_name = mapped.get("echo_name")
        if area_code and echo_name:
            return f"echo:{area_code}/{echo_name}"
    return None


def _parse_echo_uri(uri: str) -> tuple[str, str] | None:
    path = _uri_path(uri)
    parts = [part for part in path.split("/") if part]
    if len(parts) < 3:
        return None
    area_code = parts[-3]
    date_part = parts[-2]
    filename = parts[-1]
    if not re.fullmatch(r"\d{8}", date_part):
        return None
    match = _ECHO_FILE_RE.match(filename)
    if not match:
        return None
    return area_code, match.group("echo_name")


def _resolve_fire_notifications(row: Mapping[str, object]) -> str:
    stream_path = _first_text(row, ("stream_path", "location", "source_location"))
    if stream_path:
        return f"fire_notifications:{stream_path.strip('/')}"

    original_path = _first_text(row, ("original_path",))
    if original_path:
        parts = [part for part in _uri_path(original_path).split("/") if part]
        if len(parts) >= 2:
            return f"fire_notifications:{parts[0]}/{parts[1]}"

    raise SourceIdentityError(
        "fire_notifications source group could not be resolved"
    )


def _candidate_uris(row: Mapping[str, object]) -> list[str]:
    uris: list[str] = []
    for key in (
        "audio_filepath",
        "audio_uri",
        "fileUri",
        "url",
        "s3_url",
        "original_audio_uri",
    ):
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            uris.append(text)
    return uris


def _first_text(row: Mapping[str, object], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, list | tuple | set | dict):
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _basename(uri: str) -> str:
    path = _uri_path(uri)
    return path.rsplit("/", 1)[-1]


def _uri_path(uri: str) -> str:
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme and parsed.path:
        return urllib.parse.unquote(parsed.path).lstrip("/")
    return urllib.parse.unquote(uri).lstrip("/")
