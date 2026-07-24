"""Tests for immutable source inventory and continuous FLAC slicing."""

# ruff: noqa: EM101, INP001, S108, TC003, TRY003

from __future__ import annotations

import base64
import dataclasses
import hashlib
import io
import json
import pathlib
import sys
import time
from collections.abc import Callable
from types import SimpleNamespace

import google_crc32c
import numpy
import pytest
import soundfile

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(RUN_ROOT) not in sys.path:
    sys.path.insert(0, str(RUN_ROOT))

from dataset_run import media  # noqa: E402

ENCODER_CONTRACT = RUN_ROOT / "frozen/encoder_toolchain.json"
PINNED_FFMPEG = pathlib.Path("/tmp/next-round-reconstruction/toolchain/ffmpeg")
_SHA_A = "a" * 64
_SHA_B = "b" * 64


def _toolchain(tmp_path: pathlib.Path) -> media.EncoderToolchain:
    return media.EncoderToolchain(
        contract_sha256=_SHA_A,
        soundfile_version="0.13.1",
        soundfile_python_artifact_path=tmp_path / "soundfile.py",
        soundfile_python_artifact_sha256=_SHA_A,
        soundfile_python_artifact_size_bytes=1,
        soundfile_cffi_artifact_path=tmp_path / "_soundfile.py",
        soundfile_cffi_artifact_sha256=_SHA_B,
        soundfile_cffi_artifact_size_bytes=1,
        libsndfile_version="1.2.2",
        libsndfile_binary_path=tmp_path / "libsndfile_x86_64.so",
        libsndfile_binary_sha256=_SHA_B,
        libsndfile_binary_size_bytes=1,
        embedded_libflac_version="1.4.3",
        ffmpeg_executable=tmp_path / "ffmpeg",
        ffmpeg_binary_sha256=_SHA_B,
        ffmpeg_binary_size_bytes=123,
        ffmpeg_version="6.1.1",
        ffmpeg_version_line="ffmpeg version 6.1.1 test",
        ffmpeg_deployed_image_uri=("registry/normalization@sha256:" + _SHA_A),
        ffmpeg_deployed_revision="normalization-test-revision",
    )


def _provider_hashes(payload: bytes) -> tuple[str, str]:
    md5 = hashlib.md5(payload, usedforsecurity=False).digest()
    crc32c = google_crc32c.value(payload).to_bytes(4, "big")
    return (
        base64.b64encode(md5).decode(),
        base64.b64encode(crc32c).decode(),
    )


def _wav_bytes(samples: numpy.ndarray, sample_rate: int = 8_000) -> bytes:
    destination = io.BytesIO()
    soundfile.write(
        destination,
        samples,
        sample_rate,
        format="WAV",
        subtype="PCM_16",
    )
    return destination.getvalue()


@dataclasses.dataclass
class _Blob:
    payload: bytes | None
    generation: int = 7
    reported_size: int | None = None
    md5_hash: str | None = None
    crc32c: str | None = None
    drift_after_download: bool = False
    reloads: int = 0
    download_generations: list[int | None] = dataclasses.field(
        default_factory=list
    )

    @property
    def size(self) -> int | None:
        if self.reported_size is not None:
            return self.reported_size
        return None if self.payload is None else len(self.payload)

    def reload(self, *, if_generation_match: int | None = None) -> None:
        self.reloads += 1
        if self.payload is None:
            raise FileNotFoundError("missing")
        if (
            if_generation_match is not None
            and if_generation_match != self.generation
        ):
            raise RuntimeError("generation precondition")

    def download_to_file(
        self,
        destination: object,
        *,
        if_generation_match: int | None = None,
        checksum: str = "auto",
    ) -> None:
        assert checksum == "auto"
        self.download_generations.append(if_generation_match)
        if if_generation_match != self.generation:
            raise RuntimeError("generation precondition")
        assert self.payload is not None
        destination.write(self.payload)
        if self.drift_after_download:
            self.generation += 1


@dataclasses.dataclass
class _Bucket:
    blobs: dict[str, _Blob]

    def blob(self, name: str) -> _Blob:
        return self.blobs.setdefault(name, _Blob(None))


@dataclasses.dataclass
class _Client:
    buckets: dict[str, _Bucket] = dataclasses.field(default_factory=dict)

    def bucket(self, name: str) -> _Bucket:
        return self.buckets.setdefault(name, _Bucket({}))

    def add(self, uri: str, payload: bytes, **overrides: object) -> _Blob:
        bucket_name, object_name = uri.removeprefix("gs://").split("/", 1)
        md5_hash, crc32c = _provider_hashes(payload)
        arguments = {
            "payload": payload,
            "md5_hash": md5_hash,
            "crc32c": crc32c,
            **overrides,
        }
        blob = _Blob(**arguments)
        self.bucket(bucket_name).blobs[object_name] = blob
        return blob


def test_inventory_and_materialize_exact_continuous_slice(
    tmp_path: pathlib.Path,
) -> None:
    uri = "gs://audio/source.wav"
    samples = numpy.arange(-200, 200, dtype=numpy.int16).reshape(-1, 1)
    payload = _wav_bytes(samples)
    client = _Client()
    blob = client.add(uri, payload)

    source = media.inventory_source(client, uri, tmp_path / "cache")
    output = tmp_path / "request.flac"
    receipt = media.materialize_slice(
        source,
        101,
        303,
        output,
        encoder_profile=media.EncoderProfile.BCFY_FEEDS_SOUNDFILE,
        toolchain=_toolchain(tmp_path),
    )

    decoded, sample_rate = soundfile.read(
        output,
        dtype="int16",
        always_2d=True,
    )
    assert sample_rate == 8_000
    numpy.testing.assert_array_equal(decoded, samples[101:303])
    assert source.format == "WAV"
    assert source.frame_count == 400
    assert source.duration_seconds == "0.05"
    assert source.size_bytes == len(payload)
    assert source.sha256 == hashlib.sha256(payload).hexdigest()
    assert blob.download_generations == [7]
    assert receipt.start_frame == 101
    assert receipt.end_frame == 303
    assert receipt.frame_count == 202
    assert (
        receipt.output_encoded_sha256
        == hashlib.sha256(output.read_bytes()).hexdigest()
    )
    assert receipt.output_encoded_size_bytes == output.stat().st_size
    assert receipt.source_pcm_sha256 == receipt.output_decoded_pcm_sha256
    assert receipt.duration_seconds == "0.02525"
    assert receipt.encoder_binary_sha256 == _SHA_B
    assert receipt.encoder_python_artifact_sha256 == _SHA_A
    assert receipt.encoder_cffi_artifact_sha256 == _SHA_B
    assert receipt.encoder_embedded_libflac_version == "1.4.3"


def test_materialization_reads_encoded_output_once_for_hash_and_decode(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uri = "gs://audio/source.wav"
    samples = numpy.arange(-200, 200, dtype=numpy.int16).reshape(-1, 1)
    client = _Client()
    client.add(uri, _wav_bytes(samples))
    source = media.inventory_source(client, uri, tmp_path / "cache")
    output = tmp_path / "request.flac"
    original_read_bytes = pathlib.Path.read_bytes
    output_reads = 0

    def counted_read_bytes(path: pathlib.Path) -> bytes:
        nonlocal output_reads
        if path.parent == output.parent and path.suffix == ".flac":
            output_reads += 1
        return original_read_bytes(path)

    monkeypatch.setattr(pathlib.Path, "read_bytes", counted_read_bytes)

    receipt = media.materialize_slice(
        source,
        101,
        303,
        output,
        encoder_profile=media.EncoderProfile.BCFY_FEEDS_SOUNDFILE,
        toolchain=_toolchain(tmp_path),
    )

    assert output_reads == 1
    assert receipt.source_pcm_sha256 == receipt.output_decoded_pcm_sha256


def test_frozen_encoder_contract_records_request_time_decode_rejection() -> (
    None
):
    contract = media.load_encoder_contract(ENCODER_CONTRACT)
    value = json.loads(ENCODER_CONTRACT.read_bytes())
    rejection = value["provider_outcome_contract"][
        "provider_input_decode_rejection"
    ]

    assert value["schema_version"] == 2
    assert contract.receipt_sha256 == media.ENCODER_CONTRACT_SHA256
    assert rejection == {
        "attempt_category": "provider_input_decode_rejected",
        "candidate_expected": False,
        "construction_action": "preserve_geometry_without_padding_or_selection",
        "generated_empty_transcription": False,
        "generation_started": False,
        "source_invalidity": False,
    }
    assert contract.soundfile_python_artifact_relative_path == "soundfile.py"
    assert contract.soundfile_python_artifact_sha256 == (
        "e349c3858d6a0481fe8078d7a0132603261e7bef2b0facfb9596aeb44c51c563"
    )
    assert contract.soundfile_cffi_artifact_relative_path == "_soundfile.py"
    assert contract.soundfile_cffi_artifact_sha256 == (
        "31490aab77c158db180b7a6084256a1d9c6e36c2ff9ea98779494df1161f7474"
    )
    assert contract.libsndfile_binary_relative_path == (
        "_soundfile_data/libsndfile_x86_64.so"
    )
    assert contract.libsndfile_binary_sha256 == (
        "c872fb84af2afc0e96b603e0000f0f407af86c49ad1126f2785e5722c7bb27d7"
    )
    assert contract.embedded_libflac_version == "1.4.3"
    assert (
        contract.embedded_libflac_identity_marker
        == "reference libFLAC 1.4.3 20230623"
    )
    assert contract.ffmpeg_expected_version == "6.1.1"
    assert contract.ffmpeg_default_executable == PINNED_FFMPEG


def test_ffmpeg_command_uses_only_explicit_source_native_pcm_geometry(
    tmp_path: pathlib.Path,
) -> None:
    samples = numpy.arange(32, dtype=numpy.int16).reshape(-1, 2)
    client = _Client()
    uri = "gs://audio/stereo.wav"
    client.add(uri, _wav_bytes(samples, sample_rate=16_000))
    source = media.inventory_source(client, uri, tmp_path / "cache")
    output = tmp_path / "request.flac"

    command, normalized, input_format = media._ffmpeg_command(
        _toolchain(tmp_path),
        source,
        output,
    )

    assert input_format == "s16le"
    assert command == (
        str(tmp_path / "ffmpeg"),
        "-y",
        "-f",
        "s16le",
        "-ar",
        "16000",
        "-ac",
        "2",
        "-i",
        "pipe:0",
        "-map",
        "0:a:0",
        "-c:a",
        "flac",
        "-compression_level",
        "5",
        "-sample_fmt",
        "s16",
        "-f",
        "flac",
        str(output),
    )
    assert normalized[0] == "{ffmpeg}"
    assert normalized[-1] == "{output}"
    assert all(
        forbidden not in normalized
        for forbidden in ("-af", "-filter", "-filter_complex", "-t", "-ss")
    )


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        ("missing", "missing"),
        ("wrong_hash", "SHA-256"),
        ("wrong_version", "version differs"),
    ],
)
def test_encoder_toolchain_fails_closed_on_bad_binary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    failure: str,
    message: str,
) -> None:
    binary = tmp_path / "ffmpeg"
    contract = media.load_encoder_contract(ENCODER_CONTRACT)
    if failure == "missing":
        with pytest.raises(media.MediaError, match=message):
            media.validate_encoder_toolchain(binary, ENCODER_CONTRACT)
        return

    version = "7.0.2" if failure == "wrong_version" else "6.1.1"
    binary.write_text(
        f"#!/bin/sh\nprintf 'ffmpeg version {version} test\\n'\n",
        encoding="utf-8",
    )
    binary.chmod(0o755)
    payload = binary.read_bytes()
    expected_hash = hashlib.sha256(payload).hexdigest()
    if failure == "wrong_hash":
        expected_hash = "0" * 64
    fake_contract = dataclasses.replace(
        contract,
        ffmpeg_binary_size_bytes=len(payload),
        ffmpeg_binary_sha256=expected_hash,
    )
    monkeypatch.setattr(
        media,
        "load_encoder_contract",
        lambda _path: fake_contract,
    )

    with pytest.raises(media.MediaError, match=message):
        media.validate_encoder_toolchain(binary, ENCODER_CONTRACT)


def test_encoder_toolchain_freezes_actual_soundfile_artifacts() -> None:
    toolchain = media.validate_encoder_toolchain(
        PINNED_FFMPEG,
        ENCODER_CONTRACT,
    )

    assert toolchain.soundfile_python_artifact_path.name == "soundfile.py"
    assert toolchain.soundfile_python_artifact_sha256 == (
        "e349c3858d6a0481fe8078d7a0132603261e7bef2b0facfb9596aeb44c51c563"
    )
    assert toolchain.soundfile_cffi_artifact_path.name == "_soundfile.py"
    assert toolchain.soundfile_cffi_artifact_sha256 == (
        "31490aab77c158db180b7a6084256a1d9c6e36c2ff9ea98779494df1161f7474"
    )
    assert toolchain.libsndfile_binary_path.name == "libsndfile_x86_64.so"
    assert toolchain.libsndfile_binary_sha256 == (
        "c872fb84af2afc0e96b603e0000f0f407af86c49ad1126f2785e5722c7bb27d7"
    )
    assert toolchain.embedded_libflac_version == "1.4.3"


@pytest.mark.parametrize(
    ("field", "message"),
    [
        (
            "soundfile_python_artifact_sha256",
            "SoundFile Python artifact SHA-256",
        ),
        (
            "soundfile_cffi_artifact_sha256",
            "SoundFile CFFI artifact SHA-256",
        ),
        (
            "libsndfile_binary_sha256",
            "bundled libsndfile binary SHA-256",
        ),
        (
            "embedded_libflac_identity_marker",
            "embedded libFLAC identity",
        ),
    ],
)
def test_encoder_toolchain_fails_closed_on_soundfile_artifact_drift(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    message: str,
) -> None:
    contract = media.load_encoder_contract(ENCODER_CONTRACT)
    replacement = (
        "missing embedded libFLAC identity"
        if field == "embedded_libflac_identity_marker"
        else "0" * 64
    )
    fake_contract = dataclasses.replace(
        contract,
        **{field: replacement},
    )
    monkeypatch.setattr(
        media,
        "load_encoder_contract",
        lambda _path: fake_contract,
    )

    with pytest.raises(media.MediaError, match=message):
        media.validate_encoder_toolchain(
            PINNED_FFMPEG,
            ENCODER_CONTRACT,
        )


def test_encoder_toolchain_rejects_different_loaded_libsndfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        media,
        "_loaded_libsndfile_path",
        lambda: pathlib.Path(soundfile.__file__).resolve(),
    )

    with pytest.raises(media.MediaError, match="path differs"):
        media.validate_encoder_toolchain(
            PINNED_FFMPEG,
            ENCODER_CONTRACT,
        )


def _synthetic_pcm(frames: int, channels: int) -> numpy.ndarray:
    values = (
        (numpy.arange(frames, dtype=numpy.int64) * 257 + 101) % 65_536 - 32_768
    ).astype(numpy.int16)
    if channels == 1:
        return values.reshape(-1, 1)
    return numpy.column_stack((values, numpy.bitwise_not(values)))


def test_pinned_ffmpeg_preserves_short_and_over_30_second_slices(
    tmp_path: pathlib.Path,
) -> None:
    toolchain = media.validate_encoder_toolchain(
        PINNED_FFMPEG,
        ENCODER_CONTRACT,
    )
    cases = (
        ("short", 8_000, 2, 1_120, "0.14"),
        ("over-30s", 16_000, 1, 480_160, "30.01"),
    )
    for label, sample_rate, channels, frames, duration in cases:
        samples = _synthetic_pcm(frames, channels)
        client = _Client()
        uri = f"gs://audio/{label}.wav"
        client.add(uri, _wav_bytes(samples, sample_rate=sample_rate))
        source = media.inventory_source(
            client,
            uri,
            tmp_path / f"{label}-cache",
        )
        output = tmp_path / f"{label}.flac"

        receipt = media.materialize_slice(
            source,
            0,
            frames,
            output,
            encoder_profile=media.EncoderProfile.PRESEGMENTED_FFMPEG,
            toolchain=toolchain,
        )

        decoded, observed_rate = soundfile.read(
            output,
            dtype="int16",
            always_2d=True,
        )
        assert observed_rate == sample_rate
        numpy.testing.assert_array_equal(decoded, samples)
        assert receipt.frame_count == frames
        assert receipt.duration_seconds == duration
        assert receipt.channels == channels
        assert receipt.encoder_profile == (
            media.EncoderProfile.PRESEGMENTED_FFMPEG.value
        )
        assert receipt.encoder_version == "6.1.1"
        assert receipt.encoder_binary_sha256 == (
            "dcf4c806caad507b11c73754b86a8cbd0ce686ae12f90bf8ba235d145f53edc0"
        )
        assert receipt.source_pcm_sha256 == (receipt.output_decoded_pcm_sha256)


def test_malformed_ffmpeg_output_never_reaches_final_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    samples = _synthetic_pcm(1_120, 1)
    client = _Client()
    uri = "gs://audio/malformed.wav"
    client.add(uri, _wav_bytes(samples))
    source = media.inventory_source(client, uri, tmp_path / "cache")
    output = tmp_path / "bad.flac"

    def fake_run(
        command: list[str],
        **kwargs: object,
    ) -> SimpleNamespace:
        assert kwargs["input"] == samples.astype("<i2").tobytes(order="C")
        pathlib.Path(command[-1]).write_bytes(b"not a FLAC")
        return SimpleNamespace(returncode=0, stderr=b"", stdout=b"")

    monkeypatch.setattr(media.subprocess, "run", fake_run)

    with pytest.raises(media.MediaError, match="undecodable"):
        media.materialize_slice(
            source,
            0,
            len(samples),
            output,
            encoder_profile=media.EncoderProfile.PRESEGMENTED_FFMPEG,
            toolchain=_toolchain(tmp_path),
        )

    assert not output.exists()
    assert list(tmp_path.glob(".bad.flac.*.flac")) == []


def test_pinned_ffmpeg_startup_smoke_benchmark(
    tmp_path: pathlib.Path,
    record_property: Callable[[str, object], None],
) -> None:
    validation_started = time.perf_counter()
    toolchain = media.validate_encoder_toolchain(
        PINNED_FFMPEG,
        ENCODER_CONTRACT,
    )
    validation_ms = (time.perf_counter() - validation_started) * 1_000
    samples = _synthetic_pcm(2_000, 1)
    client = _Client()
    uri = "gs://audio/startup.wav"
    client.add(uri, _wav_bytes(samples))
    source = media.inventory_source(client, uri, tmp_path / "cache")
    elapsed_ms: list[float] = []
    encoded_hashes: list[str] = []
    for index in range(3):
        started = time.perf_counter()
        receipt = media.materialize_slice(
            source,
            0,
            len(samples),
            tmp_path / f"startup-{index}.flac",
            encoder_profile=media.EncoderProfile.PRESEGMENTED_FFMPEG,
            toolchain=toolchain,
        )
        elapsed_ms.append((time.perf_counter() - started) * 1_000)
        encoded_hashes.append(receipt.output_encoded_sha256)
    assert len(set(encoded_hashes)) == 1
    assert all(value > 0 for value in elapsed_ms)
    # Diagnostic only: host load must not become a correctness threshold.
    record_property("ffmpeg_validation_ms", f"{validation_ms:.3f}")
    record_property(
        "ffmpeg_three_encode_ms",
        ",".join(f"{value:.3f}" for value in elapsed_ms),
    )


def test_cache_is_generation_keyed_and_reused(
    tmp_path: pathlib.Path,
) -> None:
    uri = "gs://audio/source.wav"
    client = _Client()
    first_blob = client.add(
        uri,
        _wav_bytes(numpy.arange(10, dtype=numpy.int16)),
    )
    first = media.inventory_source(client, uri, tmp_path / "cache")
    second = media.inventory_source(client, uri, tmp_path / "cache")

    assert first.cache_path == second.cache_path
    assert first_blob.download_generations == [7]

    replacement = client.add(
        uri,
        _wav_bytes(numpy.arange(20, dtype=numpy.int16)),
        generation=8,
    )
    third = media.inventory_source(client, uri, tmp_path / "cache")
    assert third.cache_path != first.cache_path
    assert replacement.download_generations == [8]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"reported_size": 0}, "positive integer"),
        ({"md5_hash": None}, "missing MD5"),
        ({"crc32c": None}, "missing CRC32C"),
    ],
)
def test_inventory_requires_nonzero_integrity_metadata(
    tmp_path: pathlib.Path,
    overrides: dict[str, object],
    message: str,
) -> None:
    client = _Client()
    uri = "gs://audio/source.wav"
    client.add(
        uri,
        _wav_bytes(numpy.arange(10, dtype=numpy.int16)),
        **overrides,
    )
    with pytest.raises(media.MediaError, match=message):
        media.inventory_source(client, uri, tmp_path / "cache")


def test_inventory_rejects_provider_checksum_mismatch(
    tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    uri = "gs://audio/source.wav"
    client.add(
        uri,
        _wav_bytes(numpy.arange(10, dtype=numpy.int16)),
        md5_hash=base64.b64encode(b"x" * 16).decode(),
    )
    with pytest.raises(media.MediaError, match="downloaded MD5 mismatch"):
        media.inventory_source(client, uri, tmp_path / "cache")


def test_inventory_rejects_generation_drift(
    tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    uri = "gs://audio/source.wav"
    client.add(
        uri,
        _wav_bytes(numpy.arange(10, dtype=numpy.int16)),
        drift_after_download=True,
    )
    with pytest.raises(media.MediaError, match="generation drifted"):
        media.inventory_source(client, uri, tmp_path / "cache")


def test_inventory_rejects_preexisting_generation_drift(
    tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    uri = "gs://audio/source.wav"
    client.add(
        uri,
        _wav_bytes(numpy.arange(10, dtype=numpy.int16)),
        generation=8,
    )
    with pytest.raises(media.MediaError, match="generation drift"):
        media.inventory_source(
            client,
            uri,
            tmp_path / "cache",
            expected_generation=7,
        )


def test_inventory_fails_closed_on_corrupted_cache(
    tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    uri = "gs://audio/source.wav"
    client.add(uri, _wav_bytes(numpy.arange(10, dtype=numpy.int16)))
    source = media.inventory_source(client, uri, tmp_path / "cache")
    source.cache_path.write_bytes(b"x" * source.size_bytes)

    with pytest.raises(media.MediaError, match="downloaded MD5 mismatch"):
        media.inventory_source(client, uri, tmp_path / "cache")


def test_inventory_rejects_missing_and_undecodable_sources(
    tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    with pytest.raises(media.MediaError, match="metadata lookup failed"):
        media.inventory_source(
            client,
            "gs://audio/missing.wav",
            tmp_path / "cache",
        )

    uri = "gs://audio/broken.wav"
    client.add(uri, b"not audio")
    with pytest.raises(media.MediaError, match="undecodable"):
        media.inventory_source(client, uri, tmp_path / "cache")


def test_annotation_bounds_abort_on_first_out_of_bounds_row(
    tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    uri = "gs://audio/source.wav"
    client.add(uri, _wav_bytes(numpy.arange(10, dtype=numpy.int16)))
    source = media.inventory_source(client, uri, tmp_path / "cache")

    @dataclasses.dataclass
    class Annotation:
        source_uri: str
        start_frame: int
        end_frame: int

    annotations = [Annotation(uri, 0, 10), Annotation(uri, 9, 11)]
    with pytest.raises(media.MediaError, match="out of bounds"):
        media.validate_annotation_bounds([source], annotations)


def test_inventory_sources_preserves_order_and_rejects_duplicates(
    tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    uris = ("gs://audio/b.wav", "gs://audio/a.wav")
    for uri in uris:
        client.add(uri, _wav_bytes(numpy.arange(10, dtype=numpy.int16)))

    sources = media.inventory_sources(
        client,
        uris,
        tmp_path / "cache",
        max_workers=2,
    )
    assert tuple(source.uri for source in sources) == uris
    with pytest.raises(media.MediaError, match="duplicates"):
        media.inventory_sources(
            client,
            [uris[0], uris[0]],
            tmp_path / "cache",
        )


def test_inventory_receipt_round_trips_canonically(
    tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    uri = "gs://audio/source.wav"
    client.add(uri, _wav_bytes(numpy.arange(10, dtype=numpy.int16)))
    source = media.inventory_source(client, uri, tmp_path / "cache")
    receipt_path = tmp_path / "source-inventory.jsonl"

    receipt_sha256 = media.write_inventory_receipt([source], receipt_path)

    assert media.read_inventory_receipt(receipt_path) == (source,)
    assert (
        receipt_sha256 == hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    )


def test_materialization_rejects_noncontinuous_or_out_of_bounds_request(
    tmp_path: pathlib.Path,
) -> None:
    client = _Client()
    uri = "gs://audio/source.wav"
    client.add(uri, _wav_bytes(numpy.arange(10, dtype=numpy.int16)))
    source = media.inventory_source(client, uri, tmp_path / "cache")
    with pytest.raises(media.MediaError, match="bounds"):
        media.materialize_slice(
            source,
            0,
            11,
            tmp_path / "bad.flac",
            encoder_profile=media.EncoderProfile.BCFY_FEEDS_SOUNDFILE,
            toolchain=_toolchain(tmp_path),
        )
