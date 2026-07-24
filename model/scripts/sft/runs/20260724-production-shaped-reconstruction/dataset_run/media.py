"""Generation-locked source inventory and exact continuous audio slicing.

This module deliberately has only two media operations:

* inventory each externally supplied GCS source generation; and
* copy one continuous, source-native ``[start_frame, end_frame)`` slice to a
  lossless FLAC.

It does not concatenate, resample, remix, normalize, denoise, or infer audio
boundaries.  Construction code must decide the bounds before calling this
module.
"""

# One-time fail-loud media freezer: direct diagnostics and fixed /tmp toolchain
# identity are intentional.
# ruff: noqa: D202, EM101, EM102, PLR0912, PLR0915, S108, TRY003, TRY301

from __future__ import annotations

import base64
import concurrent.futures
import ctypes
import dataclasses
import decimal
import enum
import hashlib
import importlib.metadata
import io
import json
import os
import pathlib
import re
import shutil
import stat
import subprocess
import tempfile
import typing
from collections.abc import Iterable, Mapping, Sequence

import _soundfile as soundfile_cffi
import google_crc32c
import numpy
import soundfile

_HASH_CHUNK_BYTES = 1024 * 1024
_DECODE_CHUNK_FRAMES = 1024 * 1024
_SUPPORTED_PCM_SUBTYPES = frozenset({"PCM_16", "PCM_24"})
_FFMPEG_ENCODE_TIMEOUT_SECONDS = 30
_FFMPEG_VERSION_TIMEOUT_SECONDS = 5
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_PRESEGMENTED_FAMILIES = (
    "bcfy_calls",
    "echo",
    "fire_notifications",
)
_FFMPEG_COMMAND_TEMPLATE = (
    "{ffmpeg}",
    "-y",
    "-f",
    "{input_format}",
    "-ar",
    "{sample_rate}",
    "-ac",
    "{channels}",
    "-i",
    "pipe:0",
    "-map",
    "0:a:0",
    "-c:a",
    "flac",
    "-compression_level",
    "5",
    "-sample_fmt",
    "{sample_format}",
    "-f",
    "flac",
    "{output}",
)
_PROHIBITED_FFMPEG_OPERATIONS = (
    "filter",
    "resample",
    "downmix",
    "normalize",
    "pad",
    "trim",
)
DEFAULT_FFMPEG_BINARY = pathlib.Path(
    "/tmp/next-round-reconstruction/toolchain/ffmpeg"
)
ENCODER_CONTRACT_SHA256 = (
    "f0d22af2c589c36fdff9dd1da1b4f82cc93207582b72d102030ef059e3feab29"
)
PINNED_SOUNDFILE_PYTHON_ARTIFACT_SHA256 = (
    "e349c3858d6a0481fe8078d7a0132603261e7bef2b0facfb9596aeb44c51c563"
)
PINNED_SOUNDFILE_CFFI_ARTIFACT_SHA256 = (
    "31490aab77c158db180b7a6084256a1d9c6e36c2ff9ea98779494df1161f7474"
)
PINNED_LIBSNDFILE_BINARY_SHA256 = (
    "c872fb84af2afc0e96b603e0000f0f407af86c49ad1126f2785e5722c7bb27d7"
)
PINNED_FFMPEG_SHA256 = (
    "dcf4c806caad507b11c73754b86a8cbd0ce686ae12f90bf8ba235d145f53edc0"
)


class MediaError(RuntimeError):
    """Raised when immutable media evidence cannot satisfy the contract."""


class EncoderProfile(enum.StrEnum):
    """The only two frozen serialization paths for reconstructed audio."""

    BCFY_FEEDS_SOUNDFILE = "bcfy_feeds_soundfile_libflac_pcm_preserve_v1"
    PRESEGMENTED_FFMPEG = "presegmented_ffmpeg_6_1_1_flac5_pcm_preserve_v1"


@dataclasses.dataclass(frozen=True, slots=True)
class EncoderContract:
    """Frozen production serializer identities and command semantics."""

    receipt_sha256: str
    soundfile_profile: str
    expected_soundfile_version: str
    soundfile_python_artifact_relative_path: str
    soundfile_python_artifact_sha256: str
    soundfile_python_artifact_size_bytes: int
    soundfile_cffi_artifact_relative_path: str
    soundfile_cffi_artifact_sha256: str
    soundfile_cffi_artifact_size_bytes: int
    expected_libsndfile_version: str
    libsndfile_binary_relative_path: str
    libsndfile_binary_sha256: str
    libsndfile_binary_size_bytes: int
    embedded_libflac_version: str
    embedded_libflac_identity_marker: str
    ffmpeg_profile: str
    ffmpeg_default_executable: pathlib.Path
    ffmpeg_binary_sha256: str
    ffmpeg_binary_size_bytes: int
    ffmpeg_expected_version: str
    ffmpeg_deployed_image_uri: str
    ffmpeg_deployed_revision: str
    ffmpeg_compression_level: int
    ffmpeg_command_template: tuple[str, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class EncoderToolchain:
    """Validated, immutable-for-this-process production encoder handle."""

    contract_sha256: str
    soundfile_version: str
    soundfile_python_artifact_path: pathlib.Path
    soundfile_python_artifact_sha256: str
    soundfile_python_artifact_size_bytes: int
    soundfile_cffi_artifact_path: pathlib.Path
    soundfile_cffi_artifact_sha256: str
    soundfile_cffi_artifact_size_bytes: int
    libsndfile_version: str
    libsndfile_binary_path: pathlib.Path
    libsndfile_binary_sha256: str
    libsndfile_binary_size_bytes: int
    embedded_libflac_version: str
    ffmpeg_executable: pathlib.Path
    ffmpeg_binary_sha256: str
    ffmpeg_binary_size_bytes: int
    ffmpeg_version: str
    ffmpeg_version_line: str
    ffmpeg_deployed_image_uri: str
    ffmpeg_deployed_revision: str


@dataclasses.dataclass(frozen=True, slots=True)
class SourceInventory:
    """Immutable provider, byte, and decoded geometry for one source."""

    uri: str
    generation: int
    size_bytes: int
    md5_hash: str
    crc32c: str
    sha256: str
    format: str
    sample_rate: int
    channels: int
    subtype: str
    frame_count: int
    duration_seconds: str
    cache_path: pathlib.Path

    def __post_init__(self) -> None:
        _parse_gcs_uri(self.uri)
        _positive_integer(
            self.generation,
            field="generation",
            uri=self.uri,
        )
        _positive_integer(
            self.size_bytes,
            field="size",
            uri=self.uri,
        )
        _checksum_metadata(self.md5_hash, field="MD5", uri=self.uri)
        _checksum_metadata(self.crc32c, field="CRC32C", uri=self.uri)
        if (
            not isinstance(self.sha256, str)
            or len(self.sha256) != 64
            or any(
                character not in "0123456789abcdef" for character in self.sha256
            )
        ):
            message = f"{self.uri}: malformed SHA-256"
            raise MediaError(message)
        if not isinstance(self.format, str) or not self.format:
            message = f"{self.uri}: missing decoded format"
            raise MediaError(message)
        _positive_integer(
            self.sample_rate,
            field="sample rate",
            uri=self.uri,
        )
        _positive_integer(
            self.channels,
            field="channel count",
            uri=self.uri,
        )
        _positive_integer(
            self.frame_count,
            field="frame count",
            uri=self.uri,
        )
        _pcm_dtype(self.subtype)
        if self.duration_seconds != _duration_string(
            self.frame_count,
            self.sample_rate,
        ):
            message = f"{self.uri}: inconsistent frame-derived duration"
            raise MediaError(message)
        if not isinstance(self.cache_path, pathlib.Path):
            message = f"{self.uri}: cache_path must be a Path"
            raise MediaError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class SliceReceipt:
    """Exact evidence for one continuous materialized source slice."""

    output_path: pathlib.Path
    output_format: str
    output_codec: str
    output_encoded_size_bytes: int
    output_encoded_sha256: str
    encoder_profile: str
    encoder_version: str
    encoder_binary_sha256: str
    encoder_python_artifact_sha256: str | None
    encoder_cffi_artifact_sha256: str | None
    encoder_embedded_libflac_version: str | None
    encoder_command_sha256: str
    encoder_contract_sha256: str
    encoder_input_pcm_format: str
    encoder_production_image_uri: str | None
    encoder_production_revision: str | None
    source_uri: str
    source_generation: int
    source_encoded_sha256: str
    start_frame: int
    end_frame: int
    frame_count: int
    duration_seconds: str
    sample_rate: int
    channels: int
    subtype: str
    source_pcm_sha256: str
    output_decoded_pcm_sha256: str


class AnnotationFrames(typing.Protocol):
    """Minimum exact-frame annotation shape accepted by validation."""

    source_uri: str
    start_frame: int
    end_frame: int


@dataclasses.dataclass(frozen=True, slots=True)
class _ProviderMetadata:
    uri: str
    generation: int
    size_bytes: int
    md5_hash: str
    crc32c: str


@dataclasses.dataclass(frozen=True, slots=True)
class _CacheReceipt:
    uri: str
    generation: int
    size_bytes: int
    md5_hash: str
    crc32c: str
    sha256: str


@dataclasses.dataclass(frozen=True, slots=True)
class _FileDigests:
    size_bytes: int
    md5_hash: str
    crc32c: str
    sha256: str


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not isinstance(uri, str) or not uri.startswith("gs://"):
        raise MediaError(f"invalid GCS source URI: {uri!r}")
    bucket_and_object = uri.removeprefix("gs://").split("/", 1)
    if (
        len(bucket_and_object) != 2
        or not bucket_and_object[0]
        or not bucket_and_object[1]
    ):
        raise MediaError(f"GCS source URI must name one object: {uri!r}")
    return bucket_and_object[0], bucket_and_object[1]


def _positive_integer(value: object, *, field: str, uri: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise MediaError(
            f"{uri}: {field} metadata must be a positive integer; "
            f"observed {value!r}"
        )
    return value


def _checksum_metadata(value: object, *, field: str, uri: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MediaError(f"{uri}: missing {field} metadata")
    normalized = value.strip()
    try:
        decoded = base64.b64decode(normalized, validate=True)
    except (ValueError, TypeError) as error:
        raise MediaError(f"{uri}: malformed {field} metadata") from error
    expected_bytes = 16 if field == "MD5" else 4
    if len(decoded) != expected_bytes:
        raise MediaError(f"{uri}: malformed {field} metadata")
    return normalized


def _metadata(blob: object, uri: str) -> _ProviderMetadata:
    return _ProviderMetadata(
        uri=uri,
        generation=_positive_integer(
            getattr(blob, "generation", None),
            field="generation",
            uri=uri,
        ),
        size_bytes=_positive_integer(
            getattr(blob, "size", None),
            field="size",
            uri=uri,
        ),
        md5_hash=_checksum_metadata(
            getattr(blob, "md5_hash", None),
            field="MD5",
            uri=uri,
        ),
        crc32c=_checksum_metadata(
            getattr(blob, "crc32c", None),
            field="CRC32C",
            uri=uri,
        ),
    )


def _reload_metadata(
    blob: object,
    uri: str,
    *,
    generation: int | None = None,
) -> _ProviderMetadata:
    try:
        if generation is None:
            blob.reload()
        else:
            blob.reload(if_generation_match=generation)
    except Exception as error:
        qualifier = (
            "source metadata lookup failed"
            if generation is None
            else "source generation drifted"
        )
        raise MediaError(f"{uri}: {qualifier}") from error
    return _metadata(blob, uri)


def _duration_string(frames: int, sample_rate: int) -> str:
    with decimal.localcontext() as context:
        context.prec = max(50, len(str(frames)) + len(str(sample_rate)) + 10)
        duration = decimal.Decimal(frames) / decimal.Decimal(sample_rate)
        rendered = format(duration, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _file_digests(path: pathlib.Path) -> _FileDigests:
    sha256 = hashlib.sha256()
    md5 = hashlib.md5(usedforsecurity=False)
    crc32c = google_crc32c.Checksum()
    size = 0
    try:
        with path.open("rb") as source:
            while chunk := source.read(_HASH_CHUNK_BYTES):
                size += len(chunk)
                sha256.update(chunk)
                md5.update(chunk)
                crc32c.update(chunk)
    except OSError as error:
        raise MediaError(f"cannot read cached source bytes: {path}") from error
    return _FileDigests(
        size_bytes=size,
        md5_hash=base64.b64encode(md5.digest()).decode(),
        crc32c=base64.b64encode(crc32c.digest()).decode(),
        sha256=sha256.hexdigest(),
    )


def _sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(_HASH_CHUNK_BYTES):
                digest.update(chunk)
    except OSError as error:
        raise MediaError(f"cannot hash encoder artifact: {path}") from error
    return digest.hexdigest()


def _validate_encoder_artifact(
    path_value: object,
    *,
    label: str,
    expected_path: pathlib.Path,
    expected_size_bytes: int,
    expected_sha256: str,
) -> tuple[pathlib.Path, int, str]:
    """Resolve and hash one exact regular-file encoder artifact."""

    if not isinstance(path_value, (str, os.PathLike)) or not str(path_value):
        raise MediaError(f"{label} path is unavailable")
    try:
        path = pathlib.Path(path_value).resolve(strict=True)
        metadata = path.stat()
    except OSError as error:
        raise MediaError(f"{label} is missing") from error
    if not stat.S_ISREG(metadata.st_mode):
        raise MediaError(f"{label} is not a regular file")
    if path != expected_path:
        raise MediaError(
            f"{label} path differs from frozen distribution artifact"
        )
    if metadata.st_size != expected_size_bytes:
        raise MediaError(f"{label} size differs from frozen contract")
    observed_sha256 = _sha256_file(path)
    if observed_sha256 != expected_sha256:
        raise MediaError(f"{label} SHA-256 differs from frozen contract")
    return path, metadata.st_size, observed_sha256


class _DlInfoC(ctypes.Structure):
    _fields_ = [
        ("dli_fname", ctypes.c_char_p),
        ("dli_fbase", ctypes.c_void_p),
        ("dli_sname", ctypes.c_char_p),
        ("dli_saddr", ctypes.c_void_p),
    ]


def _loaded_libsndfile_path() -> pathlib.Path:
    """Resolve ``sf_version_string`` to its defining loaded shared object."""

    try:
        function = soundfile._ffi.addressof(  # noqa: SLF001
            soundfile._snd,  # noqa: SLF001
            "sf_version_string",
        )
        address = int(
            soundfile._ffi.cast("uintptr_t", function)  # noqa: SLF001
        )
        process = ctypes.CDLL(None)
        dladdr = process.dladdr
        dladdr.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_DlInfoC),
        ]
        dladdr.restype = ctypes.c_int
        info = _DlInfoC()
        result = dladdr(ctypes.c_void_p(address), ctypes.byref(info))
    except (AttributeError, TypeError, ValueError, OSError) as error:
        raise MediaError(
            "cannot resolve the actual loaded libsndfile object"
        ) from error
    if result != 1 or not info.dli_fname:
        raise MediaError("cannot resolve the actual loaded libsndfile object")
    try:
        return pathlib.Path(os.fsdecode(info.dli_fname)).resolve(strict=True)
    except OSError as error:
        raise MediaError(
            "actual loaded libsndfile object is unavailable"
        ) from error


def load_encoder_contract(path: pathlib.Path) -> EncoderContract:
    """Read the one frozen, canonical production encoder receipt."""

    contract_path = pathlib.Path(path)
    try:
        payload = contract_path.read_bytes()
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MediaError(
            f"cannot read encoder toolchain receipt: {contract_path}"
        ) from error
    if not isinstance(value, dict):
        raise MediaError("encoder toolchain receipt must be a JSON object")
    canonical = (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    if payload != canonical:
        raise MediaError("encoder toolchain receipt must be canonical JSON")
    receipt_sha256 = hashlib.sha256(payload).hexdigest()
    if receipt_sha256 != ENCODER_CONTRACT_SHA256:
        raise MediaError("encoder toolchain receipt SHA-256 drifted")
    if (
        set(value)
        != {
            "bcfy_feeds",
            "presegmented",
            "provider_outcome_contract",
            "schema_version",
        }
        or value["schema_version"] != 2
    ):
        raise MediaError("encoder toolchain receipt schema drifted")
    feeds = value["bcfy_feeds"]
    presegmented = value["presegmented"]
    outcome = value["provider_outcome_contract"]
    if (
        not isinstance(feeds, dict)
        or not isinstance(presegmented, dict)
        or not isinstance(outcome, dict)
    ):
        raise MediaError("encoder toolchain receipt sections are malformed")
    expected_feeds = {
        "embedded_libflac_identity_marker": (
            "reference libFLAC 1.4.3 20230623"
        ),
        "embedded_libflac_version": "1.4.3",
        "encoder_profile": EncoderProfile.BCFY_FEEDS_SOUNDFILE.value,
        "expected_libsndfile_version": "1.2.2",
        "expected_soundfile_version": "0.13.1",
        "libsndfile_binary_relative_path": (
            "_soundfile_data/libsndfile_x86_64.so"
        ),
        "libsndfile_binary_sha256": PINNED_LIBSNDFILE_BINARY_SHA256,
        "libsndfile_binary_size_bytes": 3_639_184,
        "soundfile_cffi_artifact_relative_path": "_soundfile.py",
        "soundfile_cffi_artifact_sha256": (
            PINNED_SOUNDFILE_CFFI_ARTIFACT_SHA256
        ),
        "soundfile_cffi_artifact_size_bytes": 5_783,
        "soundfile_python_artifact_relative_path": "soundfile.py",
        "soundfile_python_artifact_sha256": (
            PINNED_SOUNDFILE_PYTHON_ARTIFACT_SHA256
        ),
        "soundfile_python_artifact_size_bytes": 64_864,
    }
    expected_outcome = {
        "provider_input_decode_rejection": {
            "attempt_category": "provider_input_decode_rejected",
            "candidate_expected": False,
            "construction_action": (
                "preserve_geometry_without_padding_or_selection"
            ),
            "generated_empty_transcription": False,
            "generation_started": False,
            "source_invalidity": False,
        }
    }
    required_presegmented_keys = {
        "binary_sha256",
        "binary_size_bytes",
        "command_template",
        "default_executable",
        "deployed_image_uri",
        "deployed_revision",
        "encoder_profile",
        "expected_version",
        "families",
        "flac_compression_level",
        "input_mode",
        "prohibited_operations",
    }
    if feeds != expected_feeds or outcome != expected_outcome:
        raise MediaError("encoder toolchain receipt policy drifted")
    if set(presegmented) != required_presegmented_keys:
        raise MediaError("presegmented encoder receipt schema drifted")
    if (
        presegmented["encoder_profile"]
        != EncoderProfile.PRESEGMENTED_FFMPEG.value
        or presegmented["default_executable"] != str(DEFAULT_FFMPEG_BINARY)
        or presegmented["families"] != list(_PRESEGMENTED_FAMILIES)
        or presegmented["flac_compression_level"] != 5
        or presegmented["input_mode"]
        != "raw_pcm_stdin_with_explicit_source_native_format_rate_and_channels"
        or presegmented["prohibited_operations"]
        != list(_PROHIBITED_FFMPEG_OPERATIONS)
        or presegmented["command_template"] != list(_FFMPEG_COMMAND_TEMPLATE)
    ):
        raise MediaError("presegmented encoder receipt policy drifted")
    binary_sha256 = presegmented["binary_sha256"]
    binary_size = presegmented["binary_size_bytes"]
    version = presegmented["expected_version"]
    image_uri = presegmented["deployed_image_uri"]
    revision = presegmented["deployed_revision"]
    if (
        not isinstance(binary_sha256, str)
        or _SHA256.fullmatch(binary_sha256) is None
        or binary_sha256 != PINNED_FFMPEG_SHA256
        or isinstance(binary_size, bool)
        or not isinstance(binary_size, int)
        or binary_size <= 0
        or not isinstance(version, str)
        or version != "6.1.1"
        or not isinstance(image_uri, str)
        or "@sha256:" not in image_uri
        or not isinstance(revision, str)
        or not revision
    ):
        raise MediaError("presegmented encoder identity is malformed")
    return EncoderContract(
        receipt_sha256=receipt_sha256,
        soundfile_profile=str(feeds["encoder_profile"]),
        expected_soundfile_version=str(feeds["expected_soundfile_version"]),
        soundfile_python_artifact_relative_path=str(
            feeds["soundfile_python_artifact_relative_path"]
        ),
        soundfile_python_artifact_sha256=str(
            feeds["soundfile_python_artifact_sha256"]
        ),
        soundfile_python_artifact_size_bytes=int(
            feeds["soundfile_python_artifact_size_bytes"]
        ),
        soundfile_cffi_artifact_relative_path=str(
            feeds["soundfile_cffi_artifact_relative_path"]
        ),
        soundfile_cffi_artifact_sha256=str(
            feeds["soundfile_cffi_artifact_sha256"]
        ),
        soundfile_cffi_artifact_size_bytes=int(
            feeds["soundfile_cffi_artifact_size_bytes"]
        ),
        expected_libsndfile_version=str(feeds["expected_libsndfile_version"]),
        libsndfile_binary_relative_path=str(
            feeds["libsndfile_binary_relative_path"]
        ),
        libsndfile_binary_sha256=str(feeds["libsndfile_binary_sha256"]),
        libsndfile_binary_size_bytes=int(feeds["libsndfile_binary_size_bytes"]),
        embedded_libflac_version=str(feeds["embedded_libflac_version"]),
        embedded_libflac_identity_marker=str(
            feeds["embedded_libflac_identity_marker"]
        ),
        ffmpeg_profile=str(presegmented["encoder_profile"]),
        ffmpeg_default_executable=pathlib.Path(
            str(presegmented["default_executable"])
        ),
        ffmpeg_binary_sha256=binary_sha256,
        ffmpeg_binary_size_bytes=binary_size,
        ffmpeg_expected_version=version,
        ffmpeg_deployed_image_uri=image_uri,
        ffmpeg_deployed_revision=revision,
        ffmpeg_compression_level=int(presegmented["flac_compression_level"]),
        ffmpeg_command_template=tuple(presegmented["command_template"]),
    )


def validate_encoder_toolchain(
    ffmpeg_binary: pathlib.Path,
    encoder_contract_path: pathlib.Path,
) -> EncoderToolchain:
    """Fail closed unless both local encoder implementations are frozen."""

    contract = load_encoder_contract(encoder_contract_path)
    soundfile_version = str(soundfile.__version__)
    libsndfile_version = str(getattr(soundfile, "__libsndfile_version__", ""))
    if (
        soundfile_version != contract.expected_soundfile_version
        or libsndfile_version != contract.expected_libsndfile_version
    ):
        raise MediaError(
            "soundfile/libFLAC encoder version differs from frozen contract"
        )

    try:
        distribution = importlib.metadata.distribution("soundfile")
        distribution_root = pathlib.Path(distribution.locate_file("")).resolve(
            strict=True
        )
        expected_soundfile_artifact = pathlib.Path(
            distribution.locate_file(
                contract.soundfile_python_artifact_relative_path
            )
        ).resolve(strict=True)
        expected_cffi_artifact = pathlib.Path(
            distribution.locate_file(
                contract.soundfile_cffi_artifact_relative_path
            )
        ).resolve(strict=True)
        expected_bundled_library = pathlib.Path(
            distribution.locate_file(contract.libsndfile_binary_relative_path)
        ).resolve(strict=True)
    except (importlib.metadata.PackageNotFoundError, OSError) as error:
        raise MediaError(
            "cannot resolve frozen SoundFile distribution artifacts"
        ) from error
    for artifact in (
        expected_soundfile_artifact,
        expected_cffi_artifact,
        expected_bundled_library,
    ):
        if not artifact.is_relative_to(distribution_root):
            raise MediaError(
                "SoundFile encoder artifact escapes its frozen distribution"
            )

    soundfile_artifact, soundfile_artifact_size, soundfile_artifact_sha256 = (
        _validate_encoder_artifact(
            getattr(soundfile, "__file__", None),
            label="SoundFile Python artifact",
            expected_path=expected_soundfile_artifact,
            expected_size_bytes=(contract.soundfile_python_artifact_size_bytes),
            expected_sha256=contract.soundfile_python_artifact_sha256,
        )
    )
    cffi_artifact, cffi_artifact_size, cffi_artifact_sha256 = (
        _validate_encoder_artifact(
            getattr(soundfile_cffi, "__file__", None),
            label="SoundFile CFFI artifact",
            expected_path=expected_cffi_artifact,
            expected_size_bytes=contract.soundfile_cffi_artifact_size_bytes,
            expected_sha256=contract.soundfile_cffi_artifact_sha256,
        )
    )
    packaged_library_name = getattr(soundfile, "_packaged_libname", None)
    declared_library_value = getattr(soundfile, "_full_path", None)
    if packaged_library_name != expected_bundled_library.name:
        raise MediaError(
            "SoundFile did not load the frozen bundled libsndfile artifact"
        )
    try:
        declared_library = pathlib.Path(
            typing.cast("str | os.PathLike[str]", declared_library_value)
        ).resolve(strict=True)
    except (TypeError, OSError) as error:
        raise MediaError(
            "SoundFile bundled libsndfile declaration is unavailable"
        ) from error
    if declared_library != expected_bundled_library:
        raise MediaError(
            "SoundFile declared libsndfile outside its frozen distribution"
        )
    loaded_library_value = _loaded_libsndfile_path()
    libsndfile_binary, libsndfile_binary_size, libsndfile_binary_sha256 = (
        _validate_encoder_artifact(
            loaded_library_value,
            label="bundled libsndfile binary",
            expected_path=expected_bundled_library,
            expected_size_bytes=contract.libsndfile_binary_size_bytes,
            expected_sha256=contract.libsndfile_binary_sha256,
        )
    )
    try:
        libsndfile_payload = libsndfile_binary.read_bytes()
    except OSError as error:
        raise MediaError(
            "cannot inspect bundled libsndfile codec identity"
        ) from error
    if (
        contract.embedded_libflac_identity_marker.encode()
        not in libsndfile_payload
    ):
        raise MediaError(
            "bundled libsndfile embedded libFLAC identity differs from "
            "frozen contract"
        )

    executable = pathlib.Path(ffmpeg_binary).absolute()
    try:
        metadata = executable.stat()
    except OSError as error:
        raise MediaError(
            f"pinned FFmpeg binary is missing: {executable}"
        ) from error
    if not stat.S_ISREG(metadata.st_mode) or not os.access(executable, os.X_OK):
        raise MediaError(
            f"pinned FFmpeg path is not an executable regular file: {executable}"
        )
    if metadata.st_size != contract.ffmpeg_binary_size_bytes:
        raise MediaError(
            "pinned FFmpeg binary size differs from frozen contract"
        )
    observed_sha256 = _sha256_file(executable)
    if observed_sha256 != contract.ffmpeg_binary_sha256:
        raise MediaError(
            "pinned FFmpeg binary SHA-256 differs from frozen contract"
        )
    try:
        version_probe = subprocess.run(
            [str(executable), "-version"],
            capture_output=True,
            check=False,
            text=True,
            timeout=_FFMPEG_VERSION_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise MediaError("pinned FFmpeg version probe failed") from error
    if version_probe.returncode != 0:
        raise MediaError("pinned FFmpeg version probe returned an error")
    first_line = (
        version_probe.stdout.splitlines()[0]
        if version_probe.stdout.splitlines()
        else ""
    )
    expected_prefix = f"ffmpeg version {contract.ffmpeg_expected_version}"
    if not (
        first_line == expected_prefix
        or first_line.startswith(f"{expected_prefix} ")
    ):
        raise MediaError("pinned FFmpeg version differs from frozen contract")
    return EncoderToolchain(
        contract_sha256=contract.receipt_sha256,
        soundfile_version=soundfile_version,
        soundfile_python_artifact_path=soundfile_artifact,
        soundfile_python_artifact_sha256=soundfile_artifact_sha256,
        soundfile_python_artifact_size_bytes=soundfile_artifact_size,
        soundfile_cffi_artifact_path=cffi_artifact,
        soundfile_cffi_artifact_sha256=cffi_artifact_sha256,
        soundfile_cffi_artifact_size_bytes=cffi_artifact_size,
        libsndfile_version=libsndfile_version,
        libsndfile_binary_path=libsndfile_binary,
        libsndfile_binary_sha256=libsndfile_binary_sha256,
        libsndfile_binary_size_bytes=libsndfile_binary_size,
        embedded_libflac_version=contract.embedded_libflac_version,
        ffmpeg_executable=executable,
        ffmpeg_binary_sha256=observed_sha256,
        ffmpeg_binary_size_bytes=metadata.st_size,
        ffmpeg_version=contract.ffmpeg_expected_version,
        ffmpeg_version_line=first_line,
        ffmpeg_deployed_image_uri=contract.ffmpeg_deployed_image_uri,
        ffmpeg_deployed_revision=contract.ffmpeg_deployed_revision,
    )


def _validate_download(
    path: pathlib.Path,
    metadata: _ProviderMetadata,
) -> _FileDigests:
    observed = _file_digests(path)
    if observed.size_bytes != metadata.size_bytes:
        raise MediaError(
            f"{metadata.uri}: download size mismatch; expected "
            f"{metadata.size_bytes}, observed {observed.size_bytes}"
        )
    if observed.md5_hash != metadata.md5_hash:
        raise MediaError(f"{metadata.uri}: downloaded MD5 mismatch")
    if observed.crc32c != metadata.crc32c:
        raise MediaError(f"{metadata.uri}: downloaded CRC32C mismatch")
    return observed


def _receipt_bytes(receipt: _CacheReceipt) -> bytes:
    return (
        json.dumps(
            dataclasses.asdict(receipt),
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()


def _read_cache_receipt(path: pathlib.Path) -> _CacheReceipt:
    try:
        value = json.loads(path.read_text())
        receipt = _CacheReceipt(**value)
    except (OSError, TypeError, json.JSONDecodeError) as error:
        raise MediaError(f"invalid source-cache receipt: {path}") from error
    if not isinstance(value, dict) or dataclasses.asdict(receipt) != value:
        raise MediaError(f"invalid source-cache receipt: {path}")
    return receipt


def _fsync_file(path: pathlib.Path) -> None:
    with path.open("rb") as opened:
        os.fsync(opened.fileno())


def _fsync_directory(path: pathlib.Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    except (AttributeError, OSError):
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _download_to_path(
    blob: object,
    metadata: _ProviderMetadata,
    path: pathlib.Path,
) -> _FileDigests:
    try:
        with path.open("wb") as destination:
            blob.download_to_file(
                destination,
                if_generation_match=metadata.generation,
                checksum="auto",
            )
            destination.flush()
            os.fsync(destination.fileno())
    except Exception as error:
        raise MediaError(
            f"{metadata.uri}: generation-locked source download failed"
        ) from error
    observed = _validate_download(path, metadata)
    after_download = _reload_metadata(
        blob,
        metadata.uri,
        generation=metadata.generation,
    )
    if after_download != metadata:
        raise MediaError(
            f"{metadata.uri}: provider metadata drifted during download"
        )
    return observed


def _cache_key(uri: str, generation: int) -> str:
    return hashlib.sha256(f"{uri}\0{generation}".encode()).hexdigest()


def _validate_cache(
    cache_directory: pathlib.Path,
    metadata: _ProviderMetadata,
) -> tuple[pathlib.Path, _FileDigests]:
    source_path = cache_directory / "source.audio"
    receipt = _read_cache_receipt(cache_directory / "receipt.json")
    expected = _CacheReceipt(
        uri=metadata.uri,
        generation=metadata.generation,
        size_bytes=metadata.size_bytes,
        md5_hash=metadata.md5_hash,
        crc32c=metadata.crc32c,
        sha256=receipt.sha256,
    )
    if receipt != expected:
        raise MediaError(
            f"{metadata.uri}: cache receipt differs from provider metadata"
        )
    observed = _validate_download(source_path, metadata)
    if observed.sha256 != receipt.sha256:
        raise MediaError(f"{metadata.uri}: cached SHA-256 mismatch")
    return source_path, observed


def _populate_cache(
    blob: object,
    metadata: _ProviderMetadata,
    cache_root: pathlib.Path,
) -> tuple[pathlib.Path, _FileDigests]:
    cache_root.mkdir(parents=True, exist_ok=True)
    cache_directory = cache_root / _cache_key(
        metadata.uri,
        metadata.generation,
    )
    if cache_directory.exists():
        return _validate_cache(cache_directory, metadata)

    temporary_directory = pathlib.Path(
        tempfile.mkdtemp(prefix=".source-", dir=cache_root)
    )
    try:
        source_path = temporary_directory / "source.audio"
        observed = _download_to_path(blob, metadata, source_path)
        receipt = _CacheReceipt(
            uri=metadata.uri,
            generation=metadata.generation,
            size_bytes=metadata.size_bytes,
            md5_hash=metadata.md5_hash,
            crc32c=metadata.crc32c,
            sha256=observed.sha256,
        )
        receipt_path = temporary_directory / "receipt.json"
        receipt_path.write_bytes(_receipt_bytes(receipt))
        _fsync_file(receipt_path)
        _fsync_directory(temporary_directory)
        try:
            temporary_directory.rename(cache_directory)
            temporary_directory = pathlib.Path()
            _fsync_directory(cache_root)
        except FileExistsError:
            # Another worker completed this immutable entry first.
            return _validate_cache(cache_directory, metadata)
    finally:
        if temporary_directory != pathlib.Path():
            shutil.rmtree(temporary_directory, ignore_errors=True)
    return cache_directory / "source.audio", observed


def _pcm_dtype(subtype: str) -> str:
    if subtype == "PCM_16":
        return "int16"
    if subtype == "PCM_24":
        return "int32"
    raise MediaError(
        f"source subtype {subtype!r} cannot be copied losslessly to FLAC"
    )


def _canonical_pcm(samples: numpy.ndarray, subtype: str) -> bytes:
    dtype = "<i2" if subtype == "PCM_16" else "<i4"
    return (
        numpy.ascontiguousarray(samples)
        .astype(
            dtype,
            copy=False,
        )
        .tobytes(order="C")
    )


def _decode_inventory(
    metadata: _ProviderMetadata,
    source_path: pathlib.Path,
    encoded_sha256: str,
) -> SourceInventory:
    try:
        source_context = soundfile.SoundFile(source_path, mode="r")
    except (OSError, RuntimeError, soundfile.LibsndfileError) as error:
        raise MediaError(
            f"{metadata.uri}: source audio is undecodable"
        ) from error

    try:
        with source_context as source:
            if source.samplerate <= 0 or source.channels <= 0:
                raise MediaError(
                    f"{metadata.uri}: invalid decoded source geometry"
                )
            if source.frames <= 0:
                raise MediaError(f"{metadata.uri}: source has no audio frames")
            if not source.format or not source.subtype:
                raise MediaError(
                    f"{metadata.uri}: decoded format metadata is absent"
                )
            dtype = _pcm_dtype(source.subtype)
            expected_frames = source.frames
            decoded_frames = 0
            while decoded_frames < expected_frames:
                requested = min(
                    _DECODE_CHUNK_FRAMES,
                    expected_frames - decoded_frames,
                )
                chunk = source.read(
                    requested,
                    dtype=dtype,
                    always_2d=True,
                )
                if chunk.shape != (requested, source.channels):
                    raise MediaError(
                        f"{metadata.uri}: truncated source; expected "
                        f"{requested} frames, decoded {chunk.shape[0]}"
                    )
                decoded_frames += requested
            if source.read(1, dtype=dtype, always_2d=True).shape[0] != 0:
                raise MediaError(
                    f"{metadata.uri}: decoded more frames than declared"
                )
            return SourceInventory(
                uri=metadata.uri,
                generation=metadata.generation,
                size_bytes=metadata.size_bytes,
                md5_hash=metadata.md5_hash,
                crc32c=metadata.crc32c,
                sha256=encoded_sha256,
                format=source.format,
                sample_rate=source.samplerate,
                channels=source.channels,
                subtype=source.subtype,
                frame_count=source.frames,
                duration_seconds=_duration_string(
                    source.frames,
                    source.samplerate,
                ),
                cache_path=source_path,
            )
    except MediaError:
        raise
    except (OSError, RuntimeError, soundfile.LibsndfileError) as error:
        raise MediaError(
            f"{metadata.uri}: source audio is truncated or undecodable"
        ) from error


def _probe_frozen_inventory(
    metadata: _ProviderMetadata,
    source_path: pathlib.Path,
    encoded_sha256: str,
) -> SourceInventory:
    """Read only frozen source header geometry.

    The caller has accepted the frozen media as valid, so this deliberately
    avoids a separate full-file decode audit. Requested slices are still
    decoded and checked when they are materialized.
    """

    try:
        with soundfile.SoundFile(source_path, mode="r") as source:
            if (
                source.samplerate <= 0
                or source.channels <= 0
                or source.frames <= 0
            ):
                raise MediaError(
                    f"{metadata.uri}: invalid decoded source geometry"
                )
            if not source.format or not source.subtype:
                raise MediaError(
                    f"{metadata.uri}: decoded format metadata is absent"
                )
            _pcm_dtype(source.subtype)
            return SourceInventory(
                uri=metadata.uri,
                generation=metadata.generation,
                size_bytes=metadata.size_bytes,
                md5_hash=metadata.md5_hash,
                crc32c=metadata.crc32c,
                sha256=encoded_sha256,
                format=source.format,
                sample_rate=source.samplerate,
                channels=source.channels,
                subtype=source.subtype,
                frame_count=source.frames,
                duration_seconds=_duration_string(
                    source.frames,
                    source.samplerate,
                ),
                cache_path=source_path,
            )
    except MediaError:
        raise
    except (OSError, RuntimeError, soundfile.LibsndfileError) as error:
        raise MediaError(
            f"{metadata.uri}: source audio cannot be opened"
        ) from error


def _trusted_frozen_cache(
    blob: object,
    metadata: _ProviderMetadata,
    cache_root: pathlib.Path,
) -> tuple[pathlib.Path, str]:
    """Reuse our one-writer cache without rehashing an accepted frozen file."""

    cache_directory = cache_root / _cache_key(
        metadata.uri,
        metadata.generation,
    )
    if not cache_directory.exists():
        path, digests = _populate_cache(blob, metadata, cache_root)
        return path, digests.sha256

    receipt = _read_cache_receipt(cache_directory / "receipt.json")
    expected = _CacheReceipt(
        uri=metadata.uri,
        generation=metadata.generation,
        size_bytes=metadata.size_bytes,
        md5_hash=metadata.md5_hash,
        crc32c=metadata.crc32c,
        sha256=receipt.sha256,
    )
    if receipt != expected:
        raise MediaError(
            f"{metadata.uri}: frozen cache receipt does not match its source"
        )
    source_path = cache_directory / "source.audio"
    try:
        size = source_path.stat().st_size
    except OSError as error:
        raise MediaError(
            f"{metadata.uri}: frozen cached source is missing"
        ) from error
    if size != metadata.size_bytes:
        raise MediaError(
            f"{metadata.uri}: frozen cached source size is inconsistent"
        )
    return source_path, receipt.sha256


def probe_frozen_source(
    storage_client: object,
    uri: str,
    cache_root: pathlib.Path,
) -> SourceInventory:
    """Acquire one accepted frozen source and read only its header geometry."""

    bucket_name, object_name = _parse_gcs_uri(uri)
    try:
        blob = storage_client.bucket(bucket_name).blob(object_name)
    except Exception as error:
        raise MediaError(
            f"{uri}: cannot construct GCS object handle"
        ) from error
    metadata = _reload_metadata(blob, uri)
    source_path, sha256 = _trusted_frozen_cache(
        blob,
        metadata,
        pathlib.Path(cache_root),
    )
    return _probe_frozen_inventory(metadata, source_path, sha256)


def probe_frozen_sources(
    storage_client: object,
    source_uris: Iterable[str],
    cache_root: pathlib.Path,
    *,
    max_workers: int = 1,
) -> tuple[SourceInventory, ...]:
    """Acquire frozen sources with header-only probing and stable ordering."""

    uris = tuple(source_uris)
    if not uris or len(uris) != len(set(uris)):
        raise MediaError(
            "frozen source URI enumeration must be nonempty and unique"
        )
    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers <= 0
    ):
        raise MediaError("max_workers must be a positive integer")

    def load(uri: str) -> SourceInventory:
        return probe_frozen_source(storage_client, uri, cache_root)

    if max_workers == 1:
        return tuple(load(uri) for uri in uris)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:
        return tuple(executor.map(load, uris))


def inventory_source(
    storage_client: object,
    uri: str,
    cache_root: pathlib.Path,
    *,
    expected_generation: int | None = None,
) -> SourceInventory:
    """Inventory one exact GCS generation and its complete decoded geometry."""

    bucket_name, object_name = _parse_gcs_uri(uri)
    try:
        blob = storage_client.bucket(bucket_name).blob(object_name)
    except Exception as error:
        raise MediaError(
            f"{uri}: cannot construct GCS object handle"
        ) from error
    metadata = _reload_metadata(blob, uri)
    if expected_generation is not None:
        normalized_expected = _positive_integer(
            expected_generation,
            field="expected generation",
            uri=uri,
        )
        if metadata.generation != normalized_expected:
            raise MediaError(
                f"{uri}: generation drift; expected {normalized_expected}, "
                f"observed {metadata.generation}"
            )
    source_path, digests = _populate_cache(
        blob,
        metadata,
        pathlib.Path(cache_root),
    )
    return _decode_inventory(metadata, source_path, digests.sha256)


def inventory_sources(
    storage_client: object,
    source_uris: Iterable[str],
    cache_root: pathlib.Path,
    *,
    expected_generations: Mapping[str, int] | None = None,
    max_workers: int = 1,
) -> tuple[SourceInventory, ...]:
    """Inventory externally supplied URIs, preserving their input order.

    Duplicate URIs are rejected instead of hiding upstream enumeration errors.
    With multiple workers, the first observed failure is raised and outstanding
    work is cancelled.
    """

    uris = tuple(source_uris)
    for uri in uris:
        _parse_gcs_uri(uri)
    if len(uris) != len(set(uris)):
        raise MediaError("source URI enumeration contains duplicates")
    if not uris:
        raise MediaError("source URI enumeration is empty")
    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers <= 0
    ):
        raise MediaError("max_workers must be a positive integer")
    expected = expected_generations or {}
    unknown_expectations = set(expected).difference(uris)
    if unknown_expectations:
        raise MediaError(
            "expected generation supplied for an unenumerated source: "
            f"{sorted(unknown_expectations)[0]}"
        )

    def load(uri: str) -> SourceInventory:
        return inventory_source(
            storage_client,
            uri,
            cache_root,
            expected_generation=expected.get(uri),
        )

    if max_workers == 1:
        return tuple(load(uri) for uri in uris)

    by_uri: dict[str, SourceInventory] = {}
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    futures = {executor.submit(load, uri): uri for uri in uris}
    try:
        for future in concurrent.futures.as_completed(futures):
            item = future.result()
            by_uri[item.uri] = item
    except BaseException:
        for future in futures:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
        raise
    executor.shutdown(wait=True)
    return tuple(by_uri[uri] for uri in uris)


def source_inventory_by_uri(
    sources: Iterable[SourceInventory],
) -> dict[str, SourceInventory]:
    """Index source inventory and reject duplicate immutable identities."""

    result: dict[str, SourceInventory] = {}
    for source in sources:
        if source.uri in result:
            raise MediaError(f"duplicate source inventory: {source.uri}")
        result[source.uri] = source
    return result


def _source_receipt_value(source: SourceInventory) -> dict[str, object]:
    value = dataclasses.asdict(source)
    value["cache_path"] = str(source.cache_path)
    return value


def write_inventory_receipt(
    sources: Iterable[SourceInventory],
    output_path: pathlib.Path,
) -> str:
    """Atomically write canonical JSONL inventory and return its SHA-256."""
    ordered = tuple(sources)
    if not ordered:
        raise MediaError("cannot write an empty source inventory receipt")
    source_inventory_by_uri(ordered)
    destination = pathlib.Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)
    temporary_path = pathlib.Path(temporary_name)
    try:
        with temporary_path.open("wb") as receipt:
            for source in ordered:
                line = json.dumps(
                    _source_receipt_value(source),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                receipt.write(line.encode())
                receipt.write(b"\n")
            receipt.flush()
            os.fsync(receipt.fileno())
        receipt_sha256 = _file_digests(temporary_path).sha256
        temporary_path.replace(destination)
        _fsync_directory(destination.parent)
    except Exception as error:
        message = f"cannot write source inventory receipt: {destination}"
        raise MediaError(message) from error
    finally:
        temporary_path.unlink(missing_ok=True)
    return receipt_sha256


def read_inventory_receipt(
    receipt_path: pathlib.Path,
) -> tuple[SourceInventory, ...]:
    """Strictly read a previously written source-inventory JSONL receipt."""
    result: list[SourceInventory] = []
    path = pathlib.Path(receipt_path)
    try:
        with path.open(encoding="utf-8") as lines:
            for line_number, line in enumerate(lines, start=1):
                if not line.strip():
                    message = (
                        f"{path}:{line_number}: blank inventory receipt row"
                    )
                    raise MediaError(message)
                value = json.loads(line)
                if not isinstance(value, dict):
                    message = (
                        f"{path}:{line_number}: inventory row is not an object"
                    )
                    raise MediaError(message)
                value["cache_path"] = pathlib.Path(value["cache_path"])
                source = SourceInventory(**value)
                if _source_receipt_value(source) != {
                    **value,
                    "cache_path": str(value["cache_path"]),
                }:
                    message = (
                        f"{path}:{line_number}: malformed inventory receipt row"
                    )
                    raise MediaError(message)
                result.append(source)
    except MediaError:
        raise
    except (OSError, TypeError, KeyError, json.JSONDecodeError) as error:
        message = f"cannot read source inventory receipt: {path}"
        raise MediaError(message) from error
    if not result:
        message = f"source inventory receipt is empty: {path}"
        raise MediaError(message)
    source_inventory_by_uri(result)
    return tuple(result)


def validate_annotation_bounds(
    sources: Mapping[str, SourceInventory] | Iterable[SourceInventory],
    annotations: Iterable[AnnotationFrames],
) -> None:
    """Fail if any exact-frame annotation is absent, empty, or out of bounds."""

    by_uri = (
        dict(sources)
        if isinstance(sources, Mapping)
        else source_inventory_by_uri(sources)
    )
    for annotation in annotations:
        uri = annotation.source_uri
        source = by_uri.get(uri)
        if source is None:
            raise MediaError(
                f"annotation references unenumerated source: {uri}"
            )
        start = annotation.start_frame
        end = annotation.end_frame
        if (
            isinstance(start, bool)
            or not isinstance(start, int)
            or isinstance(end, bool)
            or not isinstance(end, int)
            or start < 0
            or end <= start
            or end > source.frame_count
        ):
            raise MediaError(
                f"{uri}: annotation frame bounds are invalid or out of "
                f"bounds: [{start}, {end}) of {source.frame_count}"
            )


def _read_slice(
    source: SourceInventory,
    start_frame: int,
    end_frame: int,
) -> numpy.ndarray:
    try:
        source_context = soundfile.SoundFile(source.cache_path, mode="r")
    except (OSError, RuntimeError, soundfile.LibsndfileError) as error:
        raise MediaError(
            f"{source.uri}: cached source is undecodable"
        ) from error
    with source_context as opened:
        observed = (
            opened.format,
            opened.samplerate,
            opened.channels,
            opened.subtype,
            opened.frames,
        )
        expected = (
            source.format,
            source.sample_rate,
            source.channels,
            source.subtype,
            source.frame_count,
        )
        if observed != expected:
            raise MediaError(
                f"{source.uri}: cached decoded source geometry drifted"
            )
        opened.seek(start_frame)
        samples = opened.read(
            end_frame - start_frame,
            dtype=_pcm_dtype(source.subtype),
            always_2d=True,
        )
    if samples.shape != (
        end_frame - start_frame,
        source.channels,
    ):
        raise MediaError(
            f"{source.uri}: short source slice read; expected "
            f"{end_frame - start_frame}, observed {samples.shape[0]}"
        )
    return samples


@dataclasses.dataclass(frozen=True, slots=True)
class _EncodingEvidence:
    profile: str
    version: str
    binary_sha256: str
    python_artifact_sha256: str | None
    cffi_artifact_sha256: str | None
    embedded_libflac_version: str | None
    command_sha256: str
    input_pcm_format: str
    production_image_uri: str | None
    production_revision: str | None


def _pcm_formats(subtype: str) -> tuple[str, str]:
    if subtype == "PCM_16":
        return "s16le", "s16"
    if subtype == "PCM_24":
        # libsndfile exposes PCM_24 as left-justified int32.  FFmpeg's s32
        # input and FLAC encoder preserve those exact 24 significant bits.
        return "s32le", "s32"
    _pcm_dtype(subtype)
    raise AssertionError("unreachable")


def _command_sha256(command: Sequence[str]) -> str:
    payload = json.dumps(
        list(command),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _soundfile_command(
    source: SourceInventory,
) -> tuple[tuple[str, ...], str]:
    input_format, _ = _pcm_formats(source.subtype)
    command = (
        "python-soundfile.SoundFile",
        "mode=w",
        f"samplerate={source.sample_rate}",
        f"channels={source.channels}",
        "format=FLAC",
        f"subtype={source.subtype}",
        "{output}",
    )
    return command, input_format


def _ffmpeg_command(
    toolchain: EncoderToolchain,
    source: SourceInventory,
    output_path: pathlib.Path,
) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    input_format, sample_format = _pcm_formats(source.subtype)
    substitutions = {
        "ffmpeg": str(toolchain.ffmpeg_executable),
        "input_format": input_format,
        "sample_rate": str(source.sample_rate),
        "channels": str(source.channels),
        "sample_format": sample_format,
        "output": str(output_path),
    }
    normalized_substitutions = {
        **substitutions,
        "ffmpeg": "{ffmpeg}",
        "output": "{output}",
    }
    command = tuple(
        part.format(**substitutions) for part in _FFMPEG_COMMAND_TEMPLATE
    )
    normalized = tuple(
        part.format(**normalized_substitutions)
        for part in _FFMPEG_COMMAND_TEMPLATE
    )
    return command, normalized, input_format


def _write_soundfile_flac(
    output_path: pathlib.Path,
    samples: numpy.ndarray,
    source: SourceInventory,
    toolchain: EncoderToolchain,
) -> _EncodingEvidence:
    command, input_format = _soundfile_command(source)
    try:
        with soundfile.SoundFile(
            output_path,
            mode="w",
            samplerate=source.sample_rate,
            channels=source.channels,
            format="FLAC",
            subtype=source.subtype,
        ) as destination:
            destination.write(samples)
    except Exception as error:
        raise MediaError(
            f"cannot encode BCFY-feeds FLAC: {output_path}"
        ) from error
    return _EncodingEvidence(
        profile=EncoderProfile.BCFY_FEEDS_SOUNDFILE.value,
        version=(
            f"python-soundfile {toolchain.soundfile_version}; "
            f"libsndfile {toolchain.libsndfile_version}; "
            f"libFLAC {toolchain.embedded_libflac_version}"
        ),
        binary_sha256=toolchain.libsndfile_binary_sha256,
        python_artifact_sha256=(toolchain.soundfile_python_artifact_sha256),
        cffi_artifact_sha256=toolchain.soundfile_cffi_artifact_sha256,
        embedded_libflac_version=toolchain.embedded_libflac_version,
        command_sha256=_command_sha256(command),
        input_pcm_format=input_format,
        production_image_uri=None,
        production_revision=None,
    )


def _write_ffmpeg_flac(
    output_path: pathlib.Path,
    samples: numpy.ndarray,
    source: SourceInventory,
    toolchain: EncoderToolchain,
) -> _EncodingEvidence:
    command, normalized_command, input_format = _ffmpeg_command(
        toolchain,
        source,
        output_path,
    )
    try:
        process = subprocess.run(
            list(command),
            input=_canonical_pcm(samples, source.subtype),
            capture_output=True,
            check=False,
            timeout=_FFMPEG_ENCODE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise MediaError(
            f"pinned FFmpeg could not encode FLAC: {output_path}"
        ) from error
    if process.returncode != 0:
        diagnostic = process.stderr.decode(errors="replace").strip()
        if len(diagnostic) > 2_000:
            diagnostic = diagnostic[-2_000:]
        raise MediaError(
            "pinned FFmpeg FLAC encode failed"
            + (f": {diagnostic}" if diagnostic else "")
        )
    try:
        if output_path.stat().st_size <= 0:
            raise MediaError("pinned FFmpeg produced an empty FLAC")
    except OSError as error:
        raise MediaError("pinned FFmpeg produced no FLAC") from error
    return _EncodingEvidence(
        profile=EncoderProfile.PRESEGMENTED_FFMPEG.value,
        version=toolchain.ffmpeg_version,
        binary_sha256=toolchain.ffmpeg_binary_sha256,
        python_artifact_sha256=None,
        cffi_artifact_sha256=None,
        embedded_libflac_version=None,
        command_sha256=_command_sha256(normalized_command),
        input_pcm_format=input_format,
        production_image_uri=toolchain.ffmpeg_deployed_image_uri,
        production_revision=toolchain.ffmpeg_deployed_revision,
    )


def _verify_materialized(
    output_path: pathlib.Path,
    source: SourceInventory,
    expected_frames: int,
) -> tuple[int, str, str]:
    try:
        encoded = output_path.read_bytes()
    except OSError as error:
        raise MediaError(
            f"cannot read materialized FLAC: {output_path}"
        ) from error
    encoded_sha256 = hashlib.sha256(encoded).hexdigest()
    try:
        output_context = soundfile.SoundFile(io.BytesIO(encoded), mode="r")
    except (OSError, RuntimeError, soundfile.LibsndfileError) as error:
        raise MediaError(
            f"materialized FLAC is undecodable: {output_path}"
        ) from error
    digest = hashlib.sha256()
    decoded_frames = 0
    try:
        with output_context as output:
            observed = (
                output.format,
                output.samplerate,
                output.channels,
                output.subtype,
                output.frames,
            )
            expected = (
                "FLAC",
                source.sample_rate,
                source.channels,
                source.subtype,
                expected_frames,
            )
            if observed != expected:
                raise MediaError(
                    f"materialized FLAC geometry mismatch: {observed!r} "
                    f"!= {expected!r}"
                )
            while decoded_frames < expected_frames:
                requested = min(
                    _DECODE_CHUNK_FRAMES,
                    expected_frames - decoded_frames,
                )
                samples = output.read(
                    requested,
                    dtype=_pcm_dtype(source.subtype),
                    always_2d=True,
                )
                if samples.shape != (requested, source.channels):
                    raise MediaError(
                        "materialized FLAC produced a short decoded read"
                    )
                digest.update(_canonical_pcm(samples, source.subtype))
                decoded_frames += requested
            if (
                output.read(
                    1,
                    dtype=_pcm_dtype(source.subtype),
                    always_2d=True,
                ).shape[0]
                != 0
            ):
                raise MediaError(
                    "materialized FLAC contains undeclared extra frames"
                )
    except MediaError:
        raise
    except (OSError, RuntimeError, soundfile.LibsndfileError) as error:
        raise MediaError(
            f"materialized FLAC is truncated: {output_path}"
        ) from error
    return len(encoded), encoded_sha256, digest.hexdigest()


def materialize_slice(
    source: SourceInventory,
    start_frame: int,
    end_frame: int,
    output_path: pathlib.Path,
    *,
    encoder_profile: EncoderProfile,
    toolchain: EncoderToolchain,
) -> SliceReceipt:
    """Materialize one source-native interval with its family encoder."""

    if (
        isinstance(start_frame, bool)
        or not isinstance(start_frame, int)
        or isinstance(end_frame, bool)
        or not isinstance(end_frame, int)
        or start_frame < 0
        or end_frame <= start_frame
        or end_frame > source.frame_count
    ):
        raise MediaError(
            f"{source.uri}: materialization bounds are invalid or out of "
            f"range: [{start_frame}, {end_frame}) of {source.frame_count}"
        )
    destination = pathlib.Path(output_path)
    if destination.suffix.lower() != ".flac":
        raise MediaError("materialized output path must end in .flac")
    if not isinstance(encoder_profile, EncoderProfile):
        raise MediaError("materialization encoder profile is not frozen")
    if not isinstance(toolchain, EncoderToolchain):
        raise MediaError(
            "materialization requires a validated encoder toolchain"
        )
    try:
        observed_size = source.cache_path.stat().st_size
    except OSError as error:
        raise MediaError(
            f"{source.uri}: cached source bytes are missing"
        ) from error
    if observed_size != source.size_bytes:
        raise MediaError(f"{source.uri}: cached source size drifted")

    samples = _read_slice(source, start_frame, end_frame)
    source_pcm_sha256 = hashlib.sha256(
        _canonical_pcm(samples, source.subtype)
    ).hexdigest()

    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".flac",
    )
    os.close(descriptor)
    temporary_path = pathlib.Path(temporary_name)
    try:
        if encoder_profile is EncoderProfile.BCFY_FEEDS_SOUNDFILE:
            encoding = _write_soundfile_flac(
                temporary_path,
                samples,
                source,
                toolchain,
            )
        elif encoder_profile is EncoderProfile.PRESEGMENTED_FFMPEG:
            encoding = _write_ffmpeg_flac(
                temporary_path,
                samples,
                source,
                toolchain,
            )
        else:
            raise MediaError("unsupported frozen encoder profile")
        encoded_size_bytes, encoded_sha256, output_pcm_sha256 = (
            _verify_materialized(
                temporary_path,
                source,
                end_frame - start_frame,
            )
        )
        if source_pcm_sha256 != output_pcm_sha256:
            raise MediaError(
                f"{source.uri}: output decoded PCM differs from source slice"
            )
        _fsync_file(temporary_path)
        temporary_path.replace(destination)
        _fsync_directory(destination.parent)
    except MediaError:
        raise
    except Exception as error:
        raise MediaError(
            f"cannot losslessly materialize FLAC: {destination}"
        ) from error
    finally:
        temporary_path.unlink(missing_ok=True)

    frame_count = end_frame - start_frame
    return SliceReceipt(
        output_path=destination,
        output_format="FLAC",
        output_codec="flac",
        output_encoded_size_bytes=encoded_size_bytes,
        output_encoded_sha256=encoded_sha256,
        encoder_profile=encoding.profile,
        encoder_version=encoding.version,
        encoder_binary_sha256=encoding.binary_sha256,
        encoder_python_artifact_sha256=(encoding.python_artifact_sha256),
        encoder_cffi_artifact_sha256=encoding.cffi_artifact_sha256,
        encoder_embedded_libflac_version=(encoding.embedded_libflac_version),
        encoder_command_sha256=encoding.command_sha256,
        encoder_contract_sha256=toolchain.contract_sha256,
        encoder_input_pcm_format=encoding.input_pcm_format,
        encoder_production_image_uri=encoding.production_image_uri,
        encoder_production_revision=encoding.production_revision,
        source_uri=source.uri,
        source_generation=source.generation,
        source_encoded_sha256=source.sha256,
        start_frame=start_frame,
        end_frame=end_frame,
        frame_count=frame_count,
        duration_seconds=_duration_string(
            frame_count,
            source.sample_rate,
        ),
        sample_rate=source.sample_rate,
        channels=source.channels,
        subtype=source.subtype,
        source_pcm_sha256=source_pcm_sha256,
        output_decoded_pcm_sha256=output_pcm_sha256,
    )
