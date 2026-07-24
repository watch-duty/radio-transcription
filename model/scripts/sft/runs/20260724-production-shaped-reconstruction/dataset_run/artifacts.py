"""Immutable input cache and create-only build artifacts.

All writes are content-addressed or create-only.  An existing path is accepted
only when its bytes are exactly the expected bytes; an existing mismatch is a
hard error.  Publication is deliberately exposed as an explicit function and
is never called by acquisition or local artifact helpers.
"""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import json
import os
import pathlib
import shutil
import tempfile
import typing
from collections.abc import Callable, Iterable, Mapping
from typing import Any

if typing.TYPE_CHECKING:
    from .config import LockedInput, RunConfig

_COPY_CHUNK_BYTES = 1024 * 1024
_PUBLICATION_TIMEOUT_SECONDS = 1800


class ArtifactError(RuntimeError):
    """Raised when an immutable input or build artifact drifts."""


@dataclasses.dataclass(frozen=True, slots=True)
class FileIdentity:
    """Observed local byte identity."""

    size: int
    sha256: str


@dataclasses.dataclass(frozen=True, slots=True)
class CachedInput:
    """A verified input inside the deterministic cache."""

    lock: LockedInput
    path: pathlib.Path
    identity: FileIdentity


@dataclasses.dataclass(frozen=True, slots=True)
class ArtifactReceipt:
    """Evidence returned by a create-only local artifact write."""

    path: pathlib.Path
    size: int
    sha256: str
    created: bool


@dataclasses.dataclass(frozen=True, slots=True)
class PublicationReceipt:
    """Provider identity returned by an explicit create-only publication."""

    uri: str
    generation: int
    size: int
    sha256: str


def canonical_json_bytes(value: object) -> bytes:
    """Encode canonical JSON with exactly one trailing newline."""

    try:
        rendered = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise ArtifactError("value is not canonical JSON") from error
    return rendered.encode("utf-8") + b"\n"


def canonical_json_sha256(value: object) -> str:
    """Hash the canonical JSON representation of ``value``."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def inspect_file(path: str | pathlib.Path) -> FileIdentity:
    """Read one regular file completely and return its size and SHA-256."""

    source = pathlib.Path(path)
    digest = hashlib.sha256()
    size = 0
    try:
        if not source.is_file():
            raise ArtifactError(f"not a regular file: {source}")
        with source.open("rb") as opened:
            while chunk := opened.read(_COPY_CHUNK_BYTES):
                size += len(chunk)
                digest.update(chunk)
    except ArtifactError:
        raise
    except OSError as error:
        raise ArtifactError(f"cannot read file: {source}") from error
    return FileIdentity(size=size, sha256=digest.hexdigest())


def _validate_identity(
    path: pathlib.Path,
    *,
    expected_size: int,
    expected_sha256: str,
    label: str,
) -> FileIdentity:
    observed = inspect_file(path)
    if observed.size != expected_size:
        raise ArtifactError(
            f"{label}: size drift; expected {expected_size}, "
            f"observed {observed.size}"
        )
    if observed.sha256 != expected_sha256:
        raise ArtifactError(
            f"{label}: SHA-256 drift; expected {expected_sha256}, "
            f"observed {observed.sha256}"
        )
    return observed


def _fsync_directory(path: pathlib.Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    except (AttributeError, OSError):
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _temporary_path(destination: pathlib.Path, *, suffix: str) -> pathlib.Path:
    descriptor, name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=suffix,
    )
    os.close(descriptor)
    return pathlib.Path(name)


def _commit_create_only(
    temporary: pathlib.Path,
    destination: pathlib.Path,
    expected: FileIdentity,
) -> ArtifactReceipt:
    """Atomically install a temporary file without overwriting a race winner."""

    if destination.exists():
        observed = _validate_identity(
            destination,
            expected_size=expected.size,
            expected_sha256=expected.sha256,
            label=str(destination),
        )
        return ArtifactReceipt(
            path=destination,
            size=observed.size,
            sha256=observed.sha256,
            created=False,
        )
    try:
        # link(2) supplies the no-replace guarantee that os.replace lacks.
        # The temporary file and destination share a filesystem because the
        # temporary is always allocated in destination.parent.
        os.link(temporary, destination)
        _fsync_directory(destination.parent)
        return ArtifactReceipt(
            path=destination,
            size=expected.size,
            sha256=expected.sha256,
            created=True,
        )
    except FileExistsError:
        observed = _validate_identity(
            destination,
            expected_size=expected.size,
            expected_sha256=expected.sha256,
            label=str(destination),
        )
        return ArtifactReceipt(
            path=destination,
            size=observed.size,
            sha256=observed.sha256,
            created=False,
        )
    except OSError as error:
        raise ArtifactError(
            f"cannot atomically create artifact: {destination}"
        ) from error


def write_bytes_create_only(
    destination: str | pathlib.Path,
    payload: bytes,
) -> ArtifactReceipt:
    """Atomically create one local artifact, accepting only identical reruns."""

    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")
    path = pathlib.Path(destination)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ArtifactError(
            f"cannot create artifact directory: {path.parent}"
        ) from error
    expected = FileIdentity(
        size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    temporary = _temporary_path(path, suffix=".tmp")
    try:
        with temporary.open("wb") as opened:
            opened.write(payload)
            opened.flush()
            os.fsync(opened.fileno())
        return _commit_create_only(temporary, path, expected)
    except ArtifactError:
        raise
    except OSError as error:
        raise ArtifactError(f"cannot write artifact: {path}") from error
    finally:
        temporary.unlink(missing_ok=True)


def copy_file_create_only(
    source: str | pathlib.Path,
    destination: str | pathlib.Path,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
) -> ArtifactReceipt:
    """Copy one file to a create-only artifact while hashing the copied bytes."""

    source_path = pathlib.Path(source)
    destination_path = pathlib.Path(destination)
    try:
        destination_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ArtifactError(
            f"cannot create artifact directory: {destination_path.parent}"
        ) from error
    temporary = _temporary_path(destination_path, suffix=".copy")
    digest = hashlib.sha256()
    size = 0
    try:
        if not source_path.is_file():
            raise ArtifactError(f"not a regular file: {source_path}")
        with source_path.open("rb") as opened, temporary.open("wb") as copied:
            while chunk := opened.read(_COPY_CHUNK_BYTES):
                copied.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            copied.flush()
            os.fsync(copied.fileno())
        observed = FileIdentity(size=size, sha256=digest.hexdigest())
        if expected_size is not None and observed.size != expected_size:
            raise ArtifactError(
                f"{source_path}: size drift; expected {expected_size}, "
                f"observed {observed.size}"
            )
        if expected_sha256 is not None and observed.sha256 != expected_sha256:
            raise ArtifactError(
                f"{source_path}: SHA-256 drift; expected {expected_sha256}, "
                f"observed {observed.sha256}"
            )
        return _commit_create_only(
            temporary,
            destination_path,
            observed,
        )
    except ArtifactError:
        raise
    except OSError as error:
        raise ArtifactError(
            f"cannot copy artifact: {source_path} -> {destination_path}"
        ) from error
    finally:
        temporary.unlink(missing_ok=True)


def write_json_create_only(
    destination: str | pathlib.Path,
    value: object,
) -> ArtifactReceipt:
    """Write canonical JSON with one trailing newline."""

    return write_bytes_create_only(destination, canonical_json_bytes(value))


def write_jsonl_create_only(
    destination: str | pathlib.Path,
    rows: Iterable[object],
) -> ArtifactReceipt:
    """Write canonical JSONL with no blank rows and a final newline."""

    payload = b"".join(canonical_json_bytes(row) for row in rows)
    return write_bytes_create_only(destination, payload)


def write_flac_create_only(
    destination: str | pathlib.Path,
    payload: bytes,
) -> ArtifactReceipt:
    """Write already-encoded FLAC bytes as an immutable local artifact."""

    if pathlib.Path(destination).suffix.lower() != ".flac":
        raise ArtifactError("FLAC artifact path must end in .flac")
    if not payload.startswith(b"fLaC"):
        raise ArtifactError("FLAC artifact payload lacks the fLaC signature")
    return write_bytes_create_only(destination, payload)


def _cache_key(lock: LockedInput) -> str:
    identity = {
        "generation": lock.generation,
        "label": lock.label,
        "sha256": lock.sha256,
        "size": lock.size,
        "uri": lock.uri,
    }
    return canonical_json_sha256(identity)


def _cache_receipt(lock: LockedInput) -> dict[str, object]:
    return {
        "generation": lock.generation,
        "label": lock.label,
        "schema_version": 1,
        "sha256": lock.sha256,
        "size": lock.size,
        "uri": lock.uri,
    }


def _validate_cached(
    directory: pathlib.Path,
    lock: LockedInput,
) -> CachedInput:
    payload = directory / "payload"
    receipt_path = directory / "receipt.json"
    if not directory.is_dir() or not receipt_path.is_file():
        raise ArtifactError(f"{lock.label}: incomplete immutable cache entry")
    try:
        receipt_bytes = receipt_path.read_bytes()
    except OSError as error:
        raise ArtifactError(
            f"{lock.label}: unreadable cache receipt"
        ) from error
    expected_receipt = canonical_json_bytes(_cache_receipt(lock))
    if receipt_bytes != expected_receipt:
        raise ArtifactError(
            f"{lock.label}: cache receipt byte or canonicalization drift"
        )
    identity = _validate_identity(
        payload,
        expected_size=lock.size,
        expected_sha256=lock.sha256,
        label=lock.label,
    )
    return CachedInput(lock=lock, path=payload, identity=identity)


def _download_gcs_locked(
    lock: LockedInput,
    destination: pathlib.Path,
    storage_client: object,
) -> None:
    blob = _verified_gcs_blob(lock, storage_client)
    assert lock.generation is not None
    try:
        blob.download_to_filename(
            str(destination),
            if_generation_match=lock.generation,
        )
        blob.reload(if_generation_match=lock.generation)
    except Exception as error:
        raise ArtifactError(
            f"{lock.label}: generation-locked GCS download failed"
        ) from error
    if blob.generation != lock.generation or blob.size != lock.size:
        raise ArtifactError(
            f"{lock.label}: provider identity drifted during download"
        )


def _verified_gcs_blob(lock: LockedInput, storage_client: object) -> object:
    if lock.generation is None:
        raise ArtifactError(f"{lock.label}: GCS input requires a generation")
    try:
        bucket_name, object_name = lock.uri.removeprefix("gs://").split("/", 1)
        blob = storage_client.bucket(bucket_name).blob(object_name)
        blob.reload()
        observed_generation = blob.generation
        observed_size = blob.size
    except Exception as error:
        raise ArtifactError(
            f"{lock.label}: GCS metadata lookup failed"
        ) from error
    if (
        isinstance(observed_generation, bool)
        or not isinstance(observed_generation, int)
        or observed_generation != lock.generation
    ):
        raise ArtifactError(
            f"{lock.label}: generation drift; expected {lock.generation}, "
            f"observed {observed_generation!r}"
        )
    if (
        isinstance(observed_size, bool)
        or not isinstance(observed_size, int)
        or observed_size != lock.size
    ):
        raise ArtifactError(
            f"{lock.label}: provider size drift; expected {lock.size}, "
            f"observed {observed_size!r}"
        )
    return blob


def _local_source(lock: LockedInput) -> pathlib.Path:
    if lock.generation is not None:
        raise ArtifactError(
            f"{lock.label}: local input cannot carry a GCS generation"
        )
    rendered = (
        lock.uri.removeprefix("file://")
        if lock.uri.startswith("file://")
        else lock.uri
    )
    return pathlib.Path(rendered)


def _verify_local_source(lock: LockedInput) -> pathlib.Path:
    source = _local_source(lock)
    _validate_identity(
        source,
        expected_size=lock.size,
        expected_sha256=lock.sha256,
        label=lock.label,
    )
    return source


def _copy_local_locked(lock: LockedInput, destination: pathlib.Path) -> None:
    source = _verify_local_source(lock)
    try:
        with source.open("rb") as opened, destination.open("wb") as copied:
            shutil.copyfileobj(opened, copied, length=_COPY_CHUNK_BYTES)
            copied.flush()
            os.fsync(copied.fileno())
    except ArtifactError:
        raise
    except OSError as error:
        raise ArtifactError(f"{lock.label}: local input read failed") from error


def acquire_locked_input(
    lock: LockedInput,
    cache_root: str | pathlib.Path,
    *,
    storage_client: object | None = None,
) -> CachedInput:
    """Acquire exactly the configured bytes into a deterministic cache.

    GCS acquisition checks provider generation and size both before and after
    the generation-preconditioned download.  Every source, including a local
    one, is then checked against the locked size and SHA-256.
    """

    root = pathlib.Path(cache_root)
    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ArtifactError(f"cannot create input cache: {root}") from error
    entry = root / _cache_key(lock)
    if entry.exists():
        if lock.uri.startswith("gs://"):
            if storage_client is None:
                try:
                    from google.cloud import storage

                    storage_client = storage.Client()
                except Exception as error:
                    raise ArtifactError(
                        f"{lock.label}: cannot construct GCS client"
                    ) from error
            _verified_gcs_blob(lock, storage_client)
        else:
            _verify_local_source(lock)
        return _validate_cached(entry, lock)

    temporary = pathlib.Path(
        tempfile.mkdtemp(prefix=f".{lock.label}-", dir=root)
    )
    try:
        payload = temporary / "payload"
        if lock.uri.startswith("gs://"):
            if storage_client is None:
                try:
                    from google.cloud import storage

                    storage_client = storage.Client()
                except Exception as error:
                    raise ArtifactError(
                        f"{lock.label}: cannot construct GCS client"
                    ) from error
            _download_gcs_locked(lock, payload, storage_client)
        else:
            _copy_local_locked(lock, payload)
        _validate_identity(
            payload,
            expected_size=lock.size,
            expected_sha256=lock.sha256,
            label=lock.label,
        )
        write_json_create_only(
            temporary / "receipt.json",
            _cache_receipt(lock),
        )
        _fsync_directory(temporary)
        try:
            temporary.rename(entry)
            temporary = pathlib.Path()
            _fsync_directory(root)
        except FileExistsError:
            # Another process completed the exact content-addressed entry.
            return _validate_cached(entry, lock)
    finally:
        if temporary != pathlib.Path():
            shutil.rmtree(temporary, ignore_errors=True)
    return _validate_cached(entry, lock)


def acquire_config_inputs(
    config: RunConfig,
    *,
    storage_client: object | None = None,
) -> dict[str, CachedInput]:
    """Acquire every configured manifest without changing external state."""

    result: dict[str, CachedInput] = {}
    for lock in config.all_inputs:
        result[lock.label] = acquire_locked_input(
            lock,
            config.workspace / "inputs",
            storage_client=storage_client,
        )
    return result


def _artifact_evidence(
    artifact: ArtifactReceipt | str | pathlib.Path,
) -> dict[str, object]:
    if isinstance(artifact, ArtifactReceipt):
        observed = _validate_identity(
            artifact.path,
            expected_size=artifact.size,
            expected_sha256=artifact.sha256,
            label=str(artifact.path),
        )
        path = artifact.path
    else:
        path = pathlib.Path(artifact)
        observed = inspect_file(path)
    return {
        "path": str(path),
        "sha256": observed.sha256,
        "size": observed.size,
    }


def write_stage_marker(
    stage: str,
    marker_path: str | pathlib.Path,
    *,
    artifacts: Iterable[ArtifactReceipt | str | pathlib.Path],
    verify: Callable[[], object],
    metadata: Mapping[str, Any] | None = None,
) -> ArtifactReceipt:
    """Verify a stage and only then create its immutable completion marker.

    ``verify`` must either return ``None``/``True`` or a JSON-compatible
    mapping of verification facts.  Returning ``False`` is an explicit
    failure; raising aborts the marker write unchanged.
    """

    if (
        not isinstance(stage, str)
        or not stage.strip()
        or stage != stage.strip()
    ):
        raise ArtifactError("stage must be a non-blank trimmed string")
    if not callable(verify):
        raise TypeError("verify must be callable")

    artifact_items = tuple(artifacts)
    evidence = [_artifact_evidence(item) for item in artifact_items]
    evidence.sort(key=lambda item: str(item["path"]))
    if len({str(item["path"]) for item in evidence}) != len(evidence):
        raise ArtifactError(f"{stage}: duplicate artifact in stage marker")

    verification = verify()
    if verification is False:
        raise ArtifactError(f"{stage}: caller verification failed")
    if verification is None or verification is True:
        verification_facts: Mapping[str, object] = {}
    elif isinstance(verification, Mapping):
        verification_facts = dict(verification)
    else:
        raise ArtifactError(
            f"{stage}: verifier must return None, True, False, or a mapping"
        )

    after_verification = [_artifact_evidence(item) for item in artifact_items]
    after_verification.sort(key=lambda item: str(item["path"]))
    if after_verification != evidence:
        raise ArtifactError(
            f"{stage}: artifact bytes changed during caller verification"
        )
    marker = {
        "artifacts": evidence,
        "metadata": dict(metadata or {}),
        "schema_version": 1,
        "stage": stage,
        "verification": dict(verification_facts),
        "verified": True,
    }
    receipt = write_json_create_only(marker_path, marker)
    try:
        after_marker = [_artifact_evidence(item) for item in artifact_items]
        after_marker.sort(key=lambda item: str(item["path"]))
        if after_marker != evidence:
            raise ArtifactError(
                f"{stage}: artifact bytes changed while committing marker"
            )
    except Exception:
        if receipt.created:
            current_marker = inspect_file(receipt.path)
            if (
                current_marker.size == receipt.size
                and current_marker.sha256 == receipt.sha256
            ):
                receipt.path.unlink()
                _fsync_directory(receipt.path.parent)
        raise
    return receipt


def publish_file_create_only(
    storage_client: object,
    local_path: str | pathlib.Path,
    gcs_uri: str,
    *,
    content_type: str,
) -> PublicationReceipt:
    """Explicitly publish one file without overwriting a GCS object.

    Nothing in this module invokes this function automatically.  The caller
    must obtain the separate publication approval before calling it.
    """

    source = pathlib.Path(local_path)
    try:
        if not source.is_file():
            raise ArtifactError(f"not a regular file: {source}")
        payload = source.read_bytes()
    except ArtifactError:
        raise
    except OSError as error:
        raise ArtifactError(
            f"cannot read publication source: {source}"
        ) from error
    identity = FileIdentity(
        size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    if not isinstance(gcs_uri, str) or not gcs_uri.startswith("gs://"):
        raise ArtifactError("publication URI must be a gs:// object URI")
    if gcs_uri.endswith("/"):
        raise ArtifactError("publication URI must name one object")
    if not isinstance(content_type, str) or not content_type.strip():
        raise ArtifactError("content_type must be a non-blank string")
    try:
        from google.api_core.exceptions import PreconditionFailed

        bucket_name, object_name = gcs_uri.removeprefix("gs://").split("/", 1)
        if not bucket_name or not object_name:
            raise ArtifactError(
                "publication URI must include a bucket and object name"
            )
        blob = storage_client.bucket(bucket_name).blob(object_name)
        with contextlib.suppress(PreconditionFailed):
            blob.upload_from_string(
                payload,
                content_type=content_type,
                if_generation_match=0,
                timeout=_PUBLICATION_TIMEOUT_SECONDS,
            )
        # The exact-byte comparison below determines whether a suppressed
        # precondition failure was an idempotent rerun or an immutable conflict.
        blob.reload(timeout=_PUBLICATION_TIMEOUT_SECONDS)
        generation = blob.generation
        size = blob.size
        observed_content_type = blob.content_type
        if (
            isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation <= 0
        ):
            raise ArtifactError(f"{gcs_uri}: invalid published generation")
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or size != identity.size
        ):
            raise ArtifactError(
                f"published size drift for {gcs_uri}: expected "
                f"{identity.size}, observed {size!r}"
            )
        if observed_content_type != content_type:
            raise ArtifactError(
                f"published content-type drift for {gcs_uri}: expected "
                f"{content_type!r}, observed {observed_content_type!r}"
            )
        persisted = blob.download_as_bytes(
            if_generation_match=generation,
            timeout=_PUBLICATION_TIMEOUT_SECONDS,
        )
        if persisted != payload:
            raise ArtifactError(
                f"refusing to adopt existing mismatched object: {gcs_uri}"
            )
    except ArtifactError:
        raise
    except Exception as error:
        raise ArtifactError(
            f"create-only publication failed: {gcs_uri}"
        ) from error

    return PublicationReceipt(
        uri=gcs_uri,
        generation=generation,
        size=identity.size,
        sha256=identity.sha256,
    )
