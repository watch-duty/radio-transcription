"""Tests for canonical and Gemini manifest serialization."""

# ruff: noqa: INP001, S108

from __future__ import annotations

import dataclasses
import hashlib
import json
import pathlib
import sys

import pytest

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
REPO_ROOT = RUN_ROOT.parents[4]
MODEL_SRC = REPO_ROOT / "model" / "src"
for import_root in (RUN_ROOT, MODEL_SRC):
    while str(import_root) in sys.path:
        sys.path.remove(str(import_root))
sys.path.insert(0, str(RUN_ROOT))
sys.path.insert(0, str(MODEL_SRC))

from dataset_run import domain, manifests, media  # noqa: E402
from gemini_sft import artifacts as gemini_artifacts  # noqa: E402

_A = "a" * 64
_B = "b" * 64


def _request(
    request_id: str,
    *,
    split: domain.Split,
    family: str,
    source_uri: str,
    start_frame: int,
    end_frame: int,
    owner_id: str,
    text: str,
) -> domain.Request:
    return domain.Request(
        request_id=request_id,
        family=family,
        split=split,
        source_uri=source_uri,
        start_frame=start_frame,
        end_frame=end_frame,
        sample_rate=100,
        owner_ids=(owner_id,),
        text=text,
    )


def _binding(
    request: domain.Request,
    tmp_path: pathlib.Path,
    *,
    digest: str = _A,
) -> manifests.MaterializationBinding:
    frame_count = request.end_frame - request.start_frame
    duration = manifests._seconds(frame_count, request.sample_rate)
    is_feed = request.family == "bcfy_feeds"
    return manifests.MaterializationBinding(
        request_id=request.request_id,
        receipt=media.SliceReceipt(
            output_path=tmp_path / f"{request.request_id}.flac",
            output_format="FLAC",
            output_codec="flac",
            output_encoded_size_bytes=123,
            output_encoded_sha256=_B,
            encoder_profile=(
                media.EncoderProfile.BCFY_FEEDS_SOUNDFILE.value
                if is_feed
                else media.EncoderProfile.PRESEGMENTED_FFMPEG.value
            ),
            encoder_version=(
                ("python-soundfile 0.13.1; libsndfile 1.2.2; libFLAC 1.4.3")
                if is_feed
                else "6.1.1"
            ),
            encoder_binary_sha256=(
                media.PINNED_LIBSNDFILE_BINARY_SHA256
                if is_feed
                else media.PINNED_FFMPEG_SHA256
            ),
            encoder_python_artifact_sha256=(
                media.PINNED_SOUNDFILE_PYTHON_ARTIFACT_SHA256
                if is_feed
                else None
            ),
            encoder_cffi_artifact_sha256=(
                media.PINNED_SOUNDFILE_CFFI_ARTIFACT_SHA256 if is_feed else None
            ),
            encoder_embedded_libflac_version=("1.4.3" if is_feed else None),
            encoder_command_sha256=_B,
            encoder_contract_sha256=media.ENCODER_CONTRACT_SHA256,
            encoder_input_pcm_format="s16le",
            encoder_production_image_uri=(
                None if is_feed else "registry/normalization@sha256:" + _A
            ),
            encoder_production_revision=(
                None if is_feed else "normalization-test-revision"
            ),
            source_uri=request.source_uri,
            source_generation=17,
            source_encoded_sha256=_A,
            start_frame=request.start_frame,
            end_frame=request.end_frame,
            frame_count=frame_count,
            duration_seconds=duration,
            sample_rate=request.sample_rate,
            channels=1,
            subtype="PCM_16",
            source_pcm_sha256=digest,
            output_decoded_pcm_sha256=digest,
        ),
    )


def _artifacts(tmp_path: pathlib.Path) -> manifests.ManifestArtifacts:
    train = _request(
        "train-request",
        split=domain.Split.TRAIN,
        family="bcfy_feeds",
        source_uri="gs://source/feed.wav",
        start_frame=150,
        end_frame=275,
        owner_id="train-owner",
        text="Engine 7 responding",
    )
    # Deliberately provide eval out of source order; the serializer freezes it.
    eval_late = _request(
        "eval-late",
        split=domain.Split.EVAL,
        family="bcfy_calls",
        source_uri="gs://source/call.wav",
        start_frame=500,
        end_frame=800,
        owner_id="eval-owner-2",
        text="Second call",
    )
    eval_early = _request(
        "eval-early",
        split=domain.Split.EVAL,
        family="bcfy_calls",
        source_uri="gs://source/call.wav",
        start_frame=0,
        end_frame=250,
        owner_id="eval-owner-1",
        text="First call",
    )
    requests = (eval_late, train, eval_early)
    bindings = tuple(_binding(request, tmp_path) for request in requests)
    return manifests.build_manifest_artifacts(
        requests=requests,
        materialization_bindings=bindings,
        proposed_gcs_prefix="gs://proposed/version/",
        topology_ids_by_request={
            "train-request": ("speech-noise-speech", "source-topology-feed"),
            "eval-late": ("provider-residual-2",),
            "eval-early": ("provider-residual-1",),
        },
        repo_root=REPO_ROOT,
    )


def _lines(payload: bytes) -> list[dict[str, object]]:
    lines = payload.decode("utf-8").splitlines()
    assert lines
    assert all(lines)
    return [json.loads(line) for line in lines]


def test_serializes_deterministic_lineage_and_audio_only_contract(
    tmp_path: pathlib.Path,
) -> None:
    result = _artifacts(tmp_path)

    assert [row["request_id"] for row in result.canonical_eval_rows] == [
        "eval-early",
        "eval-late",
    ]
    early = result.canonical_eval_rows[0]
    assert early["audio_filepath"] == (
        "gs://proposed/version/audio/eval/bcfy_calls/eval-early.flac"
    )
    assert early["request_order"] == 0
    assert early["context_episode_id"] == "gs://source/call.wav"
    assert early["owner_ids"] == ["eval-owner-1"]
    assert early["topology_ids"] == ["provider-residual-1"]
    assert early["source_audio"] == {
        "audio_filepath": "gs://source/call.wav",
        "duration": 2.5,
        "duration_seconds_exact": "2.5",
        "end_frame": 250,
        "offset": 0.0,
        "offset_seconds_exact": "0",
        "sample_rate": 100,
        "start_frame": 0,
    }
    lineage = early["source_lineage"]
    assert isinstance(lineage, dict)
    assert lineage["source_generation"] == 17
    assert lineage["source_start_frame"] == 0
    assert lineage["source_end_frame"] == 250
    assert lineage["context_episode_id"] == "gs://source/call.wav"
    assert "local_output_path" not in json.dumps(early)

    example = result.provider_validation_examples[0]
    assert example["contents"] == [
        {
            "parts": [
                {
                    "fileData": {
                        "fileUri": early["audio_filepath"],
                        "mimeType": "audio/flac",
                    }
                }
            ],
            "role": "user",
        },
        {"parts": [{"text": "First call"}], "role": "model"},
    ]
    assert "local_output_path" not in json.dumps(example)
    assert (
        example["systemInstruction"]["parts"][0]["text"]
        == result.prompt_contract_receipt.system_prompt
    )


def test_full_eval_and_provider_validation_without_atomic_lane(
    tmp_path: pathlib.Path,
) -> None:
    result = _artifacts(tmp_path)

    assert result.canonical_eval_jsonl == result.primary_eval_jsonl
    assert result.canonical_validation_jsonl == manifests._jsonl(
        result.provider_validation_rows
    )
    validation_targets = [
        example["contents"][1]["parts"][0]["text"]
        for example in result.provider_validation_examples
    ]
    assert validation_targets == [
        row["text"] for row in result.canonical_eval_rows
    ]
    assert [
        example["contents"][0]["parts"][0]["fileData"]["fileUri"]
        for example in result.provider_validation_examples
    ] == [row["audio_filepath"] for row in result.canonical_eval_rows]
    lanes = result.eval_lane_receipt["lanes"]
    assert set(lanes) == {
        "masked_v2",
        "predicted_prior_context_10",
        "reconstructed_primary",
    }
    assert result.eval_lane_receipt["atomic_unmasked_eval_included"] is False
    assert (
        result.eval_lane_receipt["validation"][
            "canonical_dataset_is_deterministic_primary_subset"
        ]
        is True
    )
    assert not any("atomic" in path for path in result.files())


def test_small_population_provider_validation_retains_all_primary_rows(
    tmp_path: pathlib.Path,
) -> None:
    result = _artifacts(tmp_path)
    eval_path = tmp_path / "eval.jsonl"
    validation_path = tmp_path / "validation.jsonl"
    eval_path.write_bytes(result.canonical_eval_jsonl)
    validation_path.write_bytes(result.canonical_validation_jsonl)

    eval_entries, eval_rows = gemini_artifacts.load_canonical_rows(
        eval_path, "eval"
    )
    validation_entries, validation_rows = gemini_artifacts.load_canonical_rows(
        validation_path, "validation"
    )

    assert eval_entries == validation_entries
    assert eval_rows == validation_rows
    assert all("split" not in row for row in eval_entries)
    assert result.provider_validation_receipt["selected_count"] == 2
    assert "receipts/provider_validation.json" in result.files()


def test_local_paths_exist_only_in_mapping_receipt(
    tmp_path: pathlib.Path,
) -> None:
    result = _artifacts(tmp_path)
    local_path = str(tmp_path / "eval-early.flac")

    assert local_path in result.materialization_mapping_jsonl.decode()
    mapping = result.materialization_mapping_rows[0]
    assert mapping["encoder"]["profile"] in {
        media.EncoderProfile.BCFY_FEEDS_SOUNDFILE.value,
        media.EncoderProfile.PRESEGMENTED_FFMPEG.value,
    }
    assert mapping["output"]["encoded_size_bytes"] == 123
    assert mapping["output"]["encoded_sha256"] == _B
    feed_mapping = next(
        row
        for row in result.materialization_mapping_rows
        if row["request_id"] == "train-request"
    )
    assert feed_mapping["encoder"] == {
        "binary_sha256": media.PINNED_LIBSNDFILE_BINARY_SHA256,
        "cffi_artifact_sha256": (media.PINNED_SOUNDFILE_CFFI_ARTIFACT_SHA256),
        "command_sha256": _B,
        "contract_sha256": media.ENCODER_CONTRACT_SHA256,
        "embedded_libflac_version": "1.4.3",
        "input_pcm_format": "s16le",
        "production_image_uri": None,
        "production_revision": None,
        "profile": media.EncoderProfile.BCFY_FEEDS_SOUNDFILE.value,
        "python_artifact_sha256": (
            media.PINNED_SOUNDFILE_PYTHON_ARTIFACT_SHA256
        ),
        "version": ("python-soundfile 0.13.1; libsndfile 1.2.2; libFLAC 1.4.3"),
    }
    for payload in (
        result.canonical_train_jsonl,
        result.canonical_eval_jsonl,
        result.gemini_train_jsonl,
        result.gemini_validation_jsonl,
    ):
        assert local_path not in payload.decode()


def test_prompt_contract_matches_both_current_sources() -> None:
    receipt = manifests.load_current_prompt_contract(REPO_ROOT)

    assert (
        receipt.system_prompt_sha256
        == hashlib.sha256(receipt.system_prompt.encode()).hexdigest()
    )
    assert (
        receipt.backend_prompt_source_sha256
        == hashlib.sha256(
            (REPO_ROOT / receipt.backend_prompt_source).read_bytes()
        ).hexdigest()
    )
    assert (
        receipt.model_prompt_source_sha256
        == hashlib.sha256(
            (REPO_ROOT / receipt.model_prompt_source).read_bytes()
        ).hexdigest()
    )
    assert receipt.system_prompt.startswith(
        "You are a verbatim speech-to-text transcription engine"
    )


def _write_prompt_tree(
    root: pathlib.Path,
    *,
    backend_prompt: str,
    model_prompt: str,
) -> None:
    backend = root / manifests._BACKEND_PROMPT_PATH
    model = root / manifests._MODEL_PROMPT_PATH
    contract = root / manifests._LATEST_AUDIO_ONLY_CONTRACT_PATH
    backend.parent.mkdir(parents=True)
    model.parent.mkdir(parents=True)
    contract.parent.mkdir(parents=True)
    backend.write_text(f"GEMINI_PROMPT = {backend_prompt!r}\n")
    model.write_text(f"GEMINI_TRANSCRIBE_SYSTEM_PROMPT = {model_prompt!r}\n")
    contract.write_text("# historical audio-only contract\n")


def test_prompt_drift_between_backend_and_model_fails(
    tmp_path: pathlib.Path,
) -> None:
    _write_prompt_tree(
        tmp_path,
        backend_prompt="production",
        model_prompt="different",
    )

    with pytest.raises(manifests.ManifestError, match="prompt drift"):
        manifests.load_current_prompt_contract(tmp_path)


def test_rejects_non_gcs_prefix(tmp_path: pathlib.Path) -> None:
    train = _request(
        "train",
        split=domain.Split.TRAIN,
        family="echo",
        source_uri="gs://source/train.wav",
        start_frame=0,
        end_frame=10,
        owner_id="owner-train",
        text="train",
    )
    eval_request = _request(
        "eval",
        split=domain.Split.EVAL,
        family="echo",
        source_uri="gs://source/eval.wav",
        start_frame=0,
        end_frame=10,
        owner_id="owner-eval",
        text="eval",
    )

    with pytest.raises(manifests.ManifestError, match="gs:// prefix"):
        manifests.build_manifest_artifacts(
            requests=(train, eval_request),
            materialization_bindings=(
                _binding(train, tmp_path),
                _binding(eval_request, tmp_path),
            ),
            proposed_gcs_prefix="/tmp/not-gcs/",
            topology_ids_by_request={
                "train": ("topology-train",),
                "eval": ("topology-eval",),
            },
            repo_root=REPO_ROOT,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("wrong_format", "MIME"),
        ("wrong_geometry", "receipt mismatch"),
        ("wrong_pcm", "decoded PCM differs"),
        ("wrong_encoder", "encoder profile"),
        ("wrong_contract", "contract SHA-256"),
        ("wrong_binary", "FFmpeg encoder provenance"),
    ],
)
def test_rejects_mismatched_materialization_receipt(
    tmp_path: pathlib.Path,
    mutation: str,
    message: str,
) -> None:
    train = _request(
        "train",
        split=domain.Split.TRAIN,
        family="fire_notifications",
        source_uri="gs://source/train.wav",
        start_frame=10,
        end_frame=20,
        owner_id="owner-train",
        text="train target",
    )
    eval_request = _request(
        "eval",
        split=domain.Split.EVAL,
        family="fire_notifications",
        source_uri="gs://source/eval.wav",
        start_frame=10,
        end_frame=20,
        owner_id="owner-eval",
        text="eval target",
    )
    train_binding = _binding(train, tmp_path)
    receipt = train_binding.receipt
    if mutation == "wrong_format":
        receipt = dataclasses.replace(receipt, output_format="WAV")
    elif mutation == "wrong_geometry":
        receipt = dataclasses.replace(receipt, start_frame=9)
    elif mutation == "wrong_encoder":
        receipt = dataclasses.replace(
            receipt,
            encoder_profile=(media.EncoderProfile.BCFY_FEEDS_SOUNDFILE.value),
            encoder_binary_sha256=_A,
            encoder_production_image_uri=None,
            encoder_production_revision=None,
        )
    elif mutation == "wrong_contract":
        receipt = dataclasses.replace(receipt, encoder_contract_sha256=_A)
    elif mutation == "wrong_binary":
        receipt = dataclasses.replace(receipt, encoder_binary_sha256=_A)
    else:
        receipt = dataclasses.replace(
            receipt,
            output_decoded_pcm_sha256=_B,
        )

    with pytest.raises(manifests.ManifestError, match=message):
        manifests.build_manifest_artifacts(
            requests=(train, eval_request),
            materialization_bindings=(
                manifests.MaterializationBinding("train", receipt),
                _binding(eval_request, tmp_path),
            ),
            proposed_gcs_prefix="gs://proposed/version/",
            topology_ids_by_request={
                "train": ("topology-train",),
                "eval": ("topology-eval",),
            },
            repo_root=REPO_ROOT,
        )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("encoder_binary_sha256", _A),
        ("encoder_python_artifact_sha256", _A),
        ("encoder_cffi_artifact_sha256", _A),
        ("encoder_embedded_libflac_version", "9.9.9"),
    ],
)
def test_rejects_unfrozen_feed_encoder_artifact(
    tmp_path: pathlib.Path,
    field: str,
    replacement: str,
) -> None:
    train = _request(
        "train-feed",
        split=domain.Split.TRAIN,
        family="bcfy_feeds",
        source_uri="gs://source/train-feed.wav",
        start_frame=10,
        end_frame=20,
        owner_id="owner-train",
        text="train target",
    )
    eval_request = _request(
        "eval-call",
        split=domain.Split.EVAL,
        family="bcfy_calls",
        source_uri="gs://source/eval-call.wav",
        start_frame=10,
        end_frame=20,
        owner_id="owner-eval",
        text="eval target",
    )
    train_binding = _binding(train, tmp_path)
    drifted_receipt = dataclasses.replace(
        train_binding.receipt,
        **{field: replacement},
    )

    with pytest.raises(
        manifests.ManifestError, match="feed encoder provenance"
    ):
        manifests.build_manifest_artifacts(
            requests=(train, eval_request),
            materialization_bindings=(
                manifests.MaterializationBinding(
                    train.request_id,
                    drifted_receipt,
                ),
                _binding(eval_request, tmp_path),
            ),
            proposed_gcs_prefix="gs://proposed/version/",
            topology_ids_by_request={
                train.request_id: ("topology-train",),
                eval_request.request_id: ("topology-eval",),
            },
            repo_root=REPO_ROOT,
        )


def test_rejects_duplicate_request_owner_and_missing_topology(
    tmp_path: pathlib.Path,
) -> None:
    train = _request(
        "request",
        split=domain.Split.TRAIN,
        family="bcfy_calls",
        source_uri="gs://source/train.wav",
        start_frame=0,
        end_frame=10,
        owner_id="same-owner",
        text="train",
    )
    duplicate_id = _request(
        "request",
        split=domain.Split.EVAL,
        family="bcfy_calls",
        source_uri="gs://source/eval.wav",
        start_frame=0,
        end_frame=10,
        owner_id="eval-owner",
        text="eval",
    )
    with pytest.raises(manifests.ManifestError, match="duplicate request ID"):
        manifests.build_manifest_artifacts(
            requests=(train, duplicate_id),
            materialization_bindings=(),
            proposed_gcs_prefix="gs://proposed/version/",
            topology_ids_by_request={},
            repo_root=REPO_ROOT,
        )

    eval_request = dataclasses.replace(
        duplicate_id,
        request_id="eval",
        owner_ids=("same-owner",),
    )
    with pytest.raises(manifests.ManifestError, match="multiple requests"):
        manifests.build_manifest_artifacts(
            requests=(train, eval_request),
            materialization_bindings=(),
            proposed_gcs_prefix="gs://proposed/version/",
            topology_ids_by_request={},
            repo_root=REPO_ROOT,
        )

    distinct_eval = dataclasses.replace(
        eval_request,
        owner_ids=("eval-owner",),
    )
    with pytest.raises(manifests.ManifestError, match="topology/request"):
        manifests.build_manifest_artifacts(
            requests=(train, distinct_eval),
            materialization_bindings=(
                _binding(train, tmp_path),
                _binding(distinct_eval, tmp_path),
            ),
            proposed_gcs_prefix="gs://proposed/version/",
            topology_ids_by_request={"request": ("only-one",)},
            repo_root=REPO_ROOT,
        )


def test_missing_and_duplicate_receipt_ids_fail(tmp_path: pathlib.Path) -> None:
    train = _request(
        "train",
        split=domain.Split.TRAIN,
        family="echo",
        source_uri="gs://source/train.wav",
        start_frame=0,
        end_frame=10,
        owner_id="train-owner",
        text="train",
    )
    eval_request = _request(
        "eval",
        split=domain.Split.EVAL,
        family="echo",
        source_uri="gs://source/eval.wav",
        start_frame=0,
        end_frame=10,
        owner_id="eval-owner",
        text="eval",
    )
    topology = {
        "train": ("train-topology",),
        "eval": ("eval-topology",),
    }
    with pytest.raises(manifests.ManifestError, match="receipt/request"):
        manifests.build_manifest_artifacts(
            requests=(train, eval_request),
            materialization_bindings=(_binding(train, tmp_path),),
            proposed_gcs_prefix="gs://proposed/version/",
            topology_ids_by_request=topology,
            repo_root=REPO_ROOT,
        )
    binding = _binding(train, tmp_path)
    with pytest.raises(manifests.ManifestError, match="duplicate"):
        manifests.build_manifest_artifacts(
            requests=(train, eval_request),
            materialization_bindings=(binding, binding),
            proposed_gcs_prefix="gs://proposed/version/",
            topology_ids_by_request=topology,
            repo_root=REPO_ROOT,
        )


def test_jsonl_is_canonical_and_final_newline(tmp_path: pathlib.Path) -> None:
    result = _artifacts(tmp_path)

    for payload in result.files().values():
        assert payload.endswith(b"\n")
        assert not payload.endswith(b"\n\n")
    assert _lines(result.canonical_eval_jsonl) == list(
        result.canonical_eval_rows
    )
