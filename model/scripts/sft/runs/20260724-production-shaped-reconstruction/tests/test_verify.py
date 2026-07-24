"""Tests for the optimizer-independent reconstruction verifier."""

# ruff: noqa: INP001

from __future__ import annotations

import copy
import dataclasses
import hashlib
import io
import json
import pathlib
import sys

import pytest

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(RUN_ROOT) not in sys.path:
    sys.path.insert(0, str(RUN_ROOT))

from dataset_run import verify  # noqa: E402

_A = "a" * 64
_B = "b" * 64
_C = "c" * 64
_ENCODER_CONTRACT_SHA256 = (
    "f0d22af2c589c36fdff9dd1da1b4f82cc93207582b72d102030ef059e3feab29"
)
_LIBSNDFILE_SHA256 = (
    "c872fb84af2afc0e96b603e0000f0f407af86c49ad1126f2785e5722c7bb27d7"
)
_SOUNDFILE_PYTHON_SHA256 = (
    "e349c3858d6a0481fe8078d7a0132603261e7bef2b0facfb9596aeb44c51c563"
)
_SOUNDFILE_CFFI_SHA256 = (
    "31490aab77c158db180b7a6084256a1d9c6e36c2ff9ea98779494df1161f7474"
)
_FFMPEG_SHA256 = (
    "dcf4c806caad507b11c73754b86a8cbd0ce686ae12f90bf8ba235d145f53edc0"
)
_FFMPEG_IMAGE_URI = (
    "us-central1-docker.pkg.dev/automatic-hawk-481415-m9/"
    "radio-transcription-services-prod/"
    "normalization@sha256:"
    "415da267117f80dcf13bf7d3518cb5c0ca80aedd75bb3b4211e1adc4837e0a62"
)
_FFMPEG_REVISION = "normalization-service-prod-00032-zmz"


def _command_sha256(command: tuple[str, ...]) -> str:
    return hashlib.sha256(
        json.dumps(
            list(command),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _encoder_fields(family: str) -> dict[str, object]:
    if family == "bcfy_feeds":
        command = (
            "python-soundfile.SoundFile",
            "mode=w",
            "samplerate=100",
            "channels=1",
            "format=FLAC",
            "subtype=PCM_16",
            "{output}",
        )
        return {
            "encoder_binary_sha256": _LIBSNDFILE_SHA256,
            "encoder_cffi_artifact_sha256": _SOUNDFILE_CFFI_SHA256,
            "encoder_command_sha256": _command_sha256(command),
            "encoder_contract_sha256": _ENCODER_CONTRACT_SHA256,
            "encoder_embedded_libflac_version": "1.4.3",
            "encoder_input_pcm_format": "s16le",
            "encoder_production_image_uri": None,
            "encoder_production_revision": None,
            "encoder_profile": ("bcfy_feeds_soundfile_libflac_pcm_preserve_v1"),
            "encoder_python_artifact_sha256": _SOUNDFILE_PYTHON_SHA256,
            "encoder_version": (
                "python-soundfile 0.13.1; libsndfile 1.2.2; libFLAC 1.4.3"
            ),
        }
    command = (
        "{ffmpeg}",
        "-y",
        "-f",
        "s16le",
        "-ar",
        "100",
        "-ac",
        "1",
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
        "{output}",
    )
    return {
        "encoder_binary_sha256": _FFMPEG_SHA256,
        "encoder_cffi_artifact_sha256": None,
        "encoder_command_sha256": _command_sha256(command),
        "encoder_contract_sha256": _ENCODER_CONTRACT_SHA256,
        "encoder_embedded_libflac_version": None,
        "encoder_input_pcm_format": "s16le",
        "encoder_production_image_uri": _FFMPEG_IMAGE_URI,
        "encoder_production_revision": _FFMPEG_REVISION,
        "encoder_profile": ("presegmented_ffmpeg_6_1_1_flac5_pcm_preserve_v1"),
        "encoder_python_artifact_sha256": None,
        "encoder_version": "6.1.1",
    }


def _jsonl(rows: list[dict[str, object]]) -> bytes:
    return b"".join(
        (
            json.dumps(
                row,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode()
        for row in rows
    )


def _bundle() -> dict[str, object]:
    bundle: dict[str, object] = {
        "construction_contract": {
            "algorithm": "frozen_exact_frame_reconstruction",
            "version": 1,
        },
        "source_inventory": [
            {
                "uri": "gs://audio/feed.wav",
                "generation": 11,
                "size_bytes": 4_000,
                "sha256": _A,
                "decoded_pcm_sha256": _B,
                "frame_count": 1_000,
                "sample_rate": 100,
                "channels": 1,
                "subtype": "PCM_16",
            },
            {
                "uri": "gs://audio/call.wav",
                "generation": 12,
                "size_bytes": 4_000,
                "sha256": _B,
                "decoded_pcm_sha256": _C,
                "frame_count": 1_000,
                "sample_rate": 100,
                "channels": 1,
                "subtype": "PCM_16",
            },
        ],
        "raw_atoms": [
            {
                "atom_id": "feed-atom",
                "source_uri": "gs://audio/feed.wav",
                "start_frame": 100,
                "end_frame": 200,
            },
            {
                "atom_id": "call-atom-1",
                "source_uri": "gs://audio/call.wav",
                "start_frame": 100,
                "end_frame": 200,
            },
            {
                "atom_id": "call-atom-2",
                "source_uri": "gs://audio/call.wav",
                "start_frame": 700,
                "end_frame": 800,
            },
        ],
        "owner_ledger": [
            {
                "owner_id": "feed-owner",
                "split": "train",
                "family": "bcfy_feeds",
                "source_uri": "gs://audio/feed.wav",
                "start_frame": 100,
                "end_frame": 200,
                "text": "Engine seven responding",
                "constituent_ids": ["feed-atom"],
            },
            {
                "owner_id": "call-owner-1",
                "split": "eval",
                "family": "bcfy_calls",
                "source_uri": "gs://audio/call.wav",
                "start_frame": 100,
                "end_frame": 200,
                "text": "First call",
                "constituent_ids": ["call-atom-1"],
                "provider_item_id": "gs://audio/call.wav",
            },
            {
                "owner_id": "call-owner-2",
                "split": "eval",
                "family": "bcfy_calls",
                "source_uri": "gs://audio/call.wav",
                "start_frame": 700,
                "end_frame": 800,
                "text": "Second call",
                "constituent_ids": ["call-atom-2"],
                "provider_item_id": "gs://audio/call.wav",
            },
        ],
        "pii_intervals": [
            {
                # Speech wins this overlap, so it remains in the request.
                "source_uri": "gs://audio/call.wav",
                "start_frame": 150,
                "end_frame": 160,
            },
            {
                # This PII-only island separates two provider-item requests.
                "source_uri": "gs://audio/call.wav",
                "start_frame": 400,
                "end_frame": 600,
            },
        ],
        "requests": [
            {
                "request_id": "feed-request",
                "split": "train",
                "family": "bcfy_feeds",
                "source_uri": "gs://audio/feed.wav",
                "start_frame": 50,
                "end_frame": 250,
                "sample_rate": 100,
                "owner_ids": ["feed-owner"],
                "text": "Engine seven responding",
            },
            {
                "request_id": "call-request-1",
                "split": "eval",
                "family": "bcfy_calls",
                "source_uri": "gs://audio/call.wav",
                "start_frame": 0,
                "end_frame": 400,
                "sample_rate": 100,
                "owner_ids": ["call-owner-1"],
                "text": "First call",
                "provider_item_id": "gs://audio/call.wav",
            },
            {
                "request_id": "call-request-2",
                "split": "eval",
                "family": "bcfy_calls",
                "source_uri": "gs://audio/call.wav",
                "start_frame": 600,
                "end_frame": 1_000,
                "sample_rate": 100,
                "owner_ids": ["call-owner-2"],
                "text": "Second call",
                "provider_item_id": "gs://audio/call.wav",
            },
        ],
        "physical_groups": {
            "gs://audio/feed.wav": "feed-group",
            "gs://audio/call.wav": "call-group",
            "gs://audio/masked.wav": "masked-group",
        },
        "materialization_receipts": [
            {
                **_encoder_fields("bcfy_feeds"),
                "request_id": "feed-request",
                "source_uri": "gs://audio/feed.wav",
                "start_frame": 50,
                "end_frame": 250,
                "source_pcm_sha256": _A,
                "output_pcm_sha256": _A,
                "output_format": "FLAC",
                "output_codec": "flac",
                "output_encoded_sha256": _C,
                "output_encoded_size_bytes": 901,
                "output_frame_count": 200,
                "output_sample_rate": 100,
                "output_channels": 1,
                "output_subtype": "PCM_16",
            },
            {
                **_encoder_fields("bcfy_calls"),
                "request_id": "call-request-1",
                "source_uri": "gs://audio/call.wav",
                "start_frame": 0,
                "end_frame": 400,
                "source_pcm_sha256": _B,
                "output_pcm_sha256": _B,
                "output_format": "FLAC",
                "output_codec": "flac",
                "output_encoded_sha256": _A,
                "output_encoded_size_bytes": 902,
                "output_frame_count": 400,
                "output_sample_rate": 100,
                "output_channels": 1,
                "output_subtype": "PCM_16",
            },
            {
                **_encoder_fields("bcfy_calls"),
                "request_id": "call-request-2",
                "source_uri": "gs://audio/call.wav",
                "start_frame": 600,
                "end_frame": 1_000,
                "source_pcm_sha256": _C,
                "output_pcm_sha256": _C,
                "output_format": "FLAC",
                "output_codec": "flac",
                "output_encoded_sha256": _B,
                "output_encoded_size_bytes": 903,
                "output_frame_count": 400,
                "output_sample_rate": 100,
                "output_channels": 1,
                "output_subtype": "PCM_16",
            },
        ],
    }
    primary_rows: list[dict[str, object]] = [
        {
            "audio_filepath": "gs://built/eval/call-request-1.flac",
            "context_episode_id": "gs://audio/call.wav",
            "dataset": {"family": "bcfy_calls", "name": "bcfy_calls"},
            "example_id": "call-request-1",
            "owner_ids": ["call-owner-1"],
            "request_id": "call-request-1",
            "request_order": 0,
            "segment_id": "call-request-1",
            "source_audio": {
                "audio_filepath": "gs://audio/call.wav",
                "end_frame": 400,
                "sample_rate": 100,
                "start_frame": 0,
            },
            "source_lineage": {
                "context_episode_id": "gs://audio/call.wav",
            },
            "text": "First call",
        },
        {
            "audio_filepath": "gs://built/eval/call-request-2.flac",
            "context_episode_id": "gs://audio/call.wav",
            "dataset": {"family": "bcfy_calls", "name": "bcfy_calls"},
            "example_id": "call-request-2",
            "owner_ids": ["call-owner-2"],
            "request_id": "call-request-2",
            "request_order": 1,
            "segment_id": "call-request-2",
            "source_audio": {
                "audio_filepath": "gs://audio/call.wav",
                "end_frame": 1_000,
                "sample_rate": 100,
                "start_frame": 600,
            },
            "source_lineage": {
                "context_episode_id": "gs://audio/call.wav",
            },
            "text": "Second call",
        },
    ]
    schedule_rows: list[dict[str, object]] = [
        {
            "audio_uri": "gs://built/eval/call-request-1.flac",
            "context_episode_id": "gs://audio/call.wav",
            "example_id": "call-request-1",
            "manifest_index": 0,
            "request_id": "call-request-1",
            "segment_id": "call-request-1",
            "source_ordinal": 0,
            "source_uri": "gs://audio/call.wav",
        },
        {
            "audio_uri": "gs://built/eval/call-request-2.flac",
            "context_episode_id": "gs://audio/call.wav",
            "example_id": "call-request-2",
            "manifest_index": 1,
            "request_id": "call-request-2",
            "segment_id": "call-request-2",
            "source_ordinal": 1,
            "source_uri": "gs://audio/call.wav",
        },
    ]
    masked_row = {
        "audio_filepath": "gs://built/masked/masked-1.flac",
        "dataset": {"family": "bcfy_calls", "name": "bcfy_calls"},
        "example_id": "masked-example",
        "provenance_ids": ["masked-provenance"],
        "segment_id": "masked-segment",
        "split": "eval",
        "text": "Masked reference",
    }
    masked_payload_sha256 = hashlib.sha256(_jsonl([masked_row])).hexdigest()
    primary_sha256 = hashlib.sha256(_jsonl(primary_rows)).hexdigest()
    schedule_sha256 = hashlib.sha256(_jsonl(schedule_rows)).hexdigest()
    ranked_provider_rows: list[tuple[str, str, str, dict[str, object]]] = []
    for row in primary_rows:
        request_id = str(row["request_id"])
        source_audio = row["source_audio"]
        assert isinstance(source_audio, dict)
        identity = [
            request_id,
            request_id,
            "gs://audio/call.wav",
            source_audio["start_frame"],
            source_audio["end_frame"],
            source_audio["sample_rate"],
        ]
        stable_id = hashlib.sha256(
            json.dumps(
                identity,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        rank = hashlib.sha256(
            f"{primary_sha256}\0gs://audio/call.wav\0{stable_id}".encode()
        ).hexdigest()
        ranked_provider_rows.append((rank, stable_id, request_id, row))
    ranked_provider_rows.sort(key=lambda item: (item[0], item[1]))
    provider_rows = [item[3] for item in ranked_provider_rows]
    chosen_stable_ids = [item[1] for item in ranked_provider_rows]
    selected_request_ids = [item[2] for item in ranked_provider_rows]
    provider_sha256 = hashlib.sha256(_jsonl(provider_rows)).hexdigest()
    bundle.update(
        {
            "primary_eval_rows": primary_rows,
            "validation_rows": copy.deepcopy(provider_rows),
            "provider_validation_rows": copy.deepcopy(provider_rows),
            "provider_validation_receipt": {
                "algorithm": (
                    "production_shape_dataset_duration_word_"
                    "largest_remainder_source_round_robin_v1"
                ),
                "cap": 5_000,
                "chosen_stable_ids": chosen_stable_ids,
                "duration_buckets": ["<2", "2-<5", "5-<15", ">=15"],
                "full_primary_count": 2,
                "full_primary_sha256": primary_sha256,
                "input_lock_digest": primary_sha256,
                "max_train_fraction": "3/10",
                "model_outcome_fields_used": [],
                "precedent": (
                    "20260712 dataset-duration-word Hamilton plus "
                    "hash-ranked source round-robin"
                ),
                "precedent_adaptations": [
                    "salt_is_full_primary_eval_sha256",
                    "timing_identity_uses_exact_source_frames",
                ],
                "schema_version": 1,
                "selected_count": 2,
                "selected_request_ids": selected_request_ids,
                "selected_request_ids_sha256": hashlib.sha256(
                    (
                        json.dumps(
                            selected_request_ids,
                            allow_nan=False,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        )
                        + "\n"
                    ).encode()
                ).hexdigest(),
                "selected_rows_sha256": provider_sha256,
                "selection_order": (
                    "lexical_stratum_then_hash_ranked_source_round_robin"
                ),
                "selector_fields": [
                    "dataset.name",
                    "exact_duration_frames/sample_rate",
                    "whitespace_target_word_count",
                    "source_audio.audio_filepath",
                    "stable_row_id",
                ],
                "strata": [
                    {
                        "chosen_stable_ids": chosen_stable_ids,
                        "dataset": "bcfy_calls",
                        "duration_bucket": "2-<5",
                        "population_size": 2,
                        "quota": 2,
                        "sources": [
                            {
                                "chosen_count": 2,
                                "population_size": 2,
                                "source_uri": "gs://audio/call.wav",
                            }
                        ],
                        "word_bucket": "1-3",
                    }
                ],
                "target_count": 2,
                "train_count": 1,
                "word_buckets": [
                    "1-3",
                    "4-9",
                    "10-16",
                    "17-24",
                    "25-37",
                    ">=38",
                ],
            },
            "prior_context_schedule_rows": schedule_rows,
            "manifest_sha256": {
                "reconstructed_primary": primary_sha256,
                "validation": provider_sha256,
            },
            "eval_lanes": {
                "reconstructed_primary": {
                    "kind": "manifest",
                    "row_count": 2,
                    "sha256": primary_sha256,
                },
                "predicted_prior_context_10": {
                    "kind": "inference_view",
                    "base_lane": "reconstructed_primary",
                    "max_prior_segments": 10,
                    "prior_transcript_source": "predicted_transcription",
                    "request_count": 2,
                    "schedule_sha256": schedule_sha256,
                    "uses_reference_transcription": False,
                },
                "masked_v2": {
                    "kept_count": 1,
                    "kind": "masked_v2",
                    "rows_resegmented": False,
                },
            },
            "masked_rows_by_family": {
                "bcfy_calls": [masked_row],
                "bcfy_feeds": [],
                "echo": [],
                "fire_notifications": [],
            },
            "masked_row_projections": [
                {
                    "family": "bcfy_calls",
                    "group_id": "masked-group",
                    "payload_sha256": masked_payload_sha256,
                    "row_id": ("bcfy_calls:masked-example:masked-segment"),
                    "source_uri": "gs://audio/masked.wav",
                    "split": "eval",
                }
            ],
            "masked_filter_receipt": {
                "excluded_count": 0,
                "families": [
                    {
                        "excluded_count": 0,
                        "family": family,
                        "input_count": 1 if family == "bcfy_calls" else 0,
                        "kept_count": 1 if family == "bcfy_calls" else 0,
                        "kept_rows": (
                            [
                                {
                                    "example_id": "masked-example",
                                    "row_sha256": masked_payload_sha256,
                                    "segment_id": "masked-segment",
                                }
                            ]
                            if family == "bcfy_calls"
                            else []
                        ),
                        "rows_resegmented": False,
                    }
                    for family in verify.MASKED_DATASET_FAMILIES
                ],
                "input_count": 1,
                "kept_count": 1,
                "rows_resegmented": False,
            },
            "masked_selection_proof_sha256": masked_payload_sha256,
            "masked_retained_payload_sha256": masked_payload_sha256,
        }
    )
    return bundle


def _request(bundle: dict[str, object], request_id: str) -> dict[str, object]:
    requests = bundle["requests"]
    assert isinstance(requests, list)
    return next(
        request
        for request in requests
        if isinstance(request, dict) and request["request_id"] == request_id
    )


def _rehash_masked(bundle: dict[str, object]) -> None:
    rows_by_family = bundle["masked_rows_by_family"]
    projections = bundle["masked_row_projections"]
    receipt = bundle["masked_filter_receipt"]
    assert isinstance(rows_by_family, dict)
    assert isinstance(projections, list)
    assert isinstance(receipt, dict)
    family_receipts = receipt["families"]
    assert isinstance(family_receipts, list)
    flattened: list[dict[str, object]] = []
    projection_index = 0
    for family, family_receipt in zip(
        verify.MASKED_DATASET_FAMILIES,
        family_receipts,
        strict=True,
    ):
        rows = rows_by_family[family]
        assert isinstance(rows, list)
        assert isinstance(family_receipt, dict)
        kept_receipts = family_receipt["kept_rows"]
        assert isinstance(kept_receipts, list)
        for row, kept_receipt in zip(rows, kept_receipts, strict=True):
            assert isinstance(row, dict)
            assert isinstance(kept_receipt, dict)
            digest = hashlib.sha256(_jsonl([row])).hexdigest()
            kept_receipt["row_sha256"] = digest
            projection = projections[projection_index]
            assert isinstance(projection, dict)
            projection["payload_sha256"] = digest
            projection_index += 1
            flattened.append(row)
    bundle["masked_retained_payload_sha256"] = hashlib.sha256(
        _jsonl(flattened)
    ).hexdigest()


def test_complete_bundle_passes_and_prints_complete() -> None:
    output = io.StringIO()

    report = verify.verify_and_print(_bundle(), output=output)

    assert output.getvalue() == "COMPLETE\n"
    assert report.as_dict() == {
        "source_count": 2,
        "owner_count": 3,
        "raw_atom_count": 3,
        "train_request_count": 1,
        "eval_request_count": 2,
        "masked_row_count": 1,
    }


def test_protected_owner_must_appear_exactly_once() -> None:
    bundle = _bundle()
    requests = bundle["requests"]
    assert isinstance(requests, list)
    requests[:] = [
        request
        for request in requests
        if not (
            isinstance(request, dict)
            and request["request_id"] == "call-request-2"
        )
    ]

    with pytest.raises(verify.VerificationError, match="exactly 1"):
        verify.verify_artifacts(bundle)


def test_composite_cannot_repeat_a_constituent() -> None:
    bundle = _bundle()
    owners = bundle["owner_ledger"]
    assert isinstance(owners, list)
    owners.append(
        {
            "owner_id": "composite-owner",
            "split": "train",
            "family": "bcfy_feeds",
            "source_uri": "gs://audio/feed.wav",
            "start_frame": 100,
            "end_frame": 200,
            "text": "duplicate composite",
            "constituent_ids": ["feed-atom"],
        }
    )

    with pytest.raises(
        verify.VerificationError,
        match="constituent/composite double inclusion",
    ):
        verify.verify_artifacts(bundle)


def test_request_must_not_cut_an_owner() -> None:
    bundle = _bundle()
    request = _request(bundle, "feed-request")
    request["end_frame"] = 150

    with pytest.raises(verify.VerificationError, match="cuts owner"):
        verify.verify_artifacts(bundle)


def test_requests_must_not_overlap_within_source_and_split() -> None:
    bundle = _bundle()
    request = _request(bundle, "call-request-2")
    request["start_frame"] = 300

    with pytest.raises(
        verify.VerificationError,
        match="duplicate request frame overlap",
    ):
        verify.verify_artifacts(bundle)


def test_request_cannot_include_pii_only_frames() -> None:
    bundle = _bundle()
    pii = bundle["pii_intervals"]
    assert isinstance(pii, list)
    pii.append(
        {
            "source_uri": "gs://audio/feed.wav",
            "start_frame": 50,
            "end_frame": 75,
        }
    )

    with pytest.raises(verify.VerificationError, match="PII-only"):
        verify.verify_artifacts(bundle)


def test_speech_over_pii_is_allowed() -> None:
    bundle = _bundle()
    pii = bundle["pii_intervals"]
    assert isinstance(pii, list)
    pii.append(
        {
            "source_uri": "gs://audio/feed.wav",
            "start_frame": 110,
            "end_frame": 190,
        }
    )

    verify.verify_artifacts(bundle)


def test_composite_envelope_does_not_turn_gaps_into_speech() -> None:
    bundle = _bundle()
    atoms = bundle["raw_atoms"]
    assert isinstance(atoms, list)
    feed_atom = atoms[0]
    assert isinstance(feed_atom, dict)
    feed_atom["end_frame"] = 150
    pii = bundle["pii_intervals"]
    assert isinstance(pii, list)
    pii.append(
        {
            "source_uri": "gs://audio/feed.wav",
            "start_frame": 160,
            "end_frame": 170,
        }
    )

    with pytest.raises(verify.VerificationError, match="PII-only"):
        verify.verify_artifacts(bundle)


def test_request_text_must_be_nonblank() -> None:
    bundle = _bundle()
    _request(bundle, "feed-request")["text"] = " \n "

    with pytest.raises(verify.VerificationError, match="nonblank"):
        verify.verify_artifacts(bundle)


def test_presegmented_request_can_split_only_at_pii_only_boundary() -> None:
    bundle = _bundle()
    request = _request(bundle, "call-request-1")
    request["end_frame"] = 350

    with pytest.raises(
        verify.VerificationError,
        match="not a PII-only interval",
    ):
        verify.verify_artifacts(bundle)


def test_presegmented_request_cannot_claim_another_provider_item() -> None:
    bundle = _bundle()
    request = _request(bundle, "call-request-1")
    request["provider_item_id"] = "another-provider-item"

    with pytest.raises(
        verify.VerificationError,
        match="provider_item_id must equal",
    ):
        verify.verify_artifacts(bundle)


def test_calls_and_fire_require_provider_item_not_archive_identity() -> None:
    bundle = _bundle()
    request = _request(bundle, "call-request-1")
    del request["provider_item_id"]

    with pytest.raises(
        verify.VerificationError,
        match="provider_item_id must equal",
    ):
        verify.verify_artifacts(bundle)

    bundle = _bundle()
    request = _request(bundle, "call-request-1")
    request["archive_source_id"] = "gs://audio/call.wav"
    with pytest.raises(
        verify.VerificationError,
        match="falsely claims archive_source_id",
    ):
        verify.verify_artifacts(bundle)


def test_owner_lineage_fields_cannot_be_missing_or_false() -> None:
    bundle = _bundle()
    owners = bundle["owner_ledger"]
    assert isinstance(owners, list)
    call_owner = owners[1]
    assert isinstance(call_owner, dict)
    del call_owner["provider_item_id"]
    with pytest.raises(
        verify.VerificationError,
        match="provider_item_id must equal",
    ):
        verify.verify_artifacts(bundle)

    bundle = _bundle()
    owners = bundle["owner_ledger"]
    assert isinstance(owners, list)
    feed_owner = owners[0]
    assert isinstance(feed_owner, dict)
    feed_owner["archive_source_id"] = "gs://audio/feed.wav"
    with pytest.raises(
        verify.VerificationError,
        match="must not claim provider or archive",
    ):
        verify.verify_artifacts(bundle)


def test_echo_owner_lineage_requires_archive_and_forbids_provider() -> None:
    with pytest.raises(
        verify.VerificationError,
        match="exact Echo archive_source_id",
    ):
        verify._check_family_lineage(
            family="echo",
            source_uri="gs://audio/echo-hour.flac",
            provider_item_id=None,
            archive_source_id=None,
            label="echo owner",
        )
    with pytest.raises(
        verify.VerificationError,
        match="falsely claims provider_item_id",
    ):
        verify._check_family_lineage(
            family="echo",
            source_uri="gs://audio/echo-hour.flac",
            provider_item_id="gs://audio/echo-hour.flac",
            archive_source_id="gs://audio/echo-hour.flac",
            label="echo owner",
        )


def test_bcfy_feed_request_cannot_claim_provider_or_archive_identity() -> None:
    bundle = _bundle()
    request = _request(bundle, "feed-request")
    request["provider_item_id"] = "gs://audio/feed.wav"

    with pytest.raises(
        verify.VerificationError,
        match="must not claim provider or archive",
    ):
        verify.verify_artifacts(bundle)


def _echo_owner(
    owner_id: str,
    start_frame: int,
    end_frame: int,
) -> verify._Owner:
    return verify._Owner(
        source_uri="gs://audio/echo-hour.flac",
        start_frame=start_frame,
        end_frame=end_frame,
        owner_id=owner_id,
        split="eval",
        family="echo",
        text=owner_id,
        constituent_ids=(owner_id,),
        provider_item_id=None,
        archive_source_id="gs://audio/echo-hour.flac",
    )


def _echo_request(
    request_id: str,
    start_frame: int,
    end_frame: int,
    owner_ids: tuple[str, ...],
) -> verify._Request:
    return verify._Request(
        source_uri="gs://audio/echo-hour.flac",
        start_frame=start_frame,
        end_frame=end_frame,
        request_id=request_id,
        split="eval",
        family="echo",
        sample_rate=16_000,
        owner_ids=owner_ids,
        text=" ".join(owner_ids),
        provider_item_id=None,
        archive_source_id="gs://audio/echo-hour.flac",
    )


def test_echo_uses_strict_overlap_components_and_splits_touching_speech() -> (
    None
):
    owners = {
        owner.owner_id: owner
        for owner in (
            _echo_owner("echo-1", 100, 200),
            _echo_owner("echo-2", 150, 250),
            # Touching the first component at frame 250 must stay separate.
            _echo_owner("echo-3", 250, 300),
        )
    }
    requests = {
        request.request_id: request
        for request in (
            _echo_request(
                "echo-request-1",
                100,
                250,
                ("echo-1", "echo-2"),
            ),
            _echo_request("echo-request-2", 250, 300, ("echo-3",)),
        )
    }

    verify._check_echo_geometry(owners, requests)

    merged = {
        "echo-request": _echo_request(
            "echo-request",
            100,
            300,
            ("echo-1", "echo-2", "echo-3"),
        )
    }
    with pytest.raises(
        verify.VerificationError,
        match="2 strict-overlap Speech components",
    ):
        verify._check_echo_geometry(owners, merged)


def test_echo_request_cannot_absorb_positive_blank_archive_context() -> None:
    owner = _echo_owner("echo-1", 100, 200)
    request = _echo_request("echo-request", 90, 200, ("echo-1",))

    with pytest.raises(
        verify.VerificationError,
        match="positive blank/unannotated gap",
    ):
        verify._check_echo_geometry(
            {owner.owner_id: owner},
            {request.request_id: request},
        )


def test_echo_requires_archive_identity_and_rejects_provider_identity() -> None:
    owner = _echo_owner("echo-1", 100, 200)
    valid = _echo_request("echo-request", 100, 200, ("echo-1",))

    missing_archive = dataclasses.replace(valid, archive_source_id=None)
    with pytest.raises(
        verify.VerificationError,
        match="exact archive_source_id",
    ):
        verify._check_echo_geometry(
            {owner.owner_id: owner},
            {missing_archive.request_id: missing_archive},
        )

    false_provider = dataclasses.replace(
        valid,
        provider_item_id="gs://audio/echo-hour.flac",
    )
    with pytest.raises(
        verify.VerificationError,
        match="falsely claims a provider_item_id",
    ):
        verify._check_echo_geometry(
            {owner.owner_id: owner},
            {false_provider.request_id: false_provider},
        )


def test_physical_group_cannot_cross_train_and_eval() -> None:
    bundle = _bundle()
    groups = bundle["physical_groups"]
    assert isinstance(groups, dict)
    groups["gs://audio/call.wav"] = "feed-group"

    with pytest.raises(
        verify.VerificationError,
        match="train/eval physical-group leak",
    ):
        verify.verify_artifacts(bundle)


def test_validation_manifest_hash_is_bound_to_actual_projection() -> None:
    bundle = _bundle()
    digests = bundle["manifest_sha256"]
    assert isinstance(digests, dict)
    digests["validation"] = _B

    with pytest.raises(verify.VerificationError, match="manifest SHA-256"):
        verify.verify_artifacts(bundle)


def test_actual_validation_row_order_must_match_provider_projection() -> None:
    bundle = _bundle()
    rows = bundle["validation_rows"]
    assert isinstance(rows, list)
    rows.reverse()
    digests = bundle["manifest_sha256"]
    assert isinstance(digests, dict)
    digests["validation"] = hashlib.sha256(_jsonl(rows)).hexdigest()

    with pytest.raises(
        verify.VerificationError,
        match="byte-identical",
    ):
        verify.verify_artifacts(bundle)


def _provider_test_row(
    request_id: str,
    source_uri: str,
    *,
    start_frame: int,
    end_frame: int,
    text: str = "one two",
) -> dict[str, object]:
    return {
        "audio_filepath": f"gs://built/{request_id}.flac",
        "dataset": {"family": "bcfy_calls", "name": "bcfy_calls"},
        "duration": (end_frame - start_frame) / 100,
        "example_id": request_id,
        "request_id": request_id,
        "segment_id": request_id,
        "source_audio": {
            "audio_filepath": source_uri,
            "end_frame": end_frame,
            "sample_rate": 100,
            "start_frame": start_frame,
        },
        "text": text,
    }


def test_provider_validation_target_uses_cap_train_fraction_and_floor() -> None:
    assert verify._provider_target(800, 1) == 800
    assert verify._provider_target(20_000, 100) == 1_000
    assert verify._provider_target(20_000, 20_000) == 5_000


def test_provider_strata_use_exact_duration_and_word_count() -> None:
    row = _provider_test_row(
        "request",
        "gs://audio/source.flac",
        start_frame=100,
        end_frame=300,
        text="one two three four",
    )
    # The selector must ignore this rounded display value and use 200/100.
    row["duration"] = 1.999

    assert verify._provider_stratum_key(row, "row") == (
        "bcfy_calls",
        "2-<5",
        "4-9",
    )


def test_provider_selection_preserves_july12_round_robin_precedent() -> None:
    rows = [
        _provider_test_row(
            f"a-{index}",
            "gs://audio/a.flac",
            start_frame=index * 100,
            end_frame=(index + 1) * 100,
        )
        for index in range(3)
    ]
    rows.extend(
        _provider_test_row(
            f"b-{index}",
            "gs://audio/b.flac",
            start_frame=index * 100,
            end_frame=(index + 1) * 100,
        )
        for index in range(3)
    )

    selected, _stable_ids, _strata, _salt = (
        verify._expected_provider_validation(rows, train_count=1)
    )
    sources = [
        verify._provider_source_uri(row, "selected row") for row in selected
    ]

    assert sources[0] != sources[1]
    assert sources[:2] == sources[2:4] == sources[4:6]


def test_provider_projection_rejects_duplicates_before_receipt_metadata() -> (
    None
):
    bundle = _bundle()
    rows = bundle["provider_validation_rows"]
    validation = bundle["validation_rows"]
    assert isinstance(rows, list)
    assert isinstance(validation, list)
    rows[1] = copy.deepcopy(rows[0])
    validation[1] = copy.deepcopy(validation[0])
    digests = bundle["manifest_sha256"]
    assert isinstance(digests, dict)
    digests["validation"] = hashlib.sha256(_jsonl(validation)).hexdigest()

    with pytest.raises(
        verify.VerificationError,
        match="duplicate requests",
    ):
        verify.verify_artifacts(bundle)


def test_provider_receipt_cannot_claim_outcome_based_selection() -> None:
    bundle = _bundle()
    receipt = bundle["provider_validation_receipt"]
    assert isinstance(receipt, dict)
    receipt["model_outcome_fields_used"] = ["wer"]

    with pytest.raises(
        verify.VerificationError,
        match="independently recomputed production-shape selector",
    ):
        verify.verify_artifacts(bundle)


def test_provider_receipt_hashes_are_recomputed_from_full_primary() -> None:
    bundle = _bundle()
    receipt = bundle["provider_validation_receipt"]
    assert isinstance(receipt, dict)
    receipt["input_lock_digest"] = _A

    with pytest.raises(
        verify.VerificationError,
        match="independently recomputed production-shape selector",
    ):
        verify.verify_artifacts(bundle)


def test_actual_primary_rows_must_cover_eval_requests() -> None:
    bundle = _bundle()
    rows = bundle["primary_eval_rows"]
    validation = bundle["validation_rows"]
    assert isinstance(rows, list)
    assert isinstance(validation, list)
    rows.pop()
    validation.pop()
    digest = hashlib.sha256(_jsonl(rows)).hexdigest()
    digests = bundle["manifest_sha256"]
    lanes = bundle["eval_lanes"]
    assert isinstance(digests, dict)
    assert isinstance(lanes, dict)
    primary_lane = lanes["reconstructed_primary"]
    assert isinstance(primary_lane, dict)
    digests["reconstructed_primary"] = digest
    digests["validation"] = digest
    primary_lane["row_count"] = 1
    primary_lane["sha256"] = digest

    with pytest.raises(
        verify.VerificationError,
        match="primary eval/request mismatch",
    ):
        verify.verify_artifacts(bundle)


def test_primary_context_episode_id_is_derived_from_family_semantics() -> None:
    bundle = _bundle()
    rows = bundle["primary_eval_rows"]
    assert isinstance(rows, list)
    row = rows[0]
    assert isinstance(row, dict)
    lineage = row["source_lineage"]
    assert isinstance(lineage, dict)
    lineage["context_episode_id"] = "call-request-1"

    with pytest.raises(
        verify.VerificationError,
        match="context_episode_id does not match",
    ):
        verify.verify_artifacts(bundle)


def test_atomic_or_other_eval_lane_is_rejected() -> None:
    bundle = _bundle()
    lanes = bundle["eval_lanes"]
    assert isinstance(lanes, dict)
    lanes["atomic_unmasked"] = {"kind": "manifest"}

    with pytest.raises(verify.VerificationError, match="eval lanes"):
        verify.verify_artifacts(bundle)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_prior_segments", 8),
        ("prior_transcript_source", "reference_transcription"),
        ("uses_reference_transcription", True),
    ],
)
def test_prior_context_lane_uses_ten_predictions_only(
    field: str,
    value: object,
) -> None:
    bundle = _bundle()
    lanes = bundle["eval_lanes"]
    assert isinstance(lanes, dict)
    prior = lanes["predicted_prior_context_10"]
    assert isinstance(prior, dict)
    prior[field] = value

    with pytest.raises(verify.VerificationError, match="prior-context-10"):
        verify.verify_artifacts(bundle)


def test_prior_context_schedule_must_cover_primary_once_in_frozen_order() -> (
    None
):
    bundle = _bundle()
    schedule = bundle["prior_context_schedule_rows"]
    assert isinstance(schedule, list)
    first = schedule[0]
    assert isinstance(first, dict)
    first["source_ordinal"] = 1

    with pytest.raises(
        verify.VerificationError,
        match="coverage/order/context-episode resets",
    ):
        verify.verify_artifacts(bundle)


def test_prior_context_schedule_cannot_carry_reference_text() -> None:
    bundle = _bundle()
    schedule = bundle["prior_context_schedule_rows"]
    assert isinstance(schedule, list)
    first = schedule[0]
    assert isinstance(first, dict)
    first["reference_text"] = "First call"

    with pytest.raises(
        verify.VerificationError,
        match="reference transcription field",
    ):
        verify.verify_artifacts(bundle)


def test_prior_context_schedule_hash_is_recomputed_from_actual_rows() -> None:
    bundle = _bundle()
    lanes = bundle["eval_lanes"]
    assert isinstance(lanes, dict)
    prior = lanes["predicted_prior_context_10"]
    assert isinstance(prior, dict)
    prior["schedule_sha256"] = _A

    with pytest.raises(
        verify.VerificationError,
        match="schedule SHA-256",
    ):
        verify.verify_artifacts(bundle)


def test_echo_archive_requests_use_independent_context_episodes() -> None:
    primary_rows: list[dict[str, object]] = []
    for index, request_id in enumerate(("echo-request-1", "echo-request-2")):
        primary_rows.append(
            {
                "audio_filepath": f"gs://built/eval/{request_id}.flac",
                "dataset": {"family": "echo", "name": "echo"},
                "example_id": request_id,
                "request_id": request_id,
                "request_order": index,
                "segment_id": request_id,
                "source_audio": {
                    "audio_filepath": "gs://audio/echo-hour.flac",
                    "end_frame": 200 + index * 200,
                    "sample_rate": 100,
                    "start_frame": 100 + index * 200,
                },
            }
        )
    schedule = verify._expected_prior_schedule(primary_rows)
    assert [row["context_episode_id"] for row in schedule] == [
        "echo-request-1",
        "echo-request-2",
    ]
    assert [row["source_ordinal"] for row in schedule] == [0, 0]

    schedule[1]["context_episode_id"] = "gs://audio/echo-hour.flac"
    payload_sha256 = hashlib.sha256(
        _jsonl([dict(row) for row in schedule])
    ).hexdigest()
    with pytest.raises(
        verify.VerificationError,
        match="context-episode resets",
    ):
        verify._check_prior_context_schedule(
            {"prior_context_schedule_rows": schedule},
            primary_rows,
            {
                "request_count": 2,
                "schedule_sha256": payload_sha256,
            },
        )


def test_masked_rows_must_be_disjoint_from_training_groups() -> None:
    bundle = _bundle()
    projections = bundle["masked_row_projections"]
    assert isinstance(projections, list)
    projection = projections[0]
    assert isinstance(projection, dict)
    projection["source_uri"] = "gs://audio/feed.wav"
    projection["group_id"] = "feed-group"

    with pytest.raises(verify.VerificationError, match="training physical"):
        verify.verify_artifacts(bundle)


def test_masked_rows_must_be_disjoint_from_training_provenance() -> None:
    bundle = _bundle()
    rows_by_family = bundle["masked_rows_by_family"]
    assert isinstance(rows_by_family, dict)
    rows = rows_by_family["bcfy_calls"]
    assert isinstance(rows, list)
    row = rows[0]
    assert isinstance(row, dict)
    row["provenance_ids"] = ["feed-atom"]
    _rehash_masked(bundle)

    with pytest.raises(verify.VerificationError, match="training provenance"):
        verify.verify_artifacts(bundle)


def test_masked_payload_must_match_frozen_per_row_hash() -> None:
    bundle = _bundle()
    rows_by_family = bundle["masked_rows_by_family"]
    assert isinstance(rows_by_family, dict)
    rows = rows_by_family["bcfy_calls"]
    assert isinstance(rows, list)
    row = rows[0]
    assert isinstance(row, dict)
    row["text"] = "Changed after filtering"

    with pytest.raises(
        verify.VerificationError,
        match="changed after the masked filter receipt",
    ):
        verify.verify_artifacts(bundle)


def test_masked_payload_must_match_its_frozen_payload_digest() -> None:
    bundle = _bundle()
    rows_by_family = bundle["masked_rows_by_family"]
    assert isinstance(rows_by_family, dict)
    rows = rows_by_family["bcfy_calls"]
    assert isinstance(rows, list)
    row = rows[0]
    assert isinstance(row, dict)
    row["text"] = "Changed everywhere except the frozen proof"
    payload_digest = bundle["masked_retained_payload_sha256"]
    _rehash_masked(bundle)
    bundle["masked_retained_payload_sha256"] = payload_digest

    with pytest.raises(
        verify.VerificationError,
        match="retained payload SHA-256",
    ):
        verify.verify_artifacts(bundle)


def test_masked_selection_proof_and_raw_payload_use_separate_hashes() -> None:
    bundle = _bundle()
    bundle["masked_selection_proof_sha256"] = _A

    report = verify.verify_artifacts(bundle)

    assert report.masked_row_count == 1


def test_masked_projection_payload_hash_must_match_actual_row() -> None:
    bundle = _bundle()
    projections = bundle["masked_row_projections"]
    assert isinstance(projections, list)
    projection = projections[0]
    assert isinstance(projection, dict)
    projection["payload_sha256"] = _A

    with pytest.raises(
        verify.VerificationError,
        match="payload_sha256",
    ):
        verify.verify_artifacts(bundle)


def test_masked_rows_require_all_four_family_partitions() -> None:
    bundle = _bundle()
    rows_by_family = bundle["masked_rows_by_family"]
    assert isinstance(rows_by_family, dict)
    del rows_by_family["echo"]

    with pytest.raises(
        verify.VerificationError,
        match="exactly bcfy_calls",
    ):
        verify.verify_artifacts(bundle)


def test_source_inventory_bounds_are_enforced() -> None:
    bundle = _bundle()
    request = _request(bundle, "call-request-2")
    request["end_frame"] = 1_001

    with pytest.raises(verify.VerificationError, match="out of source bounds"):
        verify.verify_artifacts(bundle)


def test_materialized_pcm_must_match_source_slice() -> None:
    bundle = _bundle()
    receipts = bundle["materialization_receipts"]
    assert isinstance(receipts, list)
    receipt = receipts[0]
    assert isinstance(receipt, dict)
    receipt["output_pcm_sha256"] = _B

    with pytest.raises(verify.VerificationError, match="decoded PCM differs"):
        verify.verify_artifacts(bundle)


def test_materialized_encoded_size_must_be_positive_when_recorded() -> None:
    bundle = _bundle()
    receipts = bundle["materialization_receipts"]
    assert isinstance(receipts, list)
    receipt = receipts[0]
    assert isinstance(receipt, dict)
    receipt["output_encoded_size_bytes"] = 0

    with pytest.raises(
        verify.VerificationError,
        match="output_encoded_size_bytes",
    ):
        verify.verify_artifacts(bundle)


@pytest.mark.parametrize(
    ("receipt_index", "field_name", "tampered_value", "message"),
    [
        (
            0,
            "encoder_profile",
            "presegmented_ffmpeg_6_1_1_flac5_pcm_preserve_v1",
            "encoder_profile",
        ),
        (0, "encoder_contract_sha256", _B, "encoder_contract_sha256"),
        (0, "encoder_binary_sha256", _A, "encoder_binary_sha256"),
        (
            0,
            "encoder_python_artifact_sha256",
            _A,
            "encoder_python_artifact_sha256",
        ),
        (
            0,
            "encoder_cffi_artifact_sha256",
            _A,
            "encoder_cffi_artifact_sha256",
        ),
        (
            0,
            "encoder_embedded_libflac_version",
            "0.0.0",
            "encoder_embedded_libflac_version",
        ),
        (1, "encoder_binary_sha256", _A, "encoder_binary_sha256"),
        (
            1,
            "encoder_production_image_uri",
            "example.invalid/image@sha256:" + _A,
            "encoder_production_image_uri",
        ),
        (
            1,
            "encoder_production_revision",
            "wrong-revision",
            "encoder_production_revision",
        ),
        (0, "encoder_command_sha256", _A, "encoder_command_sha256"),
        (
            0,
            "encoder_input_pcm_format",
            "s32le",
            "encoder_input_pcm_format",
        ),
        (0, "output_format", "WAV", "FLAC container/codec"),
        (0, "output_codec", "pcm_s16le", "FLAC container/codec"),
        (0, "output_frame_count", 199, "output_frame_count"),
        (0, "output_sample_rate", 101, "output_sample_rate"),
        (0, "output_channels", 2, "output_channels"),
        (0, "output_subtype", "PCM_24", "output_subtype"),
        (0, "output_encoded_sha256", "not-a-sha256", "output_encoded_sha256"),
    ],
)
def test_materialization_encoder_provenance_is_fail_closed(
    receipt_index: int,
    field_name: str,
    tampered_value: object,
    message: str,
) -> None:
    bundle = _bundle()
    receipts = bundle["materialization_receipts"]
    assert isinstance(receipts, list)
    receipt = receipts[receipt_index]
    assert isinstance(receipt, dict)
    receipt[field_name] = tampered_value

    with pytest.raises(verify.VerificationError, match=message):
        verify.verify_artifacts(bundle)


@pytest.mark.parametrize(
    ("receipt_index", "field_name", "tampered_value", "message"),
    [
        (0, "encoder_production_image_uri", _FFMPEG_IMAGE_URI, "must be null"),
        (0, "encoder_version", "", "encoder_version"),
        (
            0,
            "encoder_python_artifact_sha256",
            None,
            "encoder_python_artifact_sha256",
        ),
        (1, "encoder_binary_sha256", None, "encoder_binary_sha256"),
        (1, "encoder_cffi_artifact_sha256", _A, "must be null"),
        (1, "encoder_embedded_libflac_version", "1.4.3", "must be null"),
        (1, "encoder_production_revision", None, "production_revision"),
    ],
)
def test_materialization_encoder_nullability_is_profile_specific(
    receipt_index: int,
    field_name: str,
    tampered_value: object,
    message: str,
) -> None:
    bundle = _bundle()
    receipts = bundle["materialization_receipts"]
    assert isinstance(receipts, list)
    receipt = receipts[receipt_index]
    assert isinstance(receipt, dict)
    receipt[field_name] = tampered_value

    with pytest.raises(verify.VerificationError, match=message):
        verify.verify_artifacts(bundle)


def test_materialization_encoder_receipt_requires_exact_schema() -> None:
    bundle = _bundle()
    receipts = bundle["materialization_receipts"]
    assert isinstance(receipts, list)
    receipt = receipts[0]
    assert isinstance(receipt, dict)
    del receipt["encoder_command_sha256"]

    with pytest.raises(verify.VerificationError, match="schema mismatch"):
        verify.verify_artifacts(bundle)


@pytest.mark.parametrize(
    "forbidden_key",
    [
        "prediction_source",
        "reference_context",
        "wer",
        "finish_reason",
        "http_status",
        "fallback_result",
        "model_output",
    ],
)
def test_outcome_fields_cannot_influence_construction(
    forbidden_key: str,
) -> None:
    bundle = _bundle()
    contract = bundle["construction_contract"]
    assert isinstance(contract, dict)
    contract["selector"] = {forbidden_key: "anything"}

    with pytest.raises(
        verify.VerificationError,
        match="outcome-dependent construction field",
    ):
        verify.verify_artifacts(bundle)


def test_failure_does_not_print_complete() -> None:
    bundle = _bundle()
    output = io.StringIO()
    digests = bundle["manifest_sha256"]
    assert isinstance(digests, dict)
    digests["validation"] = _A

    with pytest.raises(verify.VerificationError):
        verify.verify_and_print(bundle, output=output)

    assert output.getvalue() == ""
