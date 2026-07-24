"""Focused tests for one-time local reconstruction staging."""

# ruff: noqa: INP001

from __future__ import annotations

import base64
import dataclasses
import decimal
import fractions
import hashlib
import json
import pathlib
import sys
from types import SimpleNamespace

import pytest

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(RUN_ROOT) not in sys.path:
    sys.path.insert(0, str(RUN_ROOT))

from dataset_run import (  # noqa: E402
    artifacts,
    cli,
    continuous,
    domain,
    eval_views,
    inputs,
    manifests,
    media,
    ownership,
    presegmented,
    stage,
    verify,
)

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_MD5 = base64.b64encode(bytes(16)).decode()
_CRC32C = base64.b64encode(bytes(4)).decode()


def _toolchain(tmp_path: pathlib.Path) -> media.EncoderToolchain:
    return media.EncoderToolchain(
        contract_sha256=media.ENCODER_CONTRACT_SHA256,
        soundfile_version="0.13.1",
        soundfile_python_artifact_path=tmp_path / "soundfile.py",
        soundfile_python_artifact_sha256=(
            media.PINNED_SOUNDFILE_PYTHON_ARTIFACT_SHA256
        ),
        soundfile_python_artifact_size_bytes=64_864,
        soundfile_cffi_artifact_path=tmp_path / "_soundfile.py",
        soundfile_cffi_artifact_sha256=(
            media.PINNED_SOUNDFILE_CFFI_ARTIFACT_SHA256
        ),
        soundfile_cffi_artifact_size_bytes=5_783,
        libsndfile_version="1.2.2",
        libsndfile_binary_path=tmp_path / "libsndfile_x86_64.so",
        libsndfile_binary_sha256=media.PINNED_LIBSNDFILE_BINARY_SHA256,
        libsndfile_binary_size_bytes=3_639_184,
        embedded_libflac_version="1.4.3",
        ffmpeg_executable=tmp_path / "ffmpeg",
        ffmpeg_binary_sha256=media.PINNED_FFMPEG_SHA256,
        ffmpeg_binary_size_bytes=97_674_920,
        ffmpeg_version="6.1.1",
        ffmpeg_version_line="ffmpeg version 6.1.1 test",
        ffmpeg_deployed_image_uri=("registry/normalization@sha256:" + _SHA_A),
        ffmpeg_deployed_revision="normalization-test-revision",
    )


def _source(
    uri: str,
    cache_path: pathlib.Path,
    *,
    generation: int = 1,
) -> media.SourceInventory:
    return media.SourceInventory(
        uri=uri,
        generation=generation,
        size_bytes=100,
        md5_hash=_MD5,
        crc32c=_CRC32C,
        sha256=_SHA_A,
        format="WAV",
        sample_rate=100,
        channels=1,
        subtype="PCM_16",
        frame_count=1_000,
        duration_seconds="10",
        cache_path=cache_path,
    )


def _raw(
    *,
    raw_index: int,
    atom_class: domain.AtomClass,
    text: str | None,
    source_uri: str = "gs://audio/source.wav",
) -> inputs.RawAnnotation:
    return inputs.RawAnnotation(
        locator=inputs.RawLocator("feeds_train", raw_index),
        family="bcfy_feeds",
        source_split="train",
        source_uri=source_uri,
        offset_seconds=decimal.Decimal(raw_index),
        duration_seconds=decimal.Decimal(1),
        atom_class=atom_class,
        category="PII" if atom_class is domain.AtomClass.PII else None,
        text=text,
    )


def _owned(raw: inputs.RawAnnotation) -> ownership.OwnedAtom:
    return ownership.OwnedAtom(
        raw=raw,
        initial_split=domain.Split.TRAIN,
        final_split=domain.Split.TRAIN,
        source_group_id="group-train",
        authoritative_offset_seconds=decimal.Decimal("1.25"),
        authoritative_duration_seconds=decimal.Decimal("1.5"),
        authoritative_text="Corrected target",
        canonical_manifest_role="closure",
        canonical_row_index=9,
        target_origin="closure",
        closure_row_index=4,
    )


def _speech_owner(
    atom_id: str,
    source_uri: str,
    split: domain.Split,
    family: str = "bcfy_feeds",
) -> stage.StagedOwnedSpeech:
    atom = domain.Atom(
        atom_id=atom_id,
        family=family,
        source_uri=source_uri,
        raw_manifest="raw",
        raw_index=int(atom_id.rsplit("-", 1)[-1]),
        start_frame=100,
        end_frame=200,
        atom_class=domain.AtomClass.SPEECH,
        text=f"text {atom_id}",
    )
    return stage.StagedOwnedSpeech(
        atom=atom,
        split=split,
        text=f"text {atom_id}",
        corrected=False,
        authority_rank=1,
        provenance={"target_origin": "canonical"},
    )


def _request(
    request_id: str,
    owner: stage.StagedOwnedSpeech,
) -> domain.Request:
    return domain.Request(
        request_id=request_id,
        family=owner.atom.family,
        split=owner.split,
        source_uri=owner.atom.source_uri,
        start_frame=50,
        end_frame=250,
        sample_rate=100,
        owner_ids=(owner.atom.atom_id,),
        text=owner.text,
    )


def _slice_receipt(
    output_path: pathlib.Path,
    request: domain.Request,
    *,
    generation: int = 1,
    digest: str = _SHA_A,
) -> media.SliceReceipt:
    is_feed = request.family == "bcfy_feeds"
    encoder_command = (
        (
            "python-soundfile.SoundFile",
            "mode=w",
            f"samplerate={request.sample_rate}",
            "channels=1",
            "format=FLAC",
            "subtype=PCM_16",
            "{output}",
        )
        if is_feed
        else (
            "{ffmpeg}",
            "-y",
            "-f",
            "s16le",
            "-ar",
            str(request.sample_rate),
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
    )
    return media.SliceReceipt(
        output_path=output_path,
        output_format="FLAC",
        output_codec="flac",
        output_encoded_size_bytes=123,
        output_encoded_sha256=_SHA_B,
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
            media.PINNED_SOUNDFILE_PYTHON_ARTIFACT_SHA256 if is_feed else None
        ),
        encoder_cffi_artifact_sha256=(
            media.PINNED_SOUNDFILE_CFFI_ARTIFACT_SHA256 if is_feed else None
        ),
        encoder_embedded_libflac_version="1.4.3" if is_feed else None,
        encoder_command_sha256=media._command_sha256(encoder_command),
        encoder_contract_sha256=media.ENCODER_CONTRACT_SHA256,
        encoder_input_pcm_format="s16le",
        encoder_production_image_uri=(
            None if is_feed else "registry/normalization@sha256:" + _SHA_A
        ),
        encoder_production_revision=(
            None if is_feed else "normalization-test-revision"
        ),
        source_uri=request.source_uri,
        source_generation=generation,
        source_encoded_sha256=_SHA_A,
        start_frame=request.start_frame,
        end_frame=request.end_frame,
        frame_count=request.end_frame - request.start_frame,
        duration_seconds="2",
        sample_rate=request.sample_rate,
        channels=1,
        subtype="PCM_16",
        source_pcm_sha256=digest,
        output_decoded_pcm_sha256=digest,
    )


def _write_guarded_continuous_plan(
    root: pathlib.Path,
    *,
    split: domain.Split,
    source: continuous.ContinuousSourceInput,
    guards: dict[str, str],
    histogram: dict[int, int],
) -> tuple[pathlib.Path, continuous.ContinuousSplitPlan]:
    directory = root / split.value
    directory.mkdir(parents=True)
    plan = continuous.plan_continuous_split(
        sources=(source,),
        production_histogram_ms=histogram,
    )
    input_sha256 = continuous.continuous_source_inputs_sha256(
        (source,),
        split=split,
    )
    guards["continuous_source_inputs_sha256"] = input_sha256
    split_receipt = {
        **plan.receipt,
        "continuous_source_inputs_sha256": input_sha256,
        "frozen_input_guards": guards,
        "planning_only_smoke_metrics": {
            "request_count": len(plan.requests),
            "source_count": len(plan.source_plans),
            "speech_owner_count": len(source.speeches),
        },
    }
    request_rows = []
    for request in plan.requests:
        duration = fractions.Fraction(
            (request.end_frame - request.start_frame) * 1_000,
            request.sample_rate,
        )
        request_rows.append(
            {
                **dataclasses.asdict(request),
                "split": request.split.value,
                "owner_ids": list(request.owner_ids),
                "duration_frames": request.end_frame - request.start_frame,
                "duration_ms_exact": {
                    "numerator": duration.numerator,
                    "denominator": duration.denominator,
                },
            }
        )
    payloads = {
        "requests.jsonl": b"".join(
            artifacts.canonical_json_bytes(row) for row in request_rows
        ),
        "source-receipts.jsonl": b"".join(
            artifacts.canonical_json_bytes(source_plan.receipt)
            for source_plan in plan.source_plans
        ),
        "split-receipt.json": artifacts.canonical_json_bytes(split_receipt),
    }
    for name, payload in payloads.items():
        (directory / name).write_bytes(payload)
    success = {
        "schema_version": 1,
        "kind": "continuous_planning_only_success",
        "algorithm": "continuous_bcfy_feeds_global_split_ecdf_v1",
        "split": split.value,
        "artifacts": {
            name: hashlib.sha256(payload).hexdigest()
            for name, payload in payloads.items()
        },
        "frozen_input_guards": guards,
        "metrics": {
            "source_count": 1,
            "speech_owner_count": 1,
            "request_count": 1,
        },
    }
    (directory / "_SUCCESS.json").write_bytes(
        artifacts.canonical_json_bytes(success)
    )
    return directory, plan


def test_frame_universe_uses_authoritative_speech_and_preserves_authority(
    tmp_path: pathlib.Path,
) -> None:
    speech = _raw(
        raw_index=0,
        atom_class=domain.AtomClass.SPEECH,
        text="raw target",
    )
    pii = _raw(
        raw_index=1,
        atom_class=domain.AtomClass.PII,
        text=None,
    )
    targetless = _raw(
        raw_index=2,
        atom_class=domain.AtomClass.TARGETLESS,
        text=None,
    )
    owner = _owned(speech)
    bundle = SimpleNamespace(
        source_uris=(speech.source_uri,),
        raw_annotations=(speech, pii, targetless),
        speech_annotations=(speech,),
    )
    owner_result = SimpleNamespace(
        owners=(owner,),
        owner_by_locator=lambda: {speech.locator: owner},
    )

    result = stage.build_frame_universe(
        bundle,
        owner_result,
        (_source(speech.source_uri, tmp_path / "source.wav"),),
    )

    staged = result.owned_speech[0]
    assert (staged.atom.start_frame, staged.atom.end_frame) == (125, 275)
    assert staged.text == "Corrected target"
    assert staged.corrected is True
    assert staged.authority_rank == 4
    assert staged.provenance["target_origin"] == "closure"
    assert result.pii_atoms[0].start_frame == 100
    assert len(result.atoms) == 3


def test_continuous_planning_is_independent_per_split(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    pre_owner = _speech_owner(
        "owner-0",
        "gs://audio/call.wav",
        domain.Split.TRAIN,
        family="bcfy_calls",
    )
    train_owner = _speech_owner(
        "owner-1",
        "gs://audio/train-feed.wav",
        domain.Split.TRAIN,
    )
    eval_owner = _speech_owner(
        "owner-2",
        "gs://audio/eval-feed.wav",
        domain.Split.EVAL,
    )
    sources = tuple(
        _source(owner.atom.source_uri, tmp_path / f"{index}.wav")
        for index, owner in enumerate(
            (pre_owner, train_owner, eval_owner), start=1
        )
    )
    pre_request = _request("pre", pre_owner)
    train_request = _request("train", train_owner)
    eval_request = _request("eval", eval_owner)
    pre_result = presegmented.PresegmentedResult(
        requests=(pre_request,),
        receipt=SimpleNamespace(),
        transcript_receipts=(),
    )
    monkeypatch.setattr(
        stage.presegmented,
        "reconstruct_presegmented",
        lambda **_kwargs: pre_result,
    )
    observed_splits: list[domain.Split] = []

    def fake_plan(
        *,
        sources: tuple[continuous.ContinuousSourceInput, ...],
        production_histogram_ms: object,
    ) -> continuous.ContinuousSplitPlan:
        assert production_histogram_ms == {1_000: 1}
        split = sources[0].speeches[0].split
        observed_splits.append(split)
        request = train_request if split is domain.Split.TRAIN else eval_request
        return continuous.ContinuousSplitPlan(
            requests=(request,),
            source_plans=(),
            receipt={"split": split.value},
        )

    monkeypatch.setattr(stage.continuous, "plan_continuous_split", fake_plan)
    universe = stage.FrameUniverse(
        sources=sources,
        source_media={
            source.uri: stage._domain_source(source) for source in sources
        },
        atoms=tuple(
            owner.atom for owner in (pre_owner, train_owner, eval_owner)
        ),
        owned_speech=(pre_owner, train_owner, eval_owner),
        ownership=SimpleNamespace(),
    )

    result = stage.plan_final_requests(universe, {1_000: 1})

    assert observed_splits == [domain.Split.TRAIN, domain.Split.EVAL]
    assert {request.request_id for request in result.requests} == {
        "pre",
        "train",
        "eval",
    }

    def unexpected_plan(**_kwargs: object) -> continuous.ContinuousSplitPlan:
        pytest.fail("persisted-plan reuse must not invoke the optimizer")

    monkeypatch.setattr(
        stage.continuous,
        "plan_continuous_split",
        unexpected_plan,
    )
    reused = stage.plan_final_requests(
        universe,
        {1_000: 1},
        reused_continuous_plans={
            domain.Split.TRAIN: continuous.ContinuousSplitPlan(
                requests=(train_request,),
                source_plans=(),
                receipt={"split": "train"},
            ),
            domain.Split.EVAL: continuous.ContinuousSplitPlan(
                requests=(eval_request,),
                source_plans=(),
                receipt={"split": "eval"},
            ),
        },
    )
    assert reused.continuous_train.requests == (train_request,)
    assert reused.continuous_eval.requests == (eval_request,)


def test_guarded_continuous_plan_reconstructs_requests_and_source_plan(
    tmp_path: pathlib.Path,
) -> None:
    owner = _speech_owner(
        "owner-1",
        "gs://audio/train-feed.wav",
        domain.Split.TRAIN,
    )
    source = _source(owner.atom.source_uri, tmp_path / "train.wav")
    source_input = continuous.ContinuousSourceInput(
        media=stage._domain_source(source),
        speeches=(owner,),
    )
    guards = {
        key: hashlib.sha256(key.encode()).hexdigest()
        for key in stage._CONTINUOUS_GUARD_KEYS
    }
    histogram = {1_000: 1}
    directory, persisted = _write_guarded_continuous_plan(
        tmp_path / "plans",
        split=domain.Split.TRAIN,
        source=source_input,
        guards=guards,
        histogram=histogram,
    )

    result = stage.load_guarded_continuous_plan(
        directory,
        split=domain.Split.TRAIN,
        sources=(source_input,),
        expected_guards=guards,
        production_histogram_ms=histogram,
    )

    assert result.requests == persisted.requests
    assert result.source_plans[0].requests == persisted.requests
    assert result.source_plans[0].receipt["source"]["uri"] == source.uri


def test_guarded_continuous_plan_rejects_tamper_and_stale_guard(
    tmp_path: pathlib.Path,
) -> None:
    owner = _speech_owner(
        "owner-1",
        "gs://audio/train-feed.wav",
        domain.Split.TRAIN,
    )
    source_input = continuous.ContinuousSourceInput(
        media=stage._domain_source(
            _source(owner.atom.source_uri, tmp_path / "train.wav")
        ),
        speeches=(owner,),
    )
    guards = {
        key: hashlib.sha256(key.encode()).hexdigest()
        for key in stage._CONTINUOUS_GUARD_KEYS
    }
    histogram = {1_000: 1}
    directory, _persisted = _write_guarded_continuous_plan(
        tmp_path / "plans",
        split=domain.Split.TRAIN,
        source=source_input,
        guards=guards,
        histogram=histogram,
    )
    stale_guards = {
        **guards,
        "continuous_module_sha256": _SHA_B,
    }
    with pytest.raises(stage.StagingError, match="guards drifted"):
        stage.load_guarded_continuous_plan(
            directory,
            split=domain.Split.TRAIN,
            sources=(source_input,),
            expected_guards=stale_guards,
            production_histogram_ms=histogram,
        )

    with (directory / "requests.jsonl").open("ab") as output:
        output.write(b" ")
    with pytest.raises(stage.StagingError, match=r"requests\.jsonl"):
        stage.load_guarded_continuous_plan(
            directory,
            split=domain.Split.TRAIN,
            sources=(source_input,),
            expected_guards=guards,
            production_histogram_ms=histogram,
        )


def test_guarded_continuous_plan_rejects_self_hashed_suboptimal_request(
    tmp_path: pathlib.Path,
) -> None:
    owner = _speech_owner(
        "owner-1",
        "gs://audio/train-feed.wav",
        domain.Split.TRAIN,
    )
    source_input = continuous.ContinuousSourceInput(
        media=stage._domain_source(
            _source(owner.atom.source_uri, tmp_path / "train.wav")
        ),
        speeches=(owner,),
    )
    guards = {
        key: hashlib.sha256(key.encode()).hexdigest()
        for key in stage._CONTINUOUS_GUARD_KEYS
    }
    histogram = {1_000: 1}
    directory, _persisted = _write_guarded_continuous_plan(
        tmp_path / "plans",
        split=domain.Split.TRAIN,
        source=source_input,
        guards=guards,
        histogram=histogram,
    )

    request_row = json.loads(
        (directory / "requests.jsonl").read_text(encoding="utf-8")
    )
    request_row.update(
        {
            "duration_frames": 200,
            "duration_ms_exact": {"denominator": 1, "numerator": 2_000},
            "end_frame": 250,
            "start_frame": 50,
        }
    )
    request_identity = {
        "algorithm": "continuous_bcfy_feeds_exact_frame_v1",
        "source_uri": source_input.media.uri,
        "generation": source_input.media.generation,
        "split": "train",
        "ordinal": 0,
        "start_frame": 50,
        "end_frame": 250,
        "owner_ids": request_row["owner_ids"],
    }
    request_row["request_id"] = (
        f"bcfy-feeds-{domain.stable_digest(request_identity)[:24]}"
    )
    request_payload = artifacts.canonical_json_bytes(request_row)
    (directory / "requests.jsonl").write_bytes(request_payload)
    success = json.loads(
        (directory / "_SUCCESS.json").read_text(encoding="utf-8")
    )
    success["artifacts"]["requests.jsonl"] = hashlib.sha256(
        request_payload
    ).hexdigest()
    (directory / "_SUCCESS.json").write_bytes(
        artifacts.canonical_json_bytes(success)
    )

    with pytest.raises(
        stage.StagingError,
        match="independent receipt binding",
    ):
        stage.load_guarded_continuous_plan(
            directory,
            split=domain.Split.TRAIN,
            sources=(source_input,),
            expected_guards=guards,
            production_histogram_ms=histogram,
        )


def test_materialization_decodes_only_final_request_slices(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    owner = _speech_owner(
        "owner-1",
        "gs://audio/final.wav",
        domain.Split.TRAIN,
    )
    request = _request("final-request", owner)
    sources = (
        _source(owner.atom.source_uri, tmp_path / "final.wav"),
        _source(
            "gs://audio/not-selected.wav",
            tmp_path / "not-selected.wav",
            generation=2,
        ),
    )
    calls: list[
        tuple[
            str,
            int,
            int,
            pathlib.Path,
            media.EncoderProfile,
            media.EncoderToolchain,
        ]
    ] = []
    toolchain = _toolchain(tmp_path)

    def fake_materialize(
        source: media.SourceInventory,
        start: int,
        end: int,
        output: pathlib.Path,
        *,
        encoder_profile: media.EncoderProfile,
        toolchain: media.EncoderToolchain,
    ) -> media.SliceReceipt:
        calls.append(
            (source.uri, start, end, output, encoder_profile, toolchain)
        )
        return _slice_receipt(output, request)

    monkeypatch.setattr(
        stage.media,
        "materialize_slice",
        fake_materialize,
    )

    bindings = stage.materialize_final_requests(
        (request,),
        sources,
        tmp_path / "build",
        toolchain=toolchain,
    )

    assert len(bindings) == 1
    assert calls == [
        (
            "gs://audio/final.wav",
            50,
            250,
            tmp_path / "build/audio/train/bcfy_feeds/final-request.flac",
            media.EncoderProfile.BCFY_FEEDS_SOUNDFILE,
            toolchain,
        )
    ]


def test_materialization_routes_all_families_to_frozen_profiles(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    families = (
        "bcfy_feeds",
        "bcfy_calls",
        "echo",
        "fire_notifications",
    )
    requests: list[domain.Request] = []
    sources: list[media.SourceInventory] = []
    observed: dict[str, media.EncoderProfile] = {}
    for index, family in enumerate(families, start=1):
        owner = _speech_owner(
            f"owner-{index}",
            f"gs://audio/{family}.wav",
            domain.Split.TRAIN,
            family,
        )
        requests.append(_request(f"request-{index}", owner))
        sources.append(
            _source(owner.atom.source_uri, tmp_path / f"{family}.wav")
        )

    def fake_materialize(
        source: media.SourceInventory,
        start: int,
        end: int,
        output: pathlib.Path,
        *,
        encoder_profile: media.EncoderProfile,
        toolchain: media.EncoderToolchain,
    ) -> media.SliceReceipt:
        del source, start, end, toolchain
        request = next(
            item for item in requests if output.stem == item.request_id
        )
        observed[request.family] = encoder_profile
        return _slice_receipt(output, request)

    monkeypatch.setattr(stage.media, "materialize_slice", fake_materialize)
    stage.materialize_final_requests(
        requests,
        sources,
        tmp_path / "build",
        toolchain=_toolchain(tmp_path),
        max_workers=4,
    )

    assert observed == {
        "bcfy_feeds": media.EncoderProfile.BCFY_FEEDS_SOUNDFILE,
        "bcfy_calls": media.EncoderProfile.PRESEGMENTED_FFMPEG,
        "echo": media.EncoderProfile.PRESEGMENTED_FFMPEG,
        "fire_notifications": media.EncoderProfile.PRESEGMENTED_FFMPEG,
    }


def test_completion_marker_uses_receipts_without_rehashing_audio_or_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    output_root = tmp_path / "build"
    materialization = artifacts.write_bytes_create_only(
        output_root / "receipts/materialization.jsonl",
        b'{"request_id":"one"}\n',
    )
    report_receipt = artifacts.write_bytes_create_only(
        output_root / "verification/report.json",
        b'{"owner_count":2}\n',
    )

    def unexpected_inspection(_path: object) -> object:
        pytest.fail("completion must trust existing artifact receipts")

    monkeypatch.setattr(artifacts, "inspect_file", unexpected_inspection)
    report = verify.VerificationReport(
        source_count=1,
        owner_count=2,
        raw_atom_count=3,
        train_request_count=1,
        eval_request_count=1,
        masked_row_count=4,
    )
    marker = stage.write_completion_marker(
        output_root,
        verification_report=report,
        request_count=2,
        audio_count=2,
        artifact_receipts=(materialization, report_receipt),
    )

    payload = json.loads(marker.path.read_bytes())
    assert payload["kind"] == "local_reconstruction_success"
    assert payload["request_count"] == payload["audio_count"] == 2
    assert payload["materialization_receipt_artifact_sha256"] == (
        materialization.sha256
    )
    assert payload["verification"] == report.as_dict()
    assert [item["path"] for item in payload["artifacts"]] == [
        "receipts/materialization.jsonl",
        "verification/report.json",
    ]


def test_stage_refuses_existing_success_before_reading_inputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    output_root = tmp_path / "build"
    output_root.mkdir()
    (output_root / "_SUCCESS.json").write_text("{}\n", encoding="utf-8")
    paths = stage.StagePaths(
        input_paths=SimpleNamespace(),
        source_inventory_receipt=tmp_path / "source-inventory.jsonl",
        production_duration_target=tmp_path / "duration.json",
        construction_contract=tmp_path / "contract.json",
        encoder_toolchain_receipt=tmp_path / "encoder.json",
        ffmpeg_binary=tmp_path / "ffmpeg",
        ownership_receipt=tmp_path / "ownership.json",
        masked_paths_by_family={},
        masked_retained_proof=tmp_path / "masked.jsonl",
        masked_filter_report=tmp_path / "masked-report.json",
        output_root=output_root,
        repo_root=tmp_path,
        proposed_gcs_prefix="gs://bucket/proposed/",
    )

    def unexpected_load(_paths: object) -> object:
        pytest.fail("completed output must fail before reading inputs")

    monkeypatch.setattr(stage.inputs, "load_inputs", unexpected_load)
    with pytest.raises(stage.StagingError, match="already complete"):
        stage.stage_local_dataset(
            paths,
            masked_group_index=SimpleNamespace(),
        )


def test_cli_stage_routes_explicit_local_receipts_without_publication(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_config = SimpleNamespace(
        workspace=tmp_path / "configured-output",
        proposed_gcs_prefix="gs://bucket/proposed/",
    )
    monkeypatch.setattr(
        cli.config,
        "load_run_config",
        lambda _path: run_config,
    )
    index = object()
    monkeypatch.setattr(cli, "_load_source_index", lambda _path: index)
    observed: dict[str, object] = {}

    def fake_stage(
        paths: stage.StagePaths,
        *,
        masked_group_index: object,
        max_materialization_workers: int,
    ) -> object:
        observed.update(
            {
                "index": masked_group_index,
                "paths": paths,
                "workers": max_materialization_workers,
            }
        )
        return SimpleNamespace(
            artifact_receipts=(),
            completion_marker=artifacts.ArtifactReceipt(
                path=tmp_path / "explicit-output/_SUCCESS.json",
                size=1,
                sha256=_SHA_A,
                created=True,
            ),
            verification_report=SimpleNamespace(
                as_dict=lambda: {"owner_count": 2}
            ),
        )

    monkeypatch.setattr(cli.stage, "stage_local_dataset", fake_stage)
    output = tmp_path / "explicit-output"

    status = cli.main(
        [
            "stage",
            "--source-inventory-receipt",
            str(tmp_path / "source-inventory.jsonl"),
            "--ownership-receipt",
            str(tmp_path / "ownership.json"),
            "--continuous-plan-root",
            str(tmp_path / "continuous-plans"),
            "--recording-groups-index",
            str(tmp_path / "index.json"),
            "--masked-retained-proof",
            str(tmp_path / "masked-proof.jsonl"),
            "--masked-filter-report",
            str(tmp_path / "masked-report.json"),
            "--input-root",
            str(tmp_path / "inputs"),
            "--output-root",
            str(output),
            "--workers",
            "3",
            "--ffmpeg-binary",
            str(tmp_path / "pinned-ffmpeg"),
        ]
    )

    assert status == 0
    assert observed["index"] is index
    assert observed["workers"] == 3
    paths = observed["paths"]
    assert isinstance(paths, stage.StagePaths)
    assert paths.output_root == output
    assert paths.source_inventory_receipt == (
        tmp_path / "source-inventory.jsonl"
    )
    assert paths.ownership_receipt == tmp_path / "ownership.json"
    assert paths.continuous_plan_root == tmp_path / "continuous-plans"
    assert paths.encoder_toolchain_receipt == (
        RUN_ROOT / "frozen/encoder_toolchain.json"
    )
    assert paths.ffmpeg_binary == tmp_path / "pinned-ffmpeg"
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["status"] == "COMPLETE"
    assert rendered["verification"] == {"owner_count": 2}
    assert rendered["completion_marker_sha256"] == _SHA_A


@dataclasses.dataclass
class _Resolver:
    groups: dict[tuple[str, str, str | None], str]
    sha256: str

    @property
    def policy_receipt(self) -> dict[str, object]:
        return {"policy": "test"}

    def group_ids_for_binding(
        self,
        binding: object,
    ) -> tuple[str, ...]:
        locator = binding.locators[0]
        key = (locator.kind, locator.value, locator.dataset_family)
        return (self.groups[key],)


def _masked_row(family: str, source_uri: str) -> dict[str, object]:
    return {
        "audio_filepath": source_uri,
        "dataset_family": family,
        "example_id": f"{family}-example",
        "segment_id": "000",
        "split": "eval",
        "text": "reference",
    }


def test_masked_rows_remain_raw_while_frozen_selection_is_proven(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    families = eval_views.MASKED_DATASET_FAMILIES
    masked = {
        family: [_masked_row(family, f"gs://audio/{family}.wav")]
        for family in families
    }
    train_row = {
        "audio_filepath": "gs://derived/train.flac",
        "context_episode_id": "gs://audio/train.wav",
        "dataset": {"family": "bcfy_feeds", "name": "bcfy_feeds"},
        "example_id": "train",
        "request_id": "train",
        "segment_id": "train",
        "source_audio": {
            "audio_filepath": "gs://audio/train.wav",
            "offset": 0.0,
            "duration": 1.0,
            "end_frame": 100,
            "sample_rate": 100,
            "start_frame": 0,
        },
        "source_lineage": {"context_episode_id": "gs://audio/train.wav"},
        "split": "train",
        "text": "train",
    }
    eval_row = {
        **train_row,
        "audio_filepath": "gs://derived/eval.flac",
        "context_episode_id": "gs://audio/eval.wav",
        "example_id": "eval",
        "request_id": "eval",
        "segment_id": "eval",
        "source_audio": {
            "audio_filepath": "gs://audio/eval.wav",
            "offset": 0.0,
            "duration": 1.0,
            "end_frame": 100,
            "sample_rate": 100,
            "start_frame": 0,
        },
        "source_lineage": {"context_episode_id": "gs://audio/eval.wav"},
        "split": "eval",
        "text": "eval",
    }
    eval_bytes = stage._jsonl_bytes((eval_row,))
    provider_validation = eval_views.prepare_provider_validation(
        (eval_row,),
        train_count=1,
        input_lock_digest=hashlib.sha256(eval_bytes).hexdigest(),
    )
    manifest_artifacts = SimpleNamespace(
        canonical_train_rows=(train_row,),
        canonical_eval_rows=(eval_row,),
        provider_validation_rows=provider_validation.rows,
        provider_validation_receipt=provider_validation.receipt,
        primary_eval_jsonl=eval_bytes,
        canonical_validation_jsonl=provider_validation.jsonl,
    )
    groups = {
        ("source_uri", "gs://audio/train.wav", None): "old-train-group",
        **{
            (
                "masked_example_id",
                f"{family}-example",
                family,
            ): f"old-{family}-group"
            for family in families
        },
    }
    proof_rows = tuple(
        {
            **masked[family][0],
            "dataset": {"family": family, "name": family},
            "source_audio": {
                "audio_filepath": f"gs://audio/{family}",
                "duration": 1.0,
                "offset": 0.0,
            },
        }
        for family in families
    )
    proof = stage._jsonl_bytes(proof_rows)
    proof_sha256 = hashlib.sha256(proof).hexdigest()
    index_sha256 = "c" * 64
    monkeypatch.setattr(stage, "MASKED_RETAINED_COUNT", 4)
    monkeypatch.setattr(stage, "MASKED_RETAINED_SHA256", proof_sha256)
    monkeypatch.setattr(stage, "MASKED_INDEX_SHA256", index_sha256)
    resolver = _Resolver(groups=groups, sha256=index_sha256)
    current_groups = {
        "gs://audio/train.wav": "current-train-group",
        **{
            f"gs://audio/{family}.wav": f"current-{family}-group"
            for family in families
        },
    }

    result = stage.build_evaluation_artifacts(
        manifest_artifacts,
        masked_rows_by_family=masked,
        masked_retained_proof_jsonl=proof,
        masked_filter_report={
            "index_sha256": index_sha256,
            "kept_counts": dict.fromkeys(families, 1),
            "kept_row_count": 4,
            "retained_manifest_sha256": proof_sha256,
        },
        masked_group_index=resolver,
        current_physical_groups=current_groups,
        train_group_ids=frozenset({"current-train-group"}),
    )

    assert result.masked.kept_rows_by_family() == {
        family: (masked[family][0],) for family in families
    }
    assert result.masked_selection_proof_sha256 == proof_sha256
    assert (
        result.masked_retained_payload_sha256
        == hashlib.sha256(
            stage._jsonl_bytes(masked[family][0] for family in families)
        ).hexdigest()
    )
    assert len(result.masked_verifier_rows) == 4
    assert {row["group_id"] for row in result.masked_verifier_rows} == {
        f"current-{family}-group" for family in families
    }
    assert {row["payload_sha256"] for row in result.masked_verifier_rows} == {
        artifacts.canonical_json_sha256(masked[family][0])
        for family in families
    }
    assert all("text" not in row for row in result.masked_verifier_rows)


class _Groups:
    def __init__(self, groups: dict[str, str]) -> None:
        self.groups = groups

    def group_by_source_uri(self) -> dict[str, object]:
        return {
            uri: SimpleNamespace(group_id=group)
            for uri, group in self.groups.items()
        }


def test_verification_bundle_maps_slice_pcm_and_passes_independent_verifier(
    tmp_path: pathlib.Path,
) -> None:
    train_owner = _speech_owner(
        "owner-1",
        "gs://audio/train.wav",
        domain.Split.TRAIN,
    )
    eval_owner = _speech_owner(
        "owner-2",
        "gs://audio/eval.wav",
        domain.Split.EVAL,
    )
    train_request = _request("train-request", train_owner)
    eval_request = _request("eval-request", eval_owner)
    sources = (
        _source(train_owner.atom.source_uri, tmp_path / "train.wav"),
        _source(
            eval_owner.atom.source_uri,
            tmp_path / "eval.wav",
            generation=2,
        ),
    )
    universe = stage.FrameUniverse(
        sources=sources,
        source_media={
            source.uri: stage._domain_source(source) for source in sources
        },
        atoms=(train_owner.atom, eval_owner.atom),
        owned_speech=(train_owner, eval_owner),
        ownership=_Groups(
            {
                train_owner.atom.source_uri: "train-group",
                eval_owner.atom.source_uri: "eval-group",
            }
        ),
    )
    bindings = (
        manifests.MaterializationBinding(
            "train-request",
            _slice_receipt(
                tmp_path / "train.flac",
                train_request,
                digest=_SHA_A,
            ),
        ),
        manifests.MaterializationBinding(
            "eval-request",
            _slice_receipt(
                tmp_path / "eval.flac",
                eval_request,
                generation=2,
                digest=_SHA_B,
            ),
        ),
    )
    primary_row = {
        "audio_filepath": str(tmp_path / "eval.flac"),
        "context_episode_id": "gs://audio/eval.wav",
        "dataset": {"family": "bcfy_feeds", "name": "bcfy_feeds"},
        "example_id": "eval-request",
        "owner_ids": ["owner-2"],
        "request_id": "eval-request",
        "segment_id": "eval-request",
        "source_audio": {
            "audio_filepath": "gs://audio/eval.wav",
            "end_frame": 250,
            "sample_rate": 100,
            "start_frame": 50,
        },
        "source_lineage": {"context_episode_id": "gs://audio/eval.wav"},
        "text": eval_request.text,
    }
    primary = eval_views.prepare_primary_eval((primary_row,))
    provider_validation = eval_views.prepare_provider_validation(
        primary.rows,
        train_count=1,
        input_lock_digest=str(primary.receipt["eval_sha256"]),
    )
    prior_context = eval_views.build_prior_context_schedule(primary.rows)
    masked_families: list[eval_views.MaskedFamilyResult] = []
    masked_projections: list[dict[str, object]] = []
    masked_rows: list[dict[str, object]] = []
    for family in eval_views.MASKED_DATASET_FAMILIES:
        row = _masked_row(family, "gs://audio/eval.wav")
        payload_sha256 = artifacts.canonical_json_sha256(row)
        receipt = {
            "excluded_count": 0,
            "excluded_rows": [],
            "family": family,
            "input_count": 1,
            "kept_count": 1,
            "kept_rows": [
                {
                    "example_id": f"{family}-example",
                    "row_sha256": payload_sha256,
                    "segment_id": "000",
                }
            ],
            "rows_resegmented": False,
            "schema_version": 1,
        }
        masked_families.append(
            eval_views.MaskedFamilyResult(
                family=family,
                kept_rows=(row,),
                excluded_rows=(),
                receipt=receipt,
            )
        )
        masked_rows.append(row)
        masked_projections.append(
            {
                "family": family,
                "group_id": "eval-group",
                "payload_sha256": payload_sha256,
                "row_id": f"{family}:{family}-example:000",
                "source_uri": "gs://audio/eval.wav",
                "split": "eval",
            }
        )
    masked = eval_views.MaskedEvalFilterResult(
        families=tuple(masked_families),
        receipt={
            "eval_lane": "masked_v2",
            "excluded_count": 0,
            "families": [item.receipt for item in masked_families],
            "input_count": 4,
            "kept_count": 4,
            "recording_group_gate": {
                "all_rows_resolved": True,
                "status": "pass",
                "train_eval_intersections": [],
            },
            "recording_group_index": {
                "index_sha256": _SHA_A,
                "policy": {"schema_version": 1},
            },
            "rows_resegmented": False,
            "schema_version": 1,
        },
    )
    scope_receipt = eval_views.final_eval_scope_receipt(
        primary=primary,
        provider_validation=provider_validation,
        prior_context=prior_context,
        masked=masked,
    )
    evaluation = stage.EvaluationArtifacts(
        primary=primary,
        provider_validation=provider_validation,
        prior_context=prior_context,
        masked=masked,
        masked_selection_proof_sha256=hashlib.sha256(
            stage._jsonl_bytes(masked_rows)
        ).hexdigest(),
        masked_retained_payload_sha256=hashlib.sha256(
            stage._jsonl_bytes(masked_rows)
        ).hexdigest(),
        scope_receipt=scope_receipt,
        masked_verifier_rows=tuple(masked_projections),
        masked_physical_groups={"gs://audio/eval.wav": "eval-group"},
    )

    bundle = stage.build_verification_bundle(
        universe=universe,
        requests=(train_request, eval_request),
        materialization_bindings=bindings,
        evaluation=evaluation,
        construction_contract={"schema_version": 1},
    )
    report = verify.verify_artifacts(bundle)

    assert report.owner_count == 2
    receipts = bundle["materialization_receipts"]
    assert receipts[1]["output_pcm_sha256"] == _SHA_B
