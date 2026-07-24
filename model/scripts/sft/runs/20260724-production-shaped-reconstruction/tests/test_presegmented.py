"""Tests for exact-frame provider-item reconstruction."""

from __future__ import annotations

import pathlib
import sys

import pytest

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(RUN_ROOT) not in sys.path:
    sys.path.insert(0, str(RUN_ROOT))

from dataset_run import domain, presegmented  # noqa: E402


def _media(
    uri: str,
    *,
    frames: int = 1_000,
    sample_rate: int = 100,
) -> domain.SourceMedia:
    return domain.SourceMedia(
        uri=uri,
        generation=7,
        size_bytes=123,
        md5_hash="md5",
        crc32c="crc",
        sha256="sha",
        sample_rate=sample_rate,
        channels=1,
        subtype="PCM_16",
        frame_count=frames,
    )


def _atom(
    atom_id: str,
    *,
    uri: str,
    start: int,
    end: int,
    atom_class: domain.AtomClass,
    text: str | None = None,
    family: str = "echo",
    raw_index: int = 0,
) -> domain.Atom:
    return domain.Atom(
        atom_id=atom_id,
        family=family,
        source_uri=uri,
        raw_manifest=f"{family}_raw",
        raw_index=raw_index,
        start_frame=start,
        end_frame=end,
        atom_class=atom_class,
        text=text,
    )


def _owner(
    atom: domain.Atom,
    *,
    split: domain.Split = domain.Split.TRAIN,
    text: str | None = None,
) -> domain.OwnedSpeech:
    return domain.OwnedSpeech(
        atom=atom,
        split=split,
        text=text or atom.text or "target",
    )


def test_calls_speech_wins_pii_overlap_and_pii_only_islands_split_item() -> (
    None
):
    uri = "gs://bucket/call.wav"
    first = _atom(
        "speech-a",
        uri=uri,
        start=100,
        end=300,
        atom_class=domain.AtomClass.SPEECH,
        text="Engine seven",
        family="bcfy_calls",
        raw_index=1,
    )
    second = _atom(
        "speech-b",
        uri=uri,
        start=600,
        end=700,
        atom_class=domain.AtomClass.SPEECH,
        text="Respond",
        family="bcfy_calls",
        raw_index=2,
    )
    atoms = (
        first,
        second,
        _atom(
            "pii-overlap",
            uri=uri,
            start=0,
            end=150,
            atom_class=domain.AtomClass.PII,
            family="bcfy_calls",
            raw_index=3,
        ),
        _atom(
            "pii-island",
            uri=uri,
            start=400,
            end=500,
            atom_class=domain.AtomClass.PII,
            family="bcfy_calls",
            raw_index=4,
        ),
        _atom(
            "blank-context",
            uri=uri,
            start=300,
            end=350,
            atom_class=domain.AtomClass.TARGETLESS,
            family="bcfy_calls",
            raw_index=5,
        ),
    )

    result = presegmented.reconstruct_presegmented(
        atoms=atoms,
        owned_speech=(_owner(first), _owner(second)),
        source_media={uri: _media(uri)},
    )

    assert [
        (request.start_frame, request.end_frame, request.owner_ids)
        for request in result.requests
    ] == [
        (100, 400, ("speech-a",)),
        (500, 1_000, ("speech-b",)),
    ]
    receipt = result.receipt.source_receipts[0]
    assert receipt.geometry_policy == (
        presegmented.AUTHORITATIVE_PROVIDER_ITEM_GEOMETRY_POLICY
    )
    assert all(
        request.geometry_policy
        == presegmented.AUTHORITATIVE_PROVIDER_ITEM_GEOMETRY_POLICY
        for request in receipt.request_receipts
    )
    assert receipt.pii_union_frames == 250
    assert receipt.speech_pii_overlap_frames == 50
    assert receipt.pii_only_frames == 200
    assert receipt.emitted_request_frames == 800
    assert receipt.retained_speech_frames == 300
    assert receipt.retained_targetless_annotation_frames == 50
    assert receipt.retained_unannotated_frames == 450


def test_echo_uses_strict_speech_overlap_components_only() -> None:
    uri = "gs://bucket/echo-hour-archive.wav"
    first = _atom(
        "echo-a",
        uri=uri,
        start=100,
        end=300,
        atom_class=domain.AtomClass.SPEECH,
        text="Alpha bravo",
        raw_index=1,
    )
    overlapping = _atom(
        "echo-b",
        uri=uri,
        start=250,
        end=400,
        atom_class=domain.AtomClass.SPEECH,
        text="bravo charlie",
        raw_index=2,
    )
    touching = _atom(
        "echo-c",
        uri=uri,
        start=400,
        end=500,
        atom_class=domain.AtomClass.SPEECH,
        text="Delta",
        raw_index=3,
    )
    after_positive_gap = _atom(
        "echo-d",
        uri=uri,
        start=550,
        end=650,
        atom_class=domain.AtomClass.SPEECH,
        text="Echo",
        raw_index=4,
    )
    atoms = (
        first,
        overlapping,
        touching,
        after_positive_gap,
        _atom(
            "pii-left",
            uri=uri,
            start=0,
            end=150,
            atom_class=domain.AtomClass.PII,
            raw_index=5,
        ),
        _atom(
            "pii-right",
            uri=uri,
            start=600,
            end=700,
            atom_class=domain.AtomClass.PII,
            raw_index=6,
        ),
        _atom(
            "targetless-positive-gap",
            uri=uri,
            start=500,
            end=550,
            atom_class=domain.AtomClass.TARGETLESS,
            raw_index=7,
        ),
    )

    result = presegmented.reconstruct_presegmented(
        atoms=atoms,
        owned_speech=tuple(
            _owner(atom)
            for atom in (first, overlapping, touching, after_positive_gap)
        ),
        source_media={uri: _media(uri)},
    )

    assert [
        (request.start_frame, request.end_frame, request.owner_ids)
        for request in result.requests
    ] == [
        (100, 400, ("echo-a", "echo-b")),
        (400, 500, ("echo-c",)),
        (550, 650, ("echo-d",)),
    ]
    assert [
        owner_id
        for request in result.requests
        for owner_id in request.owner_ids
    ] == ["echo-a", "echo-b", "echo-c", "echo-d"]
    receipt = result.receipt.source_receipts[0]
    assert receipt.geometry_policy == (presegmented.ECHO_GEOMETRY_POLICY)
    assert {
        request.geometry_policy for request in receipt.request_receipts
    } == {presegmented.ECHO_GEOMETRY_POLICY}
    assert receipt.speech_annotation_frames == 550
    assert receipt.speech_union_frames == 500
    assert receipt.speech_duplicate_annotation_frames == 50
    assert receipt.pii_union_frames == 250
    assert receipt.speech_pii_overlap_frames == 100
    assert receipt.pii_only_frames == 150
    assert receipt.emitted_request_frames == 500
    assert receipt.retained_speech_frames == 500
    assert receipt.retained_targetless_annotation_frames == 0
    assert receipt.retained_unannotated_frames == 0
    assert receipt.dropped_context_frames == 350


def test_echo_does_not_absorb_one_frame_unannotated_gap() -> None:
    uri = "gs://bucket/echo-hour-archive.wav"
    first = _atom(
        "first",
        uri=uri,
        start=100,
        end=200,
        atom_class=domain.AtomClass.SPEECH,
        text="Alpha",
    )
    second = _atom(
        "second",
        uri=uri,
        start=201,
        end=300,
        atom_class=domain.AtomClass.SPEECH,
        text="Bravo",
        raw_index=1,
    )

    result = presegmented.reconstruct_presegmented(
        atoms=(first, second),
        owned_speech=(_owner(first), _owner(second)),
        source_media={uri: _media(uri)},
    )

    assert [
        (request.start_frame, request.end_frame) for request in result.requests
    ] == [(100, 200), (201, 300)]
    assert all(request.duration_seconds < 2 for request in result.requests)


@pytest.mark.parametrize("speech_frames", [140, 860, 1_510, 2_300])
def test_short_speech_is_never_filtered_by_duration(
    speech_frames: int,
) -> None:
    uri = "gs://bucket/echo-hour-archive.wav"
    speech = _atom(
        f"short-{speech_frames}",
        uri=uri,
        start=0,
        end=speech_frames,
        atom_class=domain.AtomClass.SPEECH,
        text="Short but valid speech",
    )

    result = presegmented.reconstruct_presegmented(
        atoms=(speech,),
        owned_speech=(_owner(speech),),
        source_media={
            uri: _media(uri, frames=3_000, sample_rate=1_000),
        },
    )

    assert len(result.requests) == 1
    request = result.requests[0]
    assert (request.start_frame, request.end_frame) == (0, speech_frames)
    assert request.owner_ids == (speech.atom_id,)


def test_full_residual_component_is_kept_and_context_only_components_drop() -> (
    None
):
    uri = "gs://bucket/call.wav"
    speech = _atom(
        "speech",
        uri=uri,
        start=300,
        end=400,
        atom_class=domain.AtomClass.SPEECH,
        text="Medic four",
        family="bcfy_calls",
    )
    atoms = (
        speech,
        _atom(
            "left-pii",
            uri=uri,
            start=200,
            end=250,
            atom_class=domain.AtomClass.PII,
            family="bcfy_calls",
            raw_index=1,
        ),
        _atom(
            "right-pii",
            uri=uri,
            start=500,
            end=550,
            atom_class=domain.AtomClass.PII,
            family="bcfy_calls",
            raw_index=2,
        ),
    )

    result = presegmented.reconstruct_presegmented(
        atoms=atoms,
        owned_speech=(_owner(speech),),
        source_media=[_media(uri)],
    )

    assert len(result.requests) == 1
    assert (result.requests[0].start_frame, result.requests[0].end_frame) == (
        250,
        500,
    )
    receipt = result.receipt.source_receipts[0]
    assert receipt.residual_component_count == 3
    assert receipt.emitted_request_count == 1
    assert receipt.dropped_context_component_count == 2
    assert receipt.dropped_context_frames == 650


def test_overlapping_speech_audio_is_counted_once_and_each_owner_once() -> None:
    uri = "gs://bucket/fire.wav"
    first = _atom(
        "first",
        uri=uri,
        start=100,
        end=400,
        atom_class=domain.AtomClass.SPEECH,
        text="Alpha bravo charlie",
        family="fire_notifications",
        raw_index=1,
    )
    second = _atom(
        "second",
        uri=uri,
        start=300,
        end=600,
        atom_class=domain.AtomClass.SPEECH,
        text="bravo charlie delta",
        family="fire_notifications",
        raw_index=2,
    )

    result = presegmented.reconstruct_presegmented(
        atoms=(first, second),
        owned_speech=(_owner(first), _owner(second)),
        source_media={uri: _media(uri)},
    )

    assert len(result.requests) == 1
    assert result.requests[0].owner_ids == ("first", "second")
    assert result.requests[0].text == "Alpha bravo charlie delta"
    assert result.transcript_receipts[0].receipt["text"] == (
        "Alpha bravo charlie delta"
    )
    receipt = result.receipt.source_receipts[0]
    assert receipt.speech_annotation_frames == 600
    assert receipt.speech_union_frames == 500
    assert receipt.speech_duplicate_annotation_frames == 100
    assert receipt.retained_speech_frames == 500


def test_different_provider_items_never_merge() -> None:
    first_uri = "gs://bucket/call-a.wav"
    second_uri = "gs://bucket/call-b.wav"
    first = _atom(
        "a",
        uri=first_uri,
        start=10,
        end=20,
        atom_class=domain.AtomClass.SPEECH,
        text="Alpha",
        family="bcfy_calls",
    )
    second = _atom(
        "b",
        uri=second_uri,
        start=10,
        end=20,
        atom_class=domain.AtomClass.SPEECH,
        text="Bravo",
        family="bcfy_calls",
    )

    result = presegmented.reconstruct_presegmented(
        atoms=(first, second),
        owned_speech=(_owner(first), _owner(second)),
        source_media={
            first_uri: _media(first_uri, frames=100),
            second_uri: _media(second_uri, frames=100),
        },
    )

    assert len(result.requests) == 2
    assert {request.source_uri for request in result.requests} == {
        first_uri,
        second_uri,
    }
    assert all(
        (request.start_frame, request.end_frame) == (0, 100)
        for request in result.requests
    )


def test_missing_speech_owner_fails_immediately() -> None:
    uri = "gs://bucket/echo.wav"
    speech = _atom(
        "speech",
        uri=uri,
        start=10,
        end=20,
        atom_class=domain.AtomClass.SPEECH,
        text="Alpha",
    )

    with pytest.raises(ValueError, match="ownership must be exact"):
        presegmented.reconstruct_presegmented(
            atoms=(speech,),
            owned_speech=(),
            source_media={uri: _media(uri)},
        )


def test_mixed_split_provider_item_is_rejected() -> None:
    uri = "gs://bucket/fire.wav"
    first = _atom(
        "first",
        uri=uri,
        start=10,
        end=20,
        atom_class=domain.AtomClass.SPEECH,
        text="Alpha",
        family="fire_notifications",
    )
    second = _atom(
        "second",
        uri=uri,
        start=30,
        end=40,
        atom_class=domain.AtomClass.SPEECH,
        text="Bravo",
        family="fire_notifications",
        raw_index=1,
    )

    with pytest.raises(ValueError, match="exactly one split owner"):
        presegmented.reconstruct_presegmented(
            atoms=(first, second),
            owned_speech=(
                _owner(first, split=domain.Split.TRAIN),
                _owner(second, split=domain.Split.EVAL),
            ),
            source_media={uri: _media(uri)},
        )


def test_out_of_bounds_atom_fails_instead_of_clipping() -> None:
    uri = "gs://bucket/echo.wav"
    speech = _atom(
        "speech",
        uri=uri,
        start=90,
        end=110,
        atom_class=domain.AtomClass.SPEECH,
        text="Alpha",
    )

    with pytest.raises(ValueError, match="outside decoded source bounds"):
        presegmented.reconstruct_presegmented(
            atoms=(speech,),
            owned_speech=(_owner(speech),),
            source_media={uri: _media(uri, frames=100)},
        )


def test_continuous_feed_family_is_rejected() -> None:
    uri = "gs://bucket/feed.wav"
    speech = _atom(
        "speech",
        uri=uri,
        start=10,
        end=20,
        atom_class=domain.AtomClass.SPEECH,
        text="Alpha",
        family="bcfy_feeds",
    )

    with pytest.raises(ValueError, match="continuous or unsupported"):
        presegmented.reconstruct_presegmented(
            atoms=(speech,),
            owned_speech=(_owner(speech),),
            source_media={uri: _media(uri)},
        )
