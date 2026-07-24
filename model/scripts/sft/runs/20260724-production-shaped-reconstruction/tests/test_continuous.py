"""Tests for outcome-blind continuous BCFY-feeds reconstruction."""

from __future__ import annotations

import copy
import dataclasses
import fractions
import pathlib
import sys

import pytest

RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(RUN_ROOT) not in sys.path:
    sys.path.insert(0, str(RUN_ROOT))

from dataset_run import continuous, domain  # noqa: E402


def _media(*, frames: int = 100_000) -> domain.SourceMedia:
    return domain.SourceMedia(
        uri="gs://bucket/feed.wav",
        generation=7,
        size_bytes=123,
        md5_hash="md5",
        crc32c="crc",
        sha256="sha",
        sample_rate=1_000,
        channels=1,
        subtype="PCM_16",
        frame_count=frames,
    )


def _atom(
    atom_id: str,
    start: int,
    end: int,
    atom_class: domain.AtomClass,
    *,
    text: str | None = None,
) -> domain.Atom:
    return domain.Atom(
        atom_id=atom_id,
        family="bcfy_feeds",
        source_uri="gs://bucket/feed.wav",
        raw_manifest="fixture",
        raw_index=int("".join(filter(str.isdigit, atom_id)) or 0),
        start_frame=start,
        end_frame=end,
        atom_class=atom_class,
        text=text,
    )


def _speech(
    atom_id: str,
    start: int,
    end: int,
    text: str,
) -> domain.OwnedSpeech:
    return domain.OwnedSpeech(
        atom=_atom(
            atom_id,
            start,
            end,
            domain.AtomClass.SPEECH,
            text=text,
        ),
        split=domain.Split.TRAIN,
        text=text,
    )


def _plan(
    speeches: list[domain.OwnedSpeech],
    *,
    pii: list[domain.Atom] | None = None,
    targetless: list[domain.Atom] | None = None,
    histogram: dict[int | str, int] | None = None,
    frames: int = 100_000,
) -> continuous.ContinuousPlan:
    return continuous.plan_continuous_source(
        media=_media(frames=frames),
        speeches=speeches,
        pii_atoms=pii or [],
        targetless_atoms=targetless or [],
        production_histogram_ms=histogram or {2_000: 10, 5_000: 3},
    )


def test_exact_wasserstein_uses_full_ecdf_without_buckets() -> None:
    # Target has half its mass at 0 ms and half at 10 ms.  A single observed
    # request at 5 ms leaves two rectangles of area 2.5 ms.
    distance = continuous.exact_wasserstein_1_ms(
        {0: 1, 10: 1},
        [fractions.Fraction(5)],
    )
    assert distance == fractions.Fraction(5)


def test_rank_wasserstein_and_one_replacement_match_full_ecdf_exactly() -> (
    None
):
    histograms = (
        {0: 1},
        {0: 2, 7: 1, 19: 4},
        {3: 1, 5: 5, 11: 2, 30: 1},
    )
    observed_sets = (
        (fractions.Fraction(-5),),
        (
            fractions.Fraction(3),
            fractions.Fraction(3),
            fractions.Fraction(9, 2),
            fractions.Fraction(40),
        ),
        tuple(
            fractions.Fraction(value, denominator)
            for denominator in (1, 2, 3, 7)
            for value in range(-2, 15, 3)
        ),
    )
    replacements = (
        fractions.Fraction(-20),
        fractions.Fraction(0),
        fractions.Fraction(3),
        fractions.Fraction(9, 2),
        fractions.Fraction(100),
    )
    for raw_histogram in histograms:
        histogram = continuous._Histogram.from_milliseconds(raw_histogram)
        scorer = continuous._RankWassersteinScorer(histogram)
        for observed in observed_sets:
            state = continuous._ObservedWassersteinState.from_values(
                scorer,
                observed,
            )
            assert state.wasserstein_1_ms == histogram.wasserstein_1(
                observed
            )
            assert scorer.wasserstein_1(observed) == histogram.wasserstein_1(
                observed
            )
            for old_index, old_value in enumerate(observed):
                for replacement in replacements:
                    changed = list(observed)
                    changed[old_index] = replacement
                    assert state.replacing(
                        old_value,
                        replacement,
                    ) == histogram.wasserstein_1(changed)


def test_rank_cost_cache_is_deterministically_bounded_and_output_neutral(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(continuous, "_RANK_COST_CACHE_MAX_ENTRIES", 8)
    histogram = continuous._Histogram.from_milliseconds(
        {0: 2, 10: 3, 30: 1}
    )
    scorer = continuous._RankWassersteinScorer(histogram)
    observed = tuple(fractions.Fraction(value, 3) for value in range(10))

    for shift in range(20):
        shifted = tuple(value + shift for value in observed)
        assert scorer.wasserstein_1(shifted) == histogram.wasserstein_1(
            shifted
        )

    assert scorer.peak_rank_cost_cache_entries <= 8
    assert scorer.rank_cost_cache_clear_count > 0


def test_overlap_connected_speech_is_indivisible_and_speech_wins_pii() -> None:
    speeches = [
        _speech("s1", 1_000, 3_000, "engine seven"),
        _speech("s2", 2_500, 4_000, "seven responding"),
        _speech("s3", 6_000, 7_000, "second call"),
    ]
    pii = [
        _atom("p1", 3_500, 5_500, domain.AtomClass.PII),
    ]
    plan = _plan(
        speeches,
        pii=pii,
        histogram={3_000: 1, 1_000: 1},
        frames=8_000,
    )

    requests_by_owner = {
        owner_id: request
        for request in plan.requests
        for owner_id in request.owner_ids
    }
    assert requests_by_owner["s1"] is requests_by_owner["s2"]
    # Speech owns [3_500, 4_000); only the PII-only remainder [4_000, 5_500)
    # is forbidden.
    assert requests_by_owner["s1"].end_frame == 4_000
    assert requests_by_owner["s3"].start_frame >= 5_500
    assert all(
        request.end_frame <= 4_000 or request.start_frame >= 5_500
        for request in plan.requests
    )
    assert plan.receipt["structure"]["effective_pii_only_union"] == [
        {"start_frame": 4_000, "end_frame": 5_500}
    ]


def test_every_owner_exactly_once_and_requests_never_overlap() -> None:
    speeches = [
        _speech("s1", 1_000, 2_000, "alpha"),
        _speech("s2", 2_600, 3_600, "bravo"),
        _speech("s3", 8_000, 9_000, "charlie"),
        _speech("s4", 9_400, 10_400, "delta"),
    ]
    targetless = [
        _atom("n1", 2_000, 2_600, domain.AtomClass.TARGETLESS),
        _atom("n2", 3_600, 8_000, domain.AtomClass.TARGETLESS),
    ]
    plan = _plan(
        speeches,
        targetless=targetless,
        histogram={2_600: 20, 1_000: 10},
        frames=12_000,
    )

    observed = [
        owner_id for request in plan.requests for owner_id in request.owner_ids
    ]
    assert sorted(observed) == ["s1", "s2", "s3", "s4"]
    assert len(observed) == len(set(observed))
    assert all(
        left.end_frame <= right.start_frame
        for left, right in zip(plan.requests, plan.requests[1:], strict=False)
    )
    assert all(request.owner_ids for request in plan.requests)
    assert plan.receipt["hard_invariants"]["every_owner_exactly_once"] is True
    assert {row["name"] for row in plan.receipt["candidates"]} == {
        "atomic",
        "reference_800ms",
        "ecdf_targeted",
    }


def test_indivisible_owner_may_exceed_30_and_60_seconds() -> None:
    plan = _plan(
        [_speech("s1", 1_000, 71_000, "long dispatch")],
        histogram={70_000: 1},
        frames=80_000,
    )

    assert len(plan.requests) == 1
    assert plan.requests[0].end_frame - plan.requests[0].start_frame == 70_000
    assert plan.requests[0].duration_seconds == 70
    selected = plan.receipt["selected_objective"]
    assert selected["descending_positive_30s_overage_frames"] == [40_000]


def test_planner_is_deterministic_under_input_and_histogram_order() -> None:
    speeches = [
        _speech("s1", 500, 1_500, "one"),
        _speech("s2", 2_000, 3_000, "two"),
        _speech("s3", 5_000, 6_500, "three"),
    ]
    pii = [
        _atom("p1", 3_500, 4_000, domain.AtomClass.PII),
        _atom("p2", 3_750, 4_500, domain.AtomClass.PII),
    ]
    targetless = [
        _atom("n1", 1_500, 2_000, domain.AtomClass.TARGETLESS),
        _atom("n2", 3_000, 3_500, domain.AtomClass.TARGETLESS),
    ]
    first = _plan(
        speeches,
        pii=pii,
        targetless=targetless,
        histogram={1_000: 3, 2_500: 2, 5_000: 1},
        frames=8_000,
    )
    second = _plan(
        list(reversed(speeches)),
        pii=list(reversed(pii)),
        targetless=list(reversed(targetless)),
        histogram={"5000": 1, "2500": 2, "1000": 3},
        frames=8_000,
    )

    assert first.requests == second.requests
    assert first.receipt == second.receipt


def test_transcript_wording_and_word_count_cannot_change_boundaries() -> None:
    short = [
        _speech("s1", 500, 1_500, "one"),
        _speech("s2", 2_000, 3_000, "two"),
    ]
    verbose = [
        _speech(
            "s1",
            500,
            1_500,
            "a much longer corrected dispatch target with many words",
        ),
        _speech("s2", 2_000, 3_000, "[UNINTELLIGIBLE]"),
    ]
    short_plan = _plan(short, histogram={1_000: 5, 2_500: 2}, frames=4_000)
    verbose_plan = _plan(
        verbose,
        histogram={1_000: 5, 2_500: 2},
        frames=4_000,
    )

    assert [
        (request.start_frame, request.end_frame, request.owner_ids)
        for request in short_plan.requests
    ] == [
        (request.start_frame, request.end_frame, request.owner_ids)
        for request in verbose_plan.requests
    ]
    assert (
        short_plan.receipt["selected_policy"]
        == verbose_plan.receipt["selected_policy"]
    )
    assert (
        short_plan.receipt["selected_objective"]
        == verbose_plan.receipt["selected_objective"]
    )


def test_no_request_cuts_any_speech_owner() -> None:
    speeches = [
        _speech("s1", 1_000, 35_000, "first long owner"),
        _speech("s2", 36_000, 70_000, "second long owner"),
    ]
    plan = _plan(
        speeches,
        histogram={30_000: 10, 65_000: 1},
        frames=75_000,
    )
    by_id = {owner.atom.atom_id: owner for owner in speeches}
    for request in plan.requests:
        for owner_id in request.owner_ids:
            owner = by_id[owner_id]
            assert request.start_frame <= owner.atom.start_frame
            assert request.end_frame >= owner.atom.end_frame


def test_split_planner_beats_independent_source_choices_on_aggregate_ecdf() -> (
    None
):
    def source(
        uri: str,
        atom_id: str,
    ) -> tuple[domain.SourceMedia, domain.OwnedSpeech]:
        media = domain.SourceMedia(
            uri=uri,
            generation=1,
            size_bytes=1,
            md5_hash="md5",
            crc32c="crc",
            sha256="sha",
            sample_rate=1_000,
            channels=1,
            subtype="PCM_16",
            frame_count=4_000,
        )
        atom = domain.Atom(
            atom_id=atom_id,
            family="bcfy_feeds",
            source_uri=uri,
            raw_manifest="fixture",
            raw_index=0,
            start_frame=1_000,
            end_frame=2_000,
            atom_class=domain.AtomClass.SPEECH,
            text="dispatch",
        )
        return media, domain.OwnedSpeech(
            atom=atom,
            split=domain.Split.TRAIN,
            text="dispatch",
        )

    first_media, first_owner = source("gs://bucket/a.wav", "a")
    second_media, second_owner = source("gs://bucket/b.wav", "b")
    histogram = {1_000: 1, 3_000: 1}
    local_plans = [
        continuous.plan_continuous_source(
            media=media,
            speeches=[owner],
            pii_atoms=[],
            production_histogram_ms=histogram,
        )
        for media, owner in (
            (first_media, first_owner),
            (second_media, second_owner),
        )
    ]
    local_durations = [
        fractions.Fraction(
            (request.end_frame - request.start_frame) * 1_000,
            request.sample_rate,
        )
        for plan in local_plans
        for request in plan.requests
    ]
    independent_distance = continuous.exact_wasserstein_1_ms(
        histogram,
        local_durations,
    )

    split_plan = continuous.plan_continuous_split(
        sources=[
            continuous.ContinuousSourceInput(
                media=second_media,
                speeches=(second_owner,),
            ),
            continuous.ContinuousSourceInput(
                media=first_media,
                speeches=(first_owner,),
            ),
        ],
        production_histogram_ms=histogram,
    )
    parallel_plan = continuous.plan_continuous_split(
        sources=[
            continuous.ContinuousSourceInput(
                media=first_media,
                speeches=(first_owner,),
            ),
            continuous.ContinuousSourceInput(
                media=second_media,
                speeches=(second_owner,),
            ),
        ],
        production_histogram_ms=histogram,
        max_workers=2,
    )
    global_distance_receipt = split_plan.receipt["selected_objective"][
        "aggregate_exact_wasserstein_1_ms"
    ]
    global_distance = fractions.Fraction(
        global_distance_receipt["numerator"],
        global_distance_receipt["denominator"],
    )

    assert global_distance < independent_distance
    assert any(
        selection["global_selection_differs_from_local"]
        for selection in split_plan.receipt["source_selections"]
    )
    assert [request.source_uri for request in split_plan.requests] == [
        "gs://bucket/a.wav",
        "gs://bucket/b.wav",
    ]
    assert global_distance_receipt["decimal"] == "750"
    assert parallel_plan == split_plan


def test_partition_memoization_is_exactly_equivalent_to_exhaustive_scoring() -> (
    None
):
    speeches = tuple(
        [
            _speech("s1", 500, 1_500, "one"),
            _speech("s2", 1_900, 3_000, "two"),
            _speech("s3", 4_200, 5_000, "three"),
            _speech("s4", 5_450, 7_200, "four"),
            _speech("s5", 9_000, 10_000, "five"),
        ]
    )
    media = _media(frames=12_000)
    checked, _ = continuous._validate_inputs(
        media=media,
        speeches=speeches,
        pii_atoms=(),
        targetless_atoms=(),
    )
    safe_islands = ((0, media.frame_count),)
    components = continuous._speech_components(checked, safe_islands)
    island_components = continuous._island_component_indices(components, 1)
    atomic_cuts = frozenset(continuous._all_adjacencies(island_components))
    reference_cuts = continuous._reference_cuts(
        components=components,
        island_component_indices=island_components,
        sample_rate=media.sample_rate,
    )
    histogram = continuous._Histogram.from_milliseconds(
        {
            900: 10,
            1_500: 5,
            2_800: 8,
            4_500: 3,
            8_000: 1,
        }
    )
    memoized, memoized_trials = continuous._targeted_candidate(
        components=components,
        safe_islands=safe_islands,
        island_component_indices=island_components,
        histogram=histogram,
        sample_rate=media.sample_rate,
        reference_cuts=reference_cuts,
        atomic_cuts=atomic_cuts,
        memoize_optimized_partitions=True,
    )
    exhaustive, exhaustive_trials = continuous._targeted_candidate(
        components=components,
        safe_islands=safe_islands,
        island_component_indices=island_components,
        histogram=histogram,
        sample_rate=media.sample_rate,
        reference_cuts=reference_cuts,
        atomic_cuts=atomic_cuts,
        memoize_optimized_partitions=False,
    )
    full_rescore, full_rescore_trials = continuous._targeted_candidate(
        components=components,
        safe_islands=safe_islands,
        island_component_indices=island_components,
        histogram=histogram,
        sample_rate=media.sample_rate,
        reference_cuts=reference_cuts,
        atomic_cuts=atomic_cuts,
        memoize_optimized_partitions=False,
        use_incremental_wasserstein=False,
    )

    assert memoized.spans == exhaustive.spans
    assert memoized.objective == exhaustive.objective
    assert memoized_trials == exhaustive_trials
    assert memoized.spans == full_rescore.spans
    assert memoized.objective == full_rescore.objective
    assert memoized_trials == full_rescore_trials


def test_minimum_w1_duration_ties_keep_full_boundary_tie_breaks() -> None:
    media = _media(frames=4_000)
    speeches = (_speech("s1", 1_000, 2_000, "dispatch"),)
    checked, _ = continuous._validate_inputs(
        media=media,
        speeches=speeches,
        pii_atoms=(),
        targetless_atoms=(),
    )
    safe_islands = ((0, media.frame_count),)
    components = continuous._speech_components(checked, safe_islands)
    island_components = continuous._island_component_indices(components, 1)
    histogram = continuous._Histogram.from_milliseconds({1_500: 1})
    kwargs = {
        "name": "ecdf_targeted",
        "cuts": frozenset(),
        "components": components,
        "safe_islands": safe_islands,
        "island_component_indices": island_components,
        "histogram": histogram,
        "sample_rate": media.sample_rate,
        "reference_cuts": frozenset(),
    }

    incremental = continuous._optimized_candidate_for_cuts(
        **kwargs,
        wasserstein_scorer=continuous._RankWassersteinScorer(histogram),
    )
    full_rescore = continuous._optimized_candidate_for_cuts(
        **kwargs,
        wasserstein_scorer=continuous._RankWassersteinScorer(histogram),
        use_incremental_wasserstein=False,
    )

    assert incremental == full_rescore
    assert incremental.spans[0].start_frame == 1_000
    assert incremental.spans[0].end_frame == 2_500


def test_parallel_enumeration_schedules_largest_components_first_only() -> (
    None
):
    def source(
        uri: str,
        intervals: tuple[tuple[int, int], ...],
    ) -> continuous.ContinuousSourceInput:
        source_media = dataclasses.replace(_media(frames=20_000), uri=uri)
        owners = tuple(
            domain.OwnedSpeech(
                atom=dataclasses.replace(
                    _atom(
                        f"{uri.rsplit('/', 1)[-1]}-{index}",
                        start,
                        end,
                        domain.AtomClass.SPEECH,
                        text="dispatch",
                    ),
                    source_uri=uri,
                ),
                split=domain.Split.TRAIN,
                text="dispatch",
            )
            for index, (start, end) in enumerate(intervals)
        )
        return continuous.ContinuousSourceInput(
            media=source_media,
            speeches=owners,
        )

    ordered = (
        source("gs://bucket/a.wav", ((1_000, 2_000),)),
        source(
            "gs://bucket/b.wav",
            ((1_000, 2_000), (3_000, 4_000), (5_000, 6_000)),
        ),
        source(
            "gs://bucket/c.wav",
            ((1_000, 2_000), (3_000, 4_000)),
        ),
    )

    assert continuous._continuous_source_enumeration_order(ordered) == (
        1,
        2,
        0,
    )
    serial = continuous.plan_continuous_split(
        sources=ordered,
        production_histogram_ms={1_000: 2, 3_000: 1},
    )
    parallel = continuous.plan_continuous_split(
        sources=tuple(reversed(ordered)),
        production_histogram_ms={3_000: 1, 1_000: 2},
        max_workers=2,
    )
    assert parallel == serial


def _persisted_split_fixture() -> tuple[
    continuous.ContinuousSplitPlan,
    tuple[continuous.ContinuousSourceInput, ...],
    dict[int, int],
]:
    source = continuous.ContinuousSourceInput(
        media=_media(frames=8_000),
        speeches=(
            _speech("s1", 500, 1_500, "one"),
            _speech("s2", 2_000, 3_000, "two"),
            _speech("s3", 5_000, 6_500, "three"),
        ),
        pii_atoms=(
            _atom("p1", 3_500, 4_000, domain.AtomClass.PII),
        ),
        targetless_atoms=(
            _atom("n1", 1_500, 2_000, domain.AtomClass.TARGETLESS),
        ),
    )
    sources = (source,)
    histogram = {1_000: 3, 2_500: 2, 5_000: 1}
    plan = continuous.plan_continuous_split(
        sources=sources,
        production_histogram_ms=histogram,
    )
    digest = continuous.continuous_source_inputs_sha256(
        sources,
        split=domain.Split.TRAIN,
    )
    plan = dataclasses.replace(
        plan,
        receipt={
            **plan.receipt,
            "frozen_input_guards": {
                "continuous_source_inputs_sha256": digest,
            },
        },
    )
    return plan, sources, histogram


def test_continuous_source_digest_is_order_independent_and_geometry_sensitive() -> (
    None
):
    plan, sources, _ = _persisted_split_fixture()
    del plan
    source = sources[0]
    reversed_source = dataclasses.replace(
        source,
        speeches=tuple(reversed(source.speeches)),
        pii_atoms=tuple(reversed(source.pii_atoms)),
        targetless_atoms=tuple(reversed(source.targetless_atoms)),
    )
    expected = continuous.continuous_source_inputs_sha256(
        sources,
        split=domain.Split.TRAIN,
    )
    assert (
        continuous.continuous_source_inputs_sha256(
            (reversed_source,),
            split=domain.Split.TRAIN,
        )
        == expected
    )

    reworded = dataclasses.replace(
        source,
        speeches=tuple(
            dataclasses.replace(owner, text=f"changed {owner.text}")
            for owner in source.speeches
        ),
    )
    assert (
        continuous.continuous_source_inputs_sha256(
            (reworded,),
            split=domain.Split.TRAIN,
        )
        == expected
    )

    shifted_pii = dataclasses.replace(
        source,
        pii_atoms=(
            dataclasses.replace(
                source.pii_atoms[0],
                start_frame=source.pii_atoms[0].start_frame + 1,
            ),
        ),
    )
    assert (
        continuous.continuous_source_inputs_sha256(
            (shifted_pii,),
            split=domain.Split.TRAIN,
        )
        != expected
    )
    changed_generation = dataclasses.replace(
        source,
        media=dataclasses.replace(source.media, generation=8),
    )
    assert (
        continuous.continuous_source_inputs_sha256(
            (changed_generation,),
            split=domain.Split.TRAIN,
        )
        != expected
    )


def test_persisted_split_verifier_recomputes_selected_candidate_and_w1() -> (
    None
):
    plan, sources, histogram = _persisted_split_fixture()

    result = continuous.verify_persisted_continuous_split(
        plan,
        sources=sources,
        production_histogram_ms=histogram,
    )

    assert result["verified"] is True
    assert result["request_count"] == len(plan.requests)
    assert result["selected_candidate_count"] == 1


@pytest.mark.parametrize(
    "tamper",
    ("geometry_guard", "candidate_span", "source_objective", "split_w1"),
)
def test_persisted_split_verifier_rejects_tampering(tamper: str) -> None:
    plan, sources, histogram = _persisted_split_fixture()
    receipt = copy.deepcopy(plan.receipt)
    source_plans = list(plan.source_plans)
    if tamper == "geometry_guard":
        receipt["frozen_input_guards"][
            "continuous_source_inputs_sha256"
        ] = "0" * 64
    elif tamper == "split_w1":
        receipt["selected_objective"]["aggregate_exact_wasserstein_1_ms"][
            "numerator"
        ] += 1
    else:
        source_receipt = copy.deepcopy(source_plans[0].receipt)
        selected_name = source_receipt["globally_selected_policy"]
        selected = next(
            row
            for row in source_receipt["candidates"]
            if row["name"] == selected_name
        )
        if tamper == "candidate_span":
            selected["requests"][0]["end_frame"] += 1
        else:
            source_receipt["selected_source_objective"][
                "reference_cut_mismatches"
            ] += 1
        source_plans[0] = dataclasses.replace(
            source_plans[0],
            receipt=source_receipt,
        )
        receipt["source_selections"][0]["source_receipt_sha256"] = (
            domain.stable_digest(source_receipt)
        )
    tampered = dataclasses.replace(
        plan,
        source_plans=tuple(source_plans),
        receipt=receipt,
    )

    with pytest.raises(continuous.ContinuousPlanningError):
        continuous.verify_persisted_continuous_split(
            tampered,
            sources=sources,
            production_histogram_ms=histogram,
        )
