from oneoff_gemini31_flash_lite import build_candidate, normalize_public_label


def test_normalize_public_label_groups_digit_words():
    assert (
        normalize_public_label("ONE TWO NINER ALPHA FOXTROT")
        == "129 alpha foxtrot"
    )


def test_build_candidate_flags_target_features():
    candidate = build_candidate(
        row_id="r1",
        split="train",
        source_file="data/train.parquet",
        row_index=0,
        text="ONE TWO NINER ALPHA FOXTROT",
    )

    assert candidate.numeric_any
    assert candidate.numeric_phrase_context
    assert not candidate.number_only
    assert candidate.phonetic_any
    assert candidate.numeric_and_phonetic
    assert not candidate.hard_atc
