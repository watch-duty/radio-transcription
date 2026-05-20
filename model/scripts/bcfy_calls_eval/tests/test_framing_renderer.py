"""TDD source-of-truth for the framing renderer.

The renderer is pure — given a context record + variant, return a string.
The two model-wrappers append the rendered string to a base prompt; their
output must be byte-identical for the same descriptor (the model-agnostic
invariant the design hinges on).
"""

from bcfy_calls_eval.framing_renderer import render, for_chirp, for_gemini

SAMPLE = {
    "tgCname": "San Antonio Fire Dept. - Operations",
    "tgDescr": "Alerting (Locution)",
    "tgDisplay": "SAFD Alert",
    "tagDescr": "Fire Dispatch",
    "sName": "Alamo Area Regional Radio System (AARRS)",
    "county": "Bexar",
    "state": "Texas",
}


def test_baseline_returns_empty():
    assert render(SAMPLE, variant="baseline") == ""


def test_moderate_full_record():
    out = render(SAMPLE, variant="moderate")
    assert out == (
        "Fire dispatch traffic for San Antonio Fire Dept. - Operations "
        "in Bexar County, Texas "
        "on the Alamo Area Regional Radio System (AARRS)."
    )


def test_aggressive_full_record():
    out = render(SAMPLE, variant="aggressive")
    assert out == (
        "Fire dispatch traffic for San Antonio Fire Dept. - Operations "
        "(SAFD Alert; Alerting (Locution)) "
        "in Bexar County, Texas "
        "on the Alamo Area Regional Radio System (AARRS)."
    )


def test_missing_record_returns_empty():
    assert render(None, variant="moderate") == ""
    assert render({}, variant="moderate") == ""


def test_missing_county_drops_location_clause():
    rec = {**SAMPLE, "county": None, "state": None}
    out = render(rec, variant="moderate")
    assert "County" not in out
    assert "Texas" not in out
    assert "Fire dispatch traffic for San Antonio Fire Dept. - Operations" in out
    assert "on the Alamo Area Regional Radio System" in out


def test_missing_system_drops_system_clause():
    rec = {**SAMPLE, "sName": None}
    out = render(rec, variant="moderate")
    assert "on the" not in out
    assert "Bexar County, Texas." in out


def test_missing_required_field_returns_empty():
    # If agency (tgCname) or service (tagDescr) is missing, there is no
    # useful framing to emit — degrade to baseline silently.
    assert render({**SAMPLE, "tgCname": None}, variant="moderate") == ""
    assert render({**SAMPLE, "tagDescr": None}, variant="moderate") == ""


def test_for_chirp_appends_to_existing_prompt():
    base = "Existing prompt body."
    out = for_chirp(base, render(SAMPLE, variant="moderate"))
    assert out.startswith("Existing prompt body.")
    assert "Fire dispatch traffic for San Antonio Fire Dept" in out


def test_for_gemini_appends_to_existing_prompt():
    base = "System prompt body."
    out = for_gemini(base, render(SAMPLE, variant="moderate"))
    assert out.startswith("System prompt body.")
    assert "Fire dispatch traffic for San Antonio Fire Dept" in out


def test_empty_descriptor_leaves_base_unchanged():
    base = "Untouched."
    assert for_chirp(base, "") == "Untouched."
    assert for_gemini(base, "") == "Untouched."


def test_model_agnostic_invariant():
    """The same rendered descriptor must produce the same appended text in
    both wrappers — this is the model-agnostic core property the spec
    structures everything around."""
    rendered = render(SAMPLE, variant="aggressive")
    base = "X"
    chirp = for_chirp(base, rendered)
    gemini = for_gemini(base, rendered)
    assert chirp.removeprefix(base) == gemini.removeprefix(base)


def test_unknown_variant_raises():
    import pytest
    with pytest.raises(ValueError):
        render(SAMPLE, variant="ultra-aggressive")
