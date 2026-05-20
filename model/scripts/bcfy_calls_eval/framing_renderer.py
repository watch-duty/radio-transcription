"""Pure framing renderer for the Broadcastify Calls framing-context A/B.

Variants:
- "baseline"   : returns "" (no framing block; baseline prompt only).
- "moderate"   : agency (tgCname) + service (tagDescr) + system (sName) + location.
- "aggressive" : moderate + alpha tag (tgDisplay) + talkgroup descriptor (tgDescr).

Design rule (spec-critical): the output is **prose**, not a word list. The
descriptor *describes* the feed; it never enumerates vocabulary for the model
to emit. That single rule is what separates this from the experiment that
regressed short-clip WER ~0.8 pts via over-insertion of vocabulary tokens.

Missing fields drop their clause. Missing record -> "" -> baseline behavior.
The two wrappers append the rendered descriptor to a base prompt; their
appended portion is byte-identical (the model-agnostic invariant).
"""

from __future__ import annotations

VARIANTS = ("baseline", "moderate", "aggressive")


def render(record: dict | None, variant: str) -> str:
    """Return the framing descriptor for one talkgroup record.

    Pure: no I/O, no model-specific formatting.
    """
    if variant not in VARIANTS:
        raise ValueError(
            f"unknown variant: {variant!r}; expected one of {VARIANTS}"
        )
    if variant == "baseline" or not record:
        return ""

    agency = (record.get("tgCname") or "").strip()
    service = (record.get("tagDescr") or "").strip().lower()
    if not (agency and service):
        return ""  # nothing useful to frame with — degrade silently

    system = (record.get("sName") or "").strip()
    county = (record.get("county") or "").strip()
    state = (record.get("state") or "").strip()

    lead = f"{service[0].upper()}{service[1:]} traffic for {agency}"

    if variant == "aggressive":
        alpha = (record.get("tgDisplay") or "").strip()
        tg_descr = (record.get("tgDescr") or "").strip()
        annotations = [a for a in (alpha, tg_descr) if a]
        if annotations:
            lead += f" ({'; '.join(annotations)})"

    parts = [lead]
    if county and state:
        parts.append(f"in {county} County, {state}")
    if system:
        parts.append(f"on the {system}")
    return " ".join(parts) + "."


def for_chirp(base_prompt: str, rendered: str) -> str:
    """Append the framing descriptor to Chirp's custom_prompt."""
    if not rendered:
        return base_prompt
    return f"{base_prompt}\n\n{rendered}"


def for_gemini(base_prompt: str, rendered: str) -> str:
    """Append the framing descriptor to Gemini's system prompt.

    Structurally identical to for_chirp — that is the model-agnostic invariant.
    """
    if not rendered:
        return base_prompt
    return f"{base_prompt}\n\n{rendered}"
