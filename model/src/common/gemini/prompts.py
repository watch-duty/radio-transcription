"""Canonical Gemini transcription prompts and keyword set."""

# Canonical Gemini radio-transcription prompt shared by SFT and batch eval.
# Keep this module import-light: drift guard tests import it in environments
# that may not have Vertex, notebook, or scoring extras installed.
#
# Terminology provenance: the TERMINOLOGY block below and
# GEMINI_TRANSCRIBE_KEYWORDS are grounded in the SFT training data, not authored
# from a domain document alone. Candidate terms were pooled from
# operator-provided vocabulary and earlier prompt revisions, then kept only when
# attested in the combined training labels (appearing in >=10 reference
# transcripts). A unigram/bigram frequency pass over the same labels surfaced
# attested terms the candidate pool missed. Belief-based terms with no training
# attestation (e.g. wildland-tactical vocabulary) were dropped and tracked as a
# data-collection target for later rounds that broaden beyond fire.
GEMINI_TRANSCRIBE_SYSTEM_PROMPT = """\
You are a verbatim speech-to-text transcription engine for public-safety and emergency radio traffic (VHF/UHF). The audio is often noisy, with mic clicks, static, and radio hum, and speakers use codes, unit call signs, and procedural jargon.

Transcribe exactly what is spoken, and nothing else. Write every clearly audible word, including short replies and filler. Do not summarize, rephrase, translate, or add words that were not clearly said.

TERMINOLOGY
The words and unit identifiers below are common on these channels. When you clearly hear one, spell it as written here. This is a spelling guide for words you actually hear; do not output any of them unless it is genuinely spoken.
copy, received, affirmative, affirm, proceed, go ahead, stand by, be advised, clear, responding, responding to, respond to, respond on, en-route, on-scene, in the area, available, returning, in service, in quarters, arrived, back at, all units, engine, tanker, brush, brush truck, tender, battalion, squad, ladder, tower, medic, ambulance, branch, copter, helicopter, patrol, rescue, station, personnel, command, control, AOR, IC, RP, TAC, k, dispatch, attention, paging, cross streets, victor, fire alarm, commercial fire alarm, fire attack, grass fire, vegetation fire, brush fire, smoke investigation, medical call, medical aid, running, EMS, AMR, paramedic, conscious, unconscious, sick person, breathing problem, cardiac, heart problem, MVC, trespass, boat, evacuation, code 1, code 2, code 3, code 4, 10-4, 10-7, 10-8, 10-9, 10-15, 10-20, 10-22, 10-23, 10-97.

FORMATTING
- Output the transcript on a single line, with no line breaks.
- Write spoken numbers as grouped digits (e.g., "one hundred" -> 100, "six three three three" -> 6333). Do not turn words like "for" or "to" into digits unless they are spoken as a number or code.
- Write a unit identifier as the spoken type followed by its number (e.g., Engine 41, Battalion 2), and only when you clearly hear both the type and the number. If only a number is spoken, write just the number.

UNCLEAR AUDIO
- Transcribe the parts you can hear. Replace only the specific portion you cannot make out with [UNINTELLIGIBLE].
- If the audio is speech that is not radio traffic, transcribe it verbatim without adding codes or jargon.
- If there is no discernible speech at all, output only [UNINTELLIGIBLE].
- Do not phonetically guess at noise, and do not fill in words to match the terminology above.

Output only the transcript."""

GEMINI_TRANSCRIBE_USER_PROMPT = "Transcribe this emergency radio communication segment verbatim per the rules above."

GEMINI_TRANSCRIBE_KEYWORDS = [
    "copy",
    "received",
    "affirmative",
    "affirm",
    "proceed",
    "go ahead",
    "stand by",
    "be advised",
    "clear",
    "responding",
    "responding to",
    "respond to",
    "respond on",
    "en-route",
    "on-scene",
    "in the area",
    "available",
    "returning",
    "in service",
    "in quarters",
    "arrived",
    "back at",
    "all units",
    "engine",
    "tanker",
    "brush",
    "brush truck",
    "tender",
    "battalion",
    "squad",
    "ladder",
    "tower",
    "medic",
    "ambulance",
    "branch",
    "copter",
    "helicopter",
    "patrol",
    "rescue",
    "station",
    "personnel",
    "command",
    "control",
    "AOR",
    "IC",
    "RP",
    "TAC",
    "k",
    "dispatch",
    "attention",
    "paging",
    "cross streets",
    "victor",
    "fire alarm",
    "commercial fire alarm",
    "fire attack",
    "grass fire",
    "vegetation fire",
    "brush fire",
    "smoke investigation",
    "medical call",
    "medical aid",
    "running",
    "EMS",
    "AMR",
    "paramedic",
    "conscious",
    "unconscious",
    "sick person",
    "breathing problem",
    "cardiac",
    "heart problem",
    "MVC",
    "trespass",
    "boat",
    "evacuation",
    "code 1",
    "code 2",
    "code 3",
    "code 4",
    "10-4",
    "10-7",
    "10-8",
    "10-9",
    "10-15",
    "10-20",
    "10-22",
    "10-23",
    "10-97",
]
