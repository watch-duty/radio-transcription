"""Canonical SFT build prompt — pipeline-local per D-07.

Source of truth: model/colabs/gemini_transcribe_audio.ipynb inline SYSTEM_PROMPT (Cell 5).
NOT in common/prompts.py (engine-only post-PR-#472 revert — CONTEXT.md D-07).
Overridable via --system-prompt / --user-prompt CLI flags in pipeline.py.
The drift-guard test (tests/test_prompt_parity.py) asserts this matches the notebook.

WARNING: Do NOT edit without also updating the notebook AND running test_prompt_parity.py.
"""
from __future__ import annotations

from typing import Final

# SEEDED BYTE-FOR-BYTE from model/colabs/gemini_transcribe_audio.ipynb Cell 5.
# Verified source of truth per D-06 (NOT the autoresearch PRODUCTION_PROMPT — different).
# The value below is the triple-quoted string from the notebook, stripped of
# leading/trailing newlines (the notebook's triple-quoted literal starts with \n
# and ends with \n, so .strip() gives the canonical value).
PIPELINE_SYSTEM_PROMPT: Final = (
    "Evaluate all audio specifically as VHF/UHF fire-related dispatch radio traffic."
    " The audio likely contains mic clicks, RF static, radio hum, and possibly some"
    " unintelligible speech. The speakers use heavy jargon.\n\nEXPECTED TERMINOLOGY:\n"
    "copy, received, affirmative, affirm, proceed, responding, responding to, en-route,"
    " on-scene, on-scene in the area, available, returning, in service, got a caller,"
    " caller advising, in quarters, arrived, go ahead, back at, engine, tanker, brush,"
    " brush truck, tender, battalion, squad, ladder, tower, tower-ladder, medic, ambulance,"
    " k, branch, chopper, copter, AIQ, AOR, IC, ICP, LAT, RP, SEAT, TAC, VFIRE, VLAT,"
    " air attack, air tactics, helispot, lead plane, strike team, control, being toned,"
    " box alarm, cancel the balance, chaparral, exposure protection, fire attack, fire boss,"
    " forward progress stopped, forward rate of spread stopped, heavy timber, left flank,"
    " light flashy fuels, rate of spread, right flank, structure defense, structure"
    " protection, structures threatened, terrain driven, wind driven, 10-4, 10-7, 10-8,"
    " 10-9, 10-20, 10-22, 10-23, 10-97.\n\nCRITICAL RULES:\n"
    "1. Transcribe EVERY spoken word, including conversational phrasing and incomplete"
    " sentences. Only transcribe intelligible speech.\n"
    "2. Output the transcript exactly as said, with no newlines.\n"
    "3. When transcribing numbers, write the digits grouped together (e.g., 100 instead"
    " of one hundred, 6333 instead of 63 33).\n"
    "4. Format all unit identifiers as the unit type followed by digits (e.g., Engine 41,"
    " Battalion 2, Medic 12).\n"
    "5. Do not continue the speech segment beyond what is spoken.\n"
    "6. If the audio is completely unintelligible or contains only static, output"
    " exactly: [UNINTELLIGIBLE]\n\nTASK:\nTranscribe the attached audio. Output strictly"
    " the transcript."
)

# VERIFIED byte-identical to the notebook user instruction and autoresearch USER_PROMPT.
PIPELINE_USER_PROMPT: Final = (
    "Transcribe this emergency radio communication segment verbatim per the rules above."
)
