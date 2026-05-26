"""
Common prompt constants for ASR Speech-LLM evaluations.
Keeps all baseline prompts synchronized across model evaluation notebooks.
"""

COMMON_SYSTEM_PROMPT = (
    "You are a speech-to-text transcriber for emergency radio traffic. "
    "Evaluate audio as noisy fire dispatch traffic. "
    "Transcribe digits grouped together (e.g., 6333) and unit identifiers "
    "exactly as spoken (e.g., Engine 41). Output strictly the verbatim transcript."
)

COMMON_USER_INSTRUCTION = (
    "Transcribe this emergency radio audio verbatim per the rules above."
)

# Gemini production transcription — canonical source shared by the SFT pipeline
# (scripts/sft) and the gemini_transcribe_audio eval notebook. SEEDED BYTE-FOR-BYTE
# from the notebook Cell 5 SYSTEM_PROMPT (stripped); zero-import, parenthesized
# concatenation (no triple-quotes) so the drift guard and TEST-03 isolation hold.
GEMINI_TRANSCRIBE_SYSTEM_PROMPT = (
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

GEMINI_TRANSCRIBE_USER_PROMPT = "Transcribe this emergency radio communication segment verbatim per the rules above."

GEMINI_TRANSCRIBE_KEYWORDS = [
    "copy",
    "received",
    "affirmative",
    "affirm",
    "proceed",
    "responding",
    "responding to",
    "en-route",
    "on-scene",
    "on-scene in the area",
    "available",
    "returning",
    "in service",
    "got a caller",
    "caller advising",
    "in quarters",
    "arrived",
    "go ahead",
    "back at",
    "engine",
    "tanker",
    "brush",
    "brush truck",
    "tender",
    "battalion",
    "squad",
    "ladder",
    "tower",
    "tower-ladder",
    "medic",
    "ambulance",
    "k",
    "branch",
    "chopper",
    "copter",
    "AIQ",
    "AOR",
    "IC",
    "ICP",
    "LAT",
    "RP",
    "SEAT",
    "TAC",
    "VFIRE",
    "VLAT",
    "air attack",
    "air tactics",
    "helispot",
    "lead plane",
    "strike team",
    "control",
    "being toned",
    "box alarm",
    "cancel the balance",
    "chaparral",
    "exposure protection",
    "fire attack",
    "fire boss",
    "forward progress stopped",
    "forward rate of spread stopped",
    "heavy timber",
    "left flank",
    "light flashy fuels",
    "rate of spread",
    "right flank",
    "structure defense",
    "structure protection",
    "structures threatened",
    "terrain driven",
    "wind driven",
    "10-4",
    "10-7",
    "10-8",
    "10-9",
    "10-20",
    "10-22",
    "10-23",
    "10-97",
]
