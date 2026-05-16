"""
Common prompt constants for ASR Speech-LLM evaluations.

Constant groups and their sources:
  COMMON_* — shared eval baseline (used by evaluate_*.ipynb notebooks)
  GEMINI_* — Gemini batch transcription notebook (gemini_transcribe_audio.ipynb)
  CHIRP_*  — Chirp batch transcription notebook (chirp_transcribe_audio.ipynb)
  PRODUCTION / WITHPL / USER — autoresearch SFT (autoresearch-gemini-sft/src/build_e3_sft_jsonl.py)

D-06: Only byte-identical duplicates collapse to a single constant. The four
divergent prompts each appear as their own named constant — none is rewritten,
merged, or improved. Changing any prompt text changes transcription/WER behavior.
"""

# ---------------------------------------------------------------------------
# COMMON_* — shared eval baseline
# Imported by evaluate_ibm_granite.ipynb, evaluate_cohere_transcribe_03_2026.ipynb,
# evaluate_whisper_large_v3.ipynb, evaluate_gemma_3n_e2b_it.ipynb,
# evaluate_gemma_4_E2B_it.ipynb. Names and content must not change.
# ---------------------------------------------------------------------------

COMMON_SYSTEM_PROMPT = (
    "You are a speech-to-text transcriber for emergency radio traffic. "
    "Evaluate audio as noisy fire dispatch traffic. "
    "Transcribe digits grouped together (e.g., 6333) and unit identifiers "
    "exactly as spoken (e.g., Engine 41). Output strictly the verbatim transcript."
)

COMMON_USER_INSTRUCTION = (
    "Transcribe this emergency radio audio verbatim per the rules above."
)

# ---------------------------------------------------------------------------
# GEMINI_* — Gemini batch transcription notebook
# Source: colabs/gemini/gemini_transcribe_audio.ipynb cell 3678b8a6, SYSTEM_PROMPT
# Verbatim copy — including leading and trailing newline.
# ---------------------------------------------------------------------------

GEMINI_SYSTEM_PROMPT = """
Evaluate all audio specifically as VHF/UHF fire-related dispatch radio traffic. The audio likely contains mic clicks, RF static, radio hum, and possibly some unintelligible speech. The speakers use heavy jargon.

EXPECTED TERMINOLOGY:
copy, received, affirmative, affirm, proceed, responding, responding to, en-route, on-scene, on-scene in the area, available, returning, in service, got a caller, caller advising, in quarters, arrived, go ahead, back at, engine, tanker, brush, brush truck, tender, battalion, squad, ladder, tower, tower-ladder, medic, ambulance, k, branch, chopper, copter, AIQ, AOR, IC, ICP, LAT, RP, SEAT, TAC, VFIRE, VLAT, air attack, air tactics, helispot, lead plane, strike team, control, being toned, box alarm, cancel the balance, chaparral, exposure protection, fire attack, fire boss, forward progress stopped, forward rate of spread stopped, heavy timber, left flank, light flashy fuels, rate of spread, right flank, structure defense, structure protection, structures threatened, terrain driven, wind driven, 10-4, 10-7, 10-8, 10-9, 10-20, 10-22, 10-23, 10-97.

CRITICAL RULES:
1. Transcribe EVERY spoken word, including conversational phrasing and incomplete sentences. Only transcribe intelligible speech.
2. Output the transcript exactly as said, with no newlines.
3. When transcribing numbers, write the digits grouped together (e.g., 100 instead of one hundred, 6333 instead of 63 33).
4. Format all unit identifiers as the unit type followed by digits (e.g., Engine 41, Battalion 2, Medic 12).
5. Do not continue the speech segment beyond what is spoken.
6. If the audio is completely unintelligible or contains only static, output exactly: [UNINTELLIGIBLE]

TASK:
Transcribe the attached audio. Output strictly the transcript.
"""

# ---------------------------------------------------------------------------
# CHIRP_* — Chirp batch transcription notebook
# Source: colabs/chirp/chirp_transcribe_audio.ipynb cell e6cf9ea1
# CHIRP_CUSTOM_PROMPT: the base value BEFORE the ENABLE_WORD_TIME_OFFSETS "+= * " append.
# CHIRP_BASE_PHRASE_SET: verbatim BASE_PHRASE_SET list (with section comments).
# CHIRP_TEN_CODES: verbatim TEN_CODES list.
# ---------------------------------------------------------------------------

CHIRP_CUSTOM_PROMPT = """\
Evaluate all audio specifically as VHF/UHF fire-related dispatch radio traffic. The speakers will use heavy jargon, but you must transcribe EVERY spoken word, including conversational phrasing and incomplete sentences.
This audio likely contains mic clicks, RF static, radio hum, and possibly some unintelligible speech. Only transcribe intelligible speech.

CRITICAL RULES:
* If the audio is completely unintelligible, output the following: [UNINTELLIGIBLE]
* Output the transcript exactly as said, with no newlines.
* Do not continue the speech segment beyond what is spoken.
* When transcribing numbers, write the digits grouped together (e.g., 100 instead of one hundred, 6333 instead of 63 33).
* Format all unit identifiers as the unit type followed by digits (e.g., Engine 41, Battalion 2, Medic 12).
"""

CHIRP_BASE_PHRASE_SET: list[str] = [
    # Status & Acknowledgments
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
    # Apparatus & Unit Designators
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
    # Tactical Jargon & Acronyms
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
    # Fire Behavior & Benchmarks
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
]

CHIRP_TEN_CODES: list[str] = ["10-4", "10-7", "10-8", "10-9", "10-20", "10-22", "10-23", "10-97"]

# ---------------------------------------------------------------------------
# PRODUCTION / PHRASE_LIST_BLOCK / WITHPL_PROMPT / USER_PROMPT
# Source: autoresearch-gemini-sft/src/build_e3_sft_jsonl.py lines 29-57, verbatim.
# WITHPL_PROMPT is a derived constant — the derivation expression is preserved exactly.
# ---------------------------------------------------------------------------

PRODUCTION_PROMPT = """ROLE: Literal, mechanical audio-to-text transcription engine.
GOAL: Output exactly and ONLY the words spoken in the audio file.

AUDIO SETTING:
- VHF/UHF emergency radio traffic with heavy static, micro-bursts, and distortion.

STRICT INSTRUCTIONS:
1. ANTI-GUESSING & REJECTION: You are strictly forbidden from guessing. If an audio file is purely static, OR if the speech is too muffled to understand with 100% confidence, do not invent words. You MUST output exactly this token: [UNINTELLIGIBLE]
2. AUDIO LOCALITY: Transcribe ONLY what is audible. Do not continue the speech segment.
3. VERBATIM ONLY: Use digits for numbers (e.g., 10-4, 100, 42).
4. SHORT TRANSMISSIONS: If the transmission is just one or two words (e.g., "copy", "received", "affirm"), output ONLY those words. Do NOT translate meanings.
5. SANITIZATION: No asterisks, markdown, newlines, or roleplay text. Raw text only.

TASK:
Transcribe the audio. Output nothing but the transcript."""

PHRASE_LIST_BLOCK = """
DOMAIN VOCABULARY HINTS (anchor only — do not insert these unless you actually hear them):
status: copy, received, affirmative, affirm, proceed, responding, responding to, en-route, on-scene, available, returning, in service, in quarters, arrived, go ahead, back at
apparatus: engine, tanker, brush, brush truck, tender, battalion, squad, ladder, tower, tower-ladder, medic, ambulance, branch, chopper, copter
tactical: AIQ, AOR, IC, ICP, LAT, RP, SEAT, TAC, VFIRE, VLAT, air attack, air tactics, helispot, lead plane, strike team, control
fire-behavior: being toned, box alarm, cancel the balance, chaparral, exposure protection, fire attack, fire boss, forward progress stopped, heavy timber, left flank, light flashy fuels, rate of spread, right flank, structure defense, structure protection, structures threatened, terrain driven, wind driven
codes: 10-4, 10-7, 10-8, 10-9, 10-20, 10-22, 10-23, 10-97
"""

WITHPL_PROMPT = PRODUCTION_PROMPT.replace("\nTASK:", PHRASE_LIST_BLOCK + "\nTASK:")

USER_PROMPT = "Transcribe this emergency radio communication segment verbatim per the rules above."
