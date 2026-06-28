"""Transcription model prompts."""

# TODO: https://linear.app/watchduty/issue/GOO-687/update-prompt-to-allow-for-dynamic-text
# Update the prompt to allow for dynamic text to be specified.
GEMINI_PROMPT = """\
Your primary task is to produce a strict, verbatim transcription of the spoken audio. Your absolute highest priority is to transcribe only what you hear with high acoustic certainty. Do not add, invent, or infer any speech that is not clearly audible. The audio may originate from VHF/UHF radio traffic and can include mic clicks, RF static, radio hum, and potentially unintelligible speech. When the audio is unequivocally confirmed as fire-related dispatch, speakers often use heavy jargon, and specific formatting rules apply.

EXPECTED TERMINOLOGY:
These are terms and unit identifiers commonly used in fire-related dispatch. These terms and formatting rules apply exclusively to audio that is unequivocally confirmed as fire-related dispatch. If these exact terms are clearly heard in the audio, transcribe them as listed. Do not invent or infer the use of these terms if they are not genuinely spoken.
copy, received, affirmative, affirm, proceed, responding, responding to, en-route, on-scene, in the area, available, returning, in service, got a caller, caller advising, in quarters, arrived, go ahead, back at, engine, tanker, brush, brush truck, tender, battalion, squad, ladder, tower, tower-ladder, medic, ambulance, k, branch, chopper, copter, AIQ, AOR, DO, IC, ICP, LAT, RP, SEAT, TAC, VFIRE, VLAT, patrol, rescue, station, personnel, air attack, air tactics, helispot, lead plane, strike team, control, being toned, box alarm, cancel the balance, chaparral, exposure protection, fire attack, fire boss, forward progress stopped, forward rate of spread stopped, heavy timber, left flank, light flashy fuels, rate of spread, right flank, structure defense, structure protection, structures threatened, terrain driven, wind driven, clear, clear and in service, code 1, code 2, code 3, code 4, code 33, medical call, fire alarm, commercial fire alarm, breathing problem, cardiac, heart problem, diabetic shock, mvc, trespass, harassment, 10-4, 10-7, 10-8, 10-9, 10-15, 10-20, 10-22, 10-23, 10-91, 10-97.

CRITICAL RULES:
1. Output the transcript strictly and precisely as spoken in the audio, with no newlines. Do not add, invent, or infer any speech that is not clearly audible.
2. When transcribing numbers, write the digits grouped together (e.g., 100, 6333).
3. If the audio contains a unit identifier, format it as the unit type followed by digits (e.g., Engine 41, Battalion 2). Apply this rule strictly only if the unit identifier is clearly spoken AND the context is unequivocally fire-related dispatch.
4. Transcribe only the duration of speech present. Do not extend the transcription with additional words or phrases that were not spoken, even if contextually plausible.

QUALITY GATE: Your absolute highest priority is to transcribe only what you hear with high acoustic certainty.
    *   If the audio contains clear speech that is not fire-related dispatch, you MUST transcribe it verbatim, exactly as heard, without applying any fire-specific formatting or jargon, and without attempting to interpret it as fire dispatch traffic.
    *   If a portion of audio is obscured, noisy, ambiguous, or contains speech that cannot be confidently identified, you MUST replace that specific portion with [UNINTELLIGIBLE].
    *   Do not attempt to infer, guess, or invent speech to fit any expected context or terminology list.
    *   Do not attempt to phonetically guess ambiguous noise.
    *   If the entire audio segment does not contain any discernible speech, output only [UNINTELLIGIBLE].

TASK:
Transcribe the attached audio. Output strictly the transcript."""
