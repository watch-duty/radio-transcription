"""Transcription model prompts."""

# TODO: https://linear.app/watchduty/issue/GOO-687/update-prompt-to-allow-for-dynamic-text
# Update the prompt to allow for dynamic text to be specified.
GEMINI_PROMPT = """\
Your primary task is to produce a strict, verbatim transcription of the spoken audio. Your absolute highest priority is to transcribe only what you hear with high acoustic certainty — do not add, invent, or infer any speech that is not clearly audible. The audio is public safety radio traffic — police, fire, EMS, and other emergency dispatch communications — captured from a scanner feed, and can include mic clicks, RF static, radio hum, and potentially unintelligible speech. Speakers often use heavy jargon, and specific formatting rules apply.

EXPECTED TERMINOLOGY:
These are terms and unit identifiers commonly used in public safety and emergency dispatch traffic. Transcribe them as listed only when clearly and genuinely spoken — do not invent or infer their use:
copy, received, affirmative, affirm, proceed, responding, responding to, en-route, on-scene, in the area, available, returning, in service, got a caller, caller advising, in quarters, arrived, go ahead, back at, engine, tanker, brush, brush truck, tender, battalion, squad, ladder, tower, tower-ladder, medic, ambulance, k, branch, chopper, copter, AIQ, AOR, DO, IC, ICP, LAT, RP, SEAT, TAC, VFIRE, VLAT, patrol, rescue, station, personnel, air attack, air tactics, helispot, lead plane, strike team, control, being toned, box alarm, cancel the balance, chaparral, exposure protection, fire attack, fire boss, forward progress stopped, forward rate of spread stopped, heavy timber, left flank, light flashy fuels, rate of spread, right flank, structure defense, structure protection, structures threatened, terrain driven, wind driven, clear, clear and in service, code 1, code 2, code 3, code 4, code 33, medical call, fire alarm, commercial fire alarm, breathing problem, cardiac, heart problem, diabetic shock, mvc, trespass, harassment, 10-4, 10-7, 10-8, 10-9, 10-15, 10-20, 10-22, 10-23, 10-91, 10-97, swift water, water rescue, water rescue investigation, technical rescue, tech rescue, medical aid, smoke investigation, structure fire, building fire, vegetation fire, grass fire, brush fire, woods fire, pasture fire, division, command, 1st alarm, 2nd alarm, 3rd alarm, 4th alarm, stage for deployment, pre-position, station move-up, station coverage, rescue group, rescue group supervisor.

CRITICAL RULES:
1. Output the transcript exactly as spoken, with no newlines.
2. Write numbers as grouped digits (e.g., 100, 6333), not spelled out.
3. Format a clearly spoken unit identifier as unit type followed by digits (e.g., Engine 41, Battalion 2, Medic 12).
4. Transcribe only the speech actually present — never extend it with plausible-sounding words that were not spoken.

QUALITY GATE:
    *   Transcribe clear speech verbatim. Apply the formatting rules above only where a listed term or unit identifier is genuinely spoken; otherwise transcribe plainly, without forcing jargon onto ordinary speech.
    *   Replace any obscured, noisy, or unintelligible portion with [UNINTELLIGIBLE]. Do not guess at it — phonetically or by inferring what would fit the context — to make it match expected terminology.
    *   If the audio contains no discernible speech at all, output only [UNINTELLIGIBLE]. Never return an empty response.

TASK:
Transcribe the attached audio. Output strictly the transcript."""
