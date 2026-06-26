# Gemini Prior-Context SFT Analysis

## 2026-06-25: Multi-Audio SFT Rejected

The initial SFT job used the notebook-faithful history schema:

- prior `user(audio)` turns
- prior `model(transcript)` turns
- current `user(prompt + current audio)` turn
- current `model(gold transcript)` target

Vertex accepted the tuning job resource but failed it before training.

Job:
`projects/781667204380/locations/us-central1/tuningJobs/2533762982148571136`

Failure:

```text
Dataset example 2 of 16919 contains 2 audio parts, which exceeds the maximum limit of 1 per example.
```

Conclusion: Gemini SFT cannot directly reproduce the manual-context notebook's
multi-audio history format. The compatible fallback is to keep the same
same-source count-8 history window but encode prior conversations as transcript
text in the current user prompt, with exactly one audio part for the current
clip.

## 2026-06-25: Transcript-List Fallback Cancelled

Fallback job:
`projects/781667204380/locations/us-central1/tuningJobs/6973186314829037568`

This job encoded all prior transcripts as one text block in the current user
turn. It passed the SFT dataset schema gate, but was cancelled after review
because it did not preserve the requested multi-turn prior conversation
structure.

Replacement schema:

- prior `user(TURN_PROMPT text only)`
- prior `model(gold prior transcript)`
- repeat up to 8 prior same-source turns
- current `user(TURN_PROMPT + current audio)`
- current `model(gold current transcript)`

This keeps exactly one audio part per SFT example while retaining the prior
conversation role alternation. The prior user text is exactly the same
configured TURN_PROMPT text as the current user turn; only prior audio is
omitted.

## 2026-06-25: Exact-Prompt Text-Turn Run Submitted

The first text-turn run used the right conversation structure but the local TOML
prompt had one extra trailing newline relative to
`evaluate_gemini_manual_context.ipynb`'s `TURN_PROMPT`. That job was cancelled:

`projects/781667204380/locations/us-central1/tuningJobs/7168529948666232832`

Replacement round:
`20260625-prior-context-count8-text-turns-exact-sft`

Replacement tuning job:
`projects/781667204380/locations/us-central1/tuningJobs/722401138271322112`

Prepared JSONL validation:

- `config.user_prompt` byte-matches the notebook `TURN_PROMPT`.
- All 16,919 train examples contain exactly one audio part.
- Prior user turns contain exactly `TURN_PROMPT` text and no audio part.
- The final/current user turn contains exactly `TURN_PROMPT` text and the
  current audio part.
- Max history length is 8 prior user/model pairs, for 18 content turns total.

## 2026-06-26: Checkpoint 7 Best, But Worse Than Prior Baseline

The exact-prompt count-8 SFT job succeeded:

`projects/781667204380/locations/us-central1/tuningJobs/722401138271322112`

Best checkpoint on the 4-dataset eval set was checkpoint 7:

- checkpoint endpoint: `projects/781667204380/locations/us/endpoints/8455789775364816896`
- epoch: 8
- step: 2121
- WER: 27.63
- CER: 22.67
- empty rate: 6.69%

The old corrected baseline
`20260619-gemini31-flash-lite-corrected-a16-lr05-e10/checkpoint_5_epoch6_step1665`
scored better on the same current eval rows:

- WER: 22.34
- CER: 15.46
- empty rate: 0.27%

The regression is almost entirely caused by checkpoint-7 empty responses. On
the 3,833 rows where checkpoint 7 returned non-empty text, checkpoint 7 was
essentially tied with the old checkpoint:

- old checkpoint non-empty-row subset WER: 21.66
- checkpoint 7 non-empty-row subset WER: 21.55

If checkpoint-7 empty-error rows are replaced with old checkpoint outputs, the
full-eval WER drops from 27.63 to 22.24.

## 2026-06-26: Empty-Response Settings Probe

Raw re-query of the 275 checkpoint-7 empty rows showed this is not primarily
safety filtering or scorer load:

- 274/275 rows remained empty with the original count-8 text-turn prompt.
- Responses had `finishReason=STOP` or no candidate.
- There was no `promptFeedback`, no safety rating block, no max-token finish,
  and zero generated output tokens.
- SFT train/validation data contains no blank targets.

Safety and output-format settings did not recover empty rows on the failing
sample:

- `BLOCK_NONE` to `OFF`: no improvement
- no `safety_settings`: no improvement
- `response_mime_type="text/plain"`: no improvement
- `response_modalities=["TEXT"]`: no improvement
- `max_output_tokens=2048`: no improvement
- `automatic_function_calling.disable=True`: no improvement
- `thinking_budget=0`: no improvement
- `audio_timestamp=True`: no improvement

Decoding changes recovered some rows but were noisy:

- `temperature=0.7`: recovered 75/275 empty rows, fallback WER 26.65
- `temperature=1.0`: recovered 107/275 empty rows, fallback WER 26.19

The useful mitigation is changing the fallback context representation/count
only when the primary count-8 text-turn request returns empty:

| fallback setting | recovered | remaining empty | fallback full WER | fallback full CER |
|---|---:|---:|---:|---:|
| transcript8 | 190 | 85 | 23.48 | 17.85 |
| transcript4 | 194 | 81 | 23.56 | 17.91 |
| count0 | 198 | 77 | 23.64 | 17.98 |
| count1 | 157 | 118 | 24.20 | 18.76 |

Best chain across saved top-strategy predictions:

`transcript8 -> count1 -> count0 -> transcript4`

- fills 221/275 empty rows
- leaves 54/275 empty rows
- full-eval WER: 23.05
- full-eval CER: 17.34

The best oracle over the four saved strategies is WER 22.82, so simple
generation/config fallback is unlikely to fully recover the old checkpoint.
The remaining gap is probably model/prompt training behavior from repeated
count-8 text-only prior user turns, not a disable-able filter.

## 2026-06-26: Primary Prompt Variants, No Fallback

Prompt variants were evaluated as primary requests, not as fallback retries.
The first pass scored only the 275 rows that were empty under the original
checkpoint-7 count-8 text-turn prompt.

Pure current-turn wording changes with the original repeated prior user prompt
helped only modestly. The best family was a transcript-block prompt
presentation with a shorter ASR system prompt. This avoids the repeated prior
`user(TURN_PROMPT text only)` turns that appear to trigger the immediate-stop
mode.

Best empty-row prompt variants:

| primary prompt variant | non-empty | remaining empty | empty-row WER | empty-row CER |
|---|---:|---:|---:|---:|
| block_system_short_force_words | 240 | 35 | 35.60 | 26.47 |
| block_system_short_final_audio | 234 | 41 | 35.88 | 27.46 |
| block_system_short_direct | 226 | 49 | 36.33 | 28.65 |
| block_system_short_no_preceding | 244 | 31 | 37.51 | 29.29 |

Full-eval primary prompt results, with no fallback:

| primary prompt variant | non-empty | empty | WER | CER |
|---|---:|---:|---:|---:|
| original checkpoint-7 prompt | 3833 | 275 | 27.63 | 22.67 |
| block_system_short_force_words | 4055 | 53 | 23.17 | 16.59 |
| block_system_short_final_audio | 4041 | 67 | 23.26 | 16.81 |
| block_system_short_direct | 4023 | 85 | 23.44 | 17.05 |
| block_direct | 3980 | 128 | 23.88 | 18.05 |
| block_original | 3987 | 121 | 23.98 | 18.19 |

Temperature on the best prompt reduced empties in the empty-row slice but hurt
full-eval WER:

| primary prompt variant | empty-row WER | full WER | full CER | empty |
|---|---:|---:|---:|---:|
| block_system_short_force_words, temp 0.0 | 35.60 | 23.17 | 16.59 | 53 |
| block_system_short_force_words, temp 0.4 | 35.06 | 23.71 | 17.21 | 61 |
| block_system_short_force_words, temp 0.7 | 36.15 | 25.39 | 18.77 | 73 |

Recommended inference prompt for this checkpoint, if using it without fallback:

- prior context presentation: one transcript block in the current user turn
- system prompt:

```text
You are a strict verbatim ASR system for VHF/UHF dispatch radio. Transcribe only the attached current audio clip. Use prior transcripts only as context for names, units, and terminology. Output only the transcript, with no explanation.
```

- current prompt:

```text
Transcribe the attached radio audio clip. Return the words spoken in this clip only. If speech is present, return words; do not stop before producing text.
```

This fixes most but not all of the empty-response issue: WER improves from
27.63 to 23.17, but still trails the old corrected checkpoint at WER 22.34.
