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
