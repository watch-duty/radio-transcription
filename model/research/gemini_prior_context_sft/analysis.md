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
