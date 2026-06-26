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
